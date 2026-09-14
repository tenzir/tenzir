//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/concept/printable/std/chrono.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/syslog.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/to_lines.hpp>

#include <arrow/type_fwd.h>

#include <ranges>
#include <string_view>

using namespace std::chrono_literals;

namespace tenzir::plugins::syslog {

namespace {

class parse_syslog final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "parse_syslog";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    // nullopt = auto-detect: try octet-counting first, fall back to plain syslog
    // true = require octet-counting prefix, warn if missing
    // false = never parse octet-counting prefix
    auto octet_counting = Option<bool>{};
    // TODO: Consider adding a `many` option to expect multiple json values.
    auto parser = argument_parser2::function(name());
    parser.positional("input", expr, "string");
    parser.named("octet_counting", octet_counting);
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(
      parser, true, multi_series_builder_argument_parser::merge_option::hidden);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    return function_use::make([call = inv.call.get_location(),
                               msb_opts = std::move(msb_opts), octet_counting,
                               expr
                               = std::move(expr)](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series arg) {
        auto f = detail::overload{
          [&](const arrow::NullArray&) -> multi_series {
            return arg;
          },
          [&](const arrow::StringArray& arg) -> multi_series {
            auto builder = syslog_builder{infuse_new_schema(msb_opts), ctx};
            auto legacy_builder
              = legacy_syslog_builder{infuse_legacy_schema(msb_opts), ctx};
            auto legacy_structured_builder = legacy_syslog_builder{
              infuse_legacy_structured_schema(msb_opts), ctx, None{}, true};
            auto last = builder_tag::syslog_builder;
            auto res = multi_series{};
            /// flushes the current builder, if its not the same as
            /// `new_builder`
            const auto maybe_flush = [&](builder_tag new_builder) {
              if (new_builder == last) {
                return;
              }
              switch (last) {
                using enum builder_tag;
                case syslog_builder: {
                  res.append(multi_series{builder.finalize()});
                  break;
                }
                case legacy_syslog_builder: {
                  res.append(multi_series{legacy_builder.finalize()});
                  break;
                }
                case legacy_structured_syslog_builder: {
                  res.append(
                    multi_series{legacy_structured_builder.finalize()});
                  break;
                }
                case unknown_syslog_builder:
                  TENZIR_UNREACHABLE();
              }
            };
            /// adds a null to the current builder
            const auto add_null = [&]() {
              switch (last) {
                using enum builder_tag;
                case syslog_builder: {
                  builder.builder.null();
                  break;
                }
                case legacy_syslog_builder: {
                  legacy_builder.builder.null();
                  break;
                }
                case legacy_structured_syslog_builder: {
                  legacy_structured_builder.builder.null();
                  break;
                }
                case unknown_syslog_builder:
                  TENZIR_UNREACHABLE();
              }
            };
            /// Tries to parse input as syslog; returns the builder_tag
            /// indicating which parser succeeded, or unknown_syslog_builder
            /// if parsing failed. A parse only counts as successful if it
            /// consumes the entire input; otherwise a partial parse would
            /// silently drop the trailing bytes, so we reject it and let the
            /// caller emit a diagnostic instead.
            const auto try_parse
              = [&](std::string_view input, message& msg,
                    legacy_message& legacy_msg) -> builder_tag {
              auto f = input.begin();
              auto l = input.end();
              if (message_parser{}.parse(f, l, msg) and f == l) {
                return builder_tag::syslog_builder;
              }
              f = input.begin();
              if (legacy_message_parser{}.parse(f, l, legacy_msg) and f == l) {
                return get_legacy_builder_tag(legacy_msg);
              }
              return builder_tag::unknown_syslog_builder;
            };
            /// Adds a parsed message to the appropriate builder based on tag.
            const auto add_parsed = [&](builder_tag tag, message& msg,
                                        legacy_message& legacy_msg) {
              switch (tag) {
                using enum builder_tag;
                case syslog_builder:
                  maybe_flush(syslog_builder);
                  builder.add_new({std::move(msg), 0});
                  last = syslog_builder;
                  break;
                case legacy_syslog_builder:
                  maybe_flush(legacy_syslog_builder);
                  legacy_builder.add_new({std::move(legacy_msg), 0});
                  last = legacy_syslog_builder;
                  break;
                case legacy_structured_syslog_builder:
                  maybe_flush(legacy_structured_syslog_builder);
                  legacy_structured_builder.add_new({std::move(legacy_msg), 0});
                  last = legacy_structured_syslog_builder;
                  break;
                case unknown_syslog_builder:
                  TENZIR_UNREACHABLE();
              }
            };
            // RFC 6587 octet-counting algorithm:
            //
            // 1. Try to parse octet count prefix if octet_counting != false.
            // 2. If octet_counting=true (explicit) and prefix missing/invalid
            //    → warn, null.
            // 3. If prefix found:
            //    a. actual < stated → warn "exceeds actual length", null.
            //    b. actual == stated → parse content.
            //    c. actual > stated:
            //       - explicit mode → truncate to stated, parse, warn.
            //       - auto mode → try full first; fall back to truncated.
            // 4. If no prefix → parse full input.
            // 5. If parse fails → warn "not valid syslog", null.
            // 6. Emit parsed message.
            //
            // The key distinction: explicit mode trusts the octet count,
            // while auto mode treats it as a hint (maximizing leniency).
            for (int64_t i = 0; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                add_null();
                continue;
              }
              const auto input = arg.Value(i);
              // Step 1: Try to parse octet count prefix (RFC 6587 framing).
              auto has_prefix = false;
              auto stated_length = uint32_t{};
              auto content = input;
              const auto is_explicit
                = octet_counting.has_value() && *octet_counting;
              if (octet_counting.value_or(true)) { // true or auto-detect
                auto it = input.begin();
                if (octet_length_parser(it, input.end(), stated_length)
                    && stated_length <= max_syslog_message_size) {
                  has_prefix = true;
                  content = std::string_view{it, input.end()};
                } else if (is_explicit) {
                  // Step 2: Explicitly required but not found/invalid.
                  diagnostic::warning("expected valid octet-counted input")
                    .primary(expr.get_location())
                    .emit(ctx);
                  add_null();
                  continue;
                }
              }
              // Step 3: Determine what to parse based on prefix and length.
              auto msg = message{};
              auto legacy_msg = legacy_message{};
              if (has_prefix) {
                const auto actual = content.size();
                if (actual < stated_length) {
                  // Step 3a: Message shorter than stated → incomplete.
                  diagnostic::warning("octet count exceeds actual message "
                                      "length")
                    .note("expected {} bytes, got {}", stated_length, actual)
                    .primary(expr.get_location())
                    .emit(ctx);
                  add_null();
                  continue;
                }
                auto parsed_tag = builder_tag::unknown_syslog_builder;
                if (actual == stated_length) {
                  // Step 3b: Exact match → parse content.
                  parsed_tag = try_parse(content, msg, legacy_msg);
                  if (parsed_tag == builder_tag::unknown_syslog_builder) {
                    diagnostic::warning("`input` is not valid syslog")
                      .primary(expr.get_location())
                      .emit(ctx);
                    add_null();
                    continue;
                  }
                } else {
                  // Step 3c: actual > stated_length.
                  if (is_explicit) {
                    // Explicit mode: trust the count, truncate, and parse.
                    auto truncated
                      = std::string_view{content.data(), stated_length};
                    parsed_tag = try_parse(truncated, msg, legacy_msg);
                    if (parsed_tag == builder_tag::unknown_syslog_builder) {
                      diagnostic::warning("`input` is not valid syslog")
                        .primary(expr.get_location())
                        .emit(ctx);
                      add_null();
                      continue;
                    }
                    diagnostic::warning("octet count less than actual length")
                      .note("parsed truncated message")
                      .primary(expr.get_location())
                      .emit(ctx);
                  } else {
                    // Auto mode: try full first, fall back to truncated.
                    parsed_tag = try_parse(content, msg, legacy_msg);
                    if (parsed_tag != builder_tag::unknown_syslog_builder) {
                      // Full parse succeeded despite mismatched octet count.
                      diagnostic::warning("octet count prefix ignored")
                        .note("message parsed without framing")
                        .primary(expr.get_location())
                        .emit(ctx);
                    } else {
                      // Full failed; try truncated as recovery.
                      auto truncated
                        = std::string_view{content.data(), stated_length};
                      parsed_tag = try_parse(truncated, msg, legacy_msg);
                      if (parsed_tag != builder_tag::unknown_syslog_builder) {
                        diagnostic::warning("octet count less than actual "
                                            "length")
                          .note("parsed truncated message")
                          .primary(expr.get_location())
                          .emit(ctx);
                      } else {
                        diagnostic::warning("`input` is not valid syslog")
                          .primary(expr.get_location())
                          .emit(ctx);
                        add_null();
                        continue;
                      }
                    }
                  }
                }
                add_parsed(parsed_tag, msg, legacy_msg);
              } else {
                // Step 4: No prefix → parse full input.
                auto parsed_tag = try_parse(input, msg, legacy_msg);
                if (parsed_tag == builder_tag::unknown_syslog_builder) {
                  diagnostic::warning("`input` is not valid syslog")
                    .primary(expr.get_location())
                    .emit(ctx);
                  add_null();
                  continue;
                }
                add_parsed(parsed_tag, msg, legacy_msg);
              }
            }
            /// We flush with a new builder tag of "unknown", as that is
            /// guaranteed to flush the last builder
            maybe_flush(builder_tag::unknown_syslog_builder);
            return res;
          },
          [&](const auto&) -> multi_series {
            diagnostic::warning("`parse_syslog` expected `string`, got `{}`",
                                arg.type.kind())
              .primary(call)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          },
        };
        return match(*arg.array, f);
      });
    });
  }
};

} // namespace
} // namespace tenzir::plugins::syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::parse_syslog)
