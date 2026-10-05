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
#include <tenzir/nova/event_builder.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/to_lines.hpp>

#include <arrow/type_fwd.h>

#include <array>
#include <ranges>
#include <string_view>

using namespace std::chrono_literals;

namespace tenzir::plugins::syslog {

namespace {

auto try_parse(std::string_view input, message& msg, legacy_message& legacy_msg)
  -> builder_tag {
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
}

template <class AddParsed, class AddNull>
auto parse_input(std::string_view input, Option<bool> octet_counting,
                 location source, diagnostic_handler& dh, AddParsed add_parsed,
                 AddNull add_null) -> void {
  auto content = input;
  auto stated_length = Option<uint32_t>{};
  auto const explicit_count = octet_counting.value_or(false);
  if (octet_counting.value_or(true)) {
    auto it = input.begin();
    auto length = uint32_t{};
    if (octet_length_parser(it, input.end(), length)
        and length <= max_syslog_message_size) {
      stated_length = length;
      content = std::string_view{it, input.end()};
    } else if (explicit_count) {
      diagnostic::warning("expected valid octet-counted input")
        .primary(source)
        .emit(dh);
      add_null();
      return;
    }
  }
  if (stated_length and content.size() < *stated_length) {
    diagnostic::warning("octet count exceeds actual message length")
      .note("expected {} bytes, got {}", *stated_length, content.size())
      .primary(source)
      .emit(dh);
    add_null();
    return;
  }
  auto msg = message{};
  auto legacy_msg = legacy_message{};
  auto const excess = stated_length and content.size() > *stated_length;
  // An explicit count is authoritative. Auto-detection first tries the full
  // message, falling back to the framed prefix only if that fails to parse.
  auto parsed = try_parse(
    explicit_count and excess ? content.substr(0, *stated_length) : content,
    msg, legacy_msg);
  auto truncated = explicit_count and excess;
  if (parsed == builder_tag::unknown_syslog_builder and excess
      and not explicit_count) {
    msg = {};
    legacy_msg = {};
    parsed = try_parse(content.substr(0, *stated_length), msg, legacy_msg);
    truncated = true;
  }
  if (parsed == builder_tag::unknown_syslog_builder) {
    diagnostic::warning("`input` is not valid syslog").primary(source).emit(dh);
    add_null();
    return;
  }
  if (excess) {
    if (truncated) {
      diagnostic::warning("octet count less than actual length")
        .note("parsed truncated message")
        .primary(source)
        .emit(dh);
    } else {
      diagnostic::warning("octet count prefix ignored")
        .note("message parsed without framing")
        .primary(source)
        .emit(dh);
    }
  }
  add_parsed(parsed, msg, legacy_msg);
}

struct ParseSyslogArgs {
  nova::ValueArgument input;
  Option<bool> octet_counting;
  nova::EventBuilder::Settings settings;
  std::array<nova::EventBuilder::Settings, 3> dialect_settings;
  location call;
};

struct ParseSyslogFunction {
  static auto eval(ParseSyslogArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto last = builder_tag::syslog_builder;
    auto builder
      = nova::EventBuilder::make_prevalidated(args.dialect_settings[0], frame);
    auto chunks = std::vector<nova::Array<nova::Data>>{};
    auto add_null = [&] {
      builder.value().null();
    };
    auto add_parsed
      = [&](builder_tag tag, message& msg, legacy_message& legacy_msg) {
          if (tag != last) {
            if (builder.length() > 0) {
              chunks.push_back(builder.finish_data());
            }
            builder = nova::EventBuilder::make_prevalidated(
              args.dialect_settings[static_cast<size_t>(tag)], frame);
            last = tag;
          }
          auto record = builder.value().record();
          if (tag == builder_tag::syslog_builder) {
            append_message(record, msg);
          } else {
            append_message(record, legacy_msg);
          }
        };
    auto warned_type = false;
    for (auto row = nova::storage::Index{0}; row < args.input.data.length();
         ++row) {
      if (not frame.mask().get(row)) {
        builder.skip();
        continue;
      }
      match(args.input.data.get(row), [&]<class T>(nova::RowView<T> value) {
        if constexpr (std::same_as<T, nova::String>) {
          parse_input(std::string_view{*value}, args.octet_counting,
                      args.input.source, frame, add_parsed, add_null);
        } else {
          if constexpr (not std::same_as<T, nova::Null>) {
            if (not std::exchange(warned_type, true)) {
              diagnostic::warning("`parse_syslog` expected `string`, got `{}`",
                                  nova::Type<T>::static_name)
                .primary(args.call)
                .emit(frame);
            }
          }
          add_null();
        }
      });
    }
    if (chunks.empty()) {
      return builder.finish_data();
    }
    chunks.push_back(builder.finish_data());
    auto result = nova::ArrayBuilder<nova::Data>{};
    for (auto const& chunk : chunks) {
      for (auto row = nova::storage::Index{0}; row < chunk.length(); ++row) {
        if (frame.mask().get(result.length())) {
          nova::append_row(result, chunk.get(row));
        } else {
          result.skip();
        }
      }
    }
    return result.finish();
  }
};

class parse_syslog final : public virtual nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<ParseSyslogArgs, ParseSyslogFunction>{};
    d.positional("input", &ParseSyslogArgs::input, "string");
    d.named("octet_counting", &ParseSyslogArgs::octet_counting);
    d.call_location(&ParseSyslogArgs::call);
    auto validate = nova::add_event_builder_to_describer(
      d, &ParseSyslogArgs::settings,
      {.schema_only_requires_schema_or_selector = false});
    d.validate([validate](ParseSyslogArgs& args,
                          nova::FunctionValidateCtx& ctx) -> failure_or<void> {
      TRY(validate(args, ctx));
      constexpr auto schemas = std::array{"syslog.rfc5424", "syslog.rfc3164",
                                          "syslog.rfc3164.structured"};
      for (auto i = size_t{0}; i < schemas.size(); ++i) {
        auto& settings = args.dialect_settings[i];
        settings = args.settings;
        settings.infer_unparsed_under = "structured_data";
        if (try_as<nova::EventBuilder::NoPolicy>(settings.policy)) {
          settings.policy = nova::EventBuilder::SchemaPolicy{schemas[i]};
          TRY(nova::EventBuilder::make(settings, ctx));
        }
      }
      return {};
    });
    return std::move(d).finish();
  }

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
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                add_null();
                continue;
              }
              parse_input(arg.Value(i), octet_counting, expr.get_location(),
                          ctx, add_parsed, add_null);
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
