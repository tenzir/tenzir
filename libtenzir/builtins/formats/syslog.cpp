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

#include <tenzir/argument_parser.hpp>
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

auto parse_loop(generator<std::optional<std::string_view>> lines,
                operator_control_plane& ctrl,
                multi_series_builder::options opts,
                const std::optional<ast::field_path>& raw_message_field
                = std::nullopt) -> generator<table_slice> {
  auto dh = transforming_diagnostic_handler{
    ctrl.diagnostics(), [](auto diag) {
      diag.message = fmt::format("syslog parser: {}", diag.message);
      return diag;
    }};
  const auto ordered = opts.settings.ordered;
  auto new_builder
    = syslog_builder{infuse_new_schema(opts), dh, raw_message_field};
  auto legacy_builder
    = legacy_syslog_builder{infuse_legacy_schema(opts), dh, raw_message_field};
  auto legacy_structured_builder = legacy_syslog_builder{
    infuse_legacy_structured_schema(opts), dh, raw_message_field, true};
  auto unknown_builder = unknown_syslog_builder{opts, dh};
  auto last = builder_tag::unknown_syslog_builder;
  const auto maybe_flush
    = [&](builder_tag new_tag) -> std::vector<table_slice> {
    if (not ordered) {
      return {};
    }
    if (new_tag == last) {
      return {};
    }
    switch (last) {
      using enum builder_tag;
      case syslog_builder:
        return new_builder.finalize_as_table_slice();
      case legacy_syslog_builder:
        return legacy_builder.finalize_as_table_slice();
      case legacy_structured_syslog_builder:
        return legacy_structured_builder.finalize_as_table_slice();
      case unknown_syslog_builder:
        return unknown_builder.finalize_as_table_slice();
    }
    TENZIR_UNREACHABLE();
  };
  auto line_nr = size_t{0};
  co_yield {};
  for (auto&& line : lines) {
    for (auto&& s : new_builder.yield_ready()) {
      co_yield std::move(s);
    }
    for (auto&& s : legacy_builder.yield_ready()) {
      co_yield std::move(s);
    }
    for (auto&& s : legacy_structured_builder.yield_ready()) {
      co_yield std::move(s);
    }
    for (auto&& s : unknown_builder.yield_ready()) {
      co_yield std::move(s);
    }
    if (not line) {
      co_yield {};
      continue;
    }
    ++line_nr;
    if (line->empty()) {
      continue;
    }
    const auto* f = line->begin();
    const auto* const l = line->end();
    message msg{};
    legacy_message legacy_msg{};
    if (auto parser = message_parser{}; parser(f, l, msg)) {
      for (auto&& s : maybe_flush(builder_tag::syslog_builder)) {
        co_yield std::move(s);
      }
      last = builder_tag::syslog_builder;
      if (raw_message_field) {
        new_builder.add_new({std::move(msg), line_nr, std::string{*line}});
      } else {
        new_builder.add_new({std::move(msg), line_nr});
      }
    } else if (auto legacy_parser = legacy_message_parser{};
               legacy_parser(f, l, legacy_msg)) {
      auto tag = get_legacy_builder_tag(legacy_msg);
      for (auto&& s : maybe_flush(tag)) {
        co_yield std::move(s);
      }
      last = tag;
      if (tag == builder_tag::legacy_syslog_builder) {
        if (raw_message_field) {
          legacy_builder.add_new(
            {std::move(legacy_msg), line_nr, std::string{*line}});
        } else {
          legacy_builder.add_new({std::move(legacy_msg), line_nr});
        }
      } else {
        TENZIR_ASSERT(tag == builder_tag::legacy_structured_syslog_builder);
        if (raw_message_field) {
          legacy_structured_builder.add_new(
            {std::move(legacy_msg), line_nr, std::string{*line}});
        } else {
          legacy_structured_builder.add_new({std::move(legacy_msg), line_nr});
        }
      }
    } else if (last == builder_tag::syslog_builder
               and new_builder.add_line_to_latest(*line)) {
      continue;
    } else if (last == builder_tag::legacy_syslog_builder
               and legacy_builder.add_line_to_latest(*line)) {
      continue;
    } else if (last == builder_tag::legacy_structured_syslog_builder
               and legacy_structured_builder.add_line_to_latest(*line)) {
      continue;
    } else {
      for (auto&& s : maybe_flush(builder_tag::unknown_syslog_builder)) {
        co_yield std::move(s);
      }
      last = builder_tag::unknown_syslog_builder;
      unknown_builder.add_new({std::string{*line}, line_nr});
    }
  }
  for (auto&& s : new_builder.finalize_as_table_slice()) {
    co_yield std::move(s);
  }
  for (auto&& s : legacy_builder.finalize_as_table_slice()) {
    co_yield std::move(s);
  }
  for (auto&& s : legacy_structured_builder.finalize_as_table_slice()) {
    co_yield std::move(s);
  }
  for (auto&& s : unknown_builder.finalize_as_table_slice()) {
    co_yield std::move(s);
  }
}

inline auto split_octet(generator<chunk_ptr> input, diagnostic_handler& dh)
  -> generator<std::optional<std::string_view>> {
  auto buffer = std::string{};
  auto remaining_message_length = size_t{};
  for (auto&& chunk : input) {
    if (not chunk || chunk->size() == 0) {
      co_yield std::nullopt;
      continue;
    }
    auto* chunk_begin = reinterpret_cast<const char*>(chunk->data());
    const auto chunk_end = chunk_begin + chunk->size();
    auto chunk_length = [&]() -> size_t {
      return static_cast<size_t>(chunk_end - chunk_begin);
    };
    // We still need bytes to continue a previous message
    if (remaining_message_length > 0) {
      const auto take = std::min(remaining_message_length, chunk_length());
      buffer.append(chunk_begin, take);
      remaining_message_length -= take;
      // The message is complete, we can yield it
      if (remaining_message_length == 0) {
        co_yield buffer;
        buffer.clear();
      }
      chunk_begin += take;
    }
    // A new message starts in (the remainder of) the chunk
    while (chunk_begin != chunk_end) {
      auto it = chunk_begin;
      if (not octet_length_parser(it, chunk_end, remaining_message_length)) {
        TENZIR_WARN("`{}`", std::string_view{chunk_begin, chunk_end});
        diagnostic::error("failed to parse octet-counting length prefix")
          .emit(dh);
        co_return;
      }
      if (remaining_message_length > max_syslog_message_size) {
        diagnostic::error("octet-counted message length {} exceeds maximum {}",
                          remaining_message_length, max_syslog_message_size)
          .emit(dh);
        co_return;
      }
      chunk_begin = it;
      // The new message is fully within this chunk
      if (remaining_message_length <= chunk_length()) {
        const auto message_end = chunk_begin + remaining_message_length;
        remaining_message_length = 0;
        co_yield std::string_view{chunk_begin, message_end};
        chunk_begin = message_end;
        continue; // more messages in this chunk
      } else {
        // The message is longer than the remainder of the chunk:
        buffer.append(chunk_begin, chunk_end);
        remaining_message_length -= chunk_length();
        break; // next chunk
      }
    }
  }
}

class syslog_parser final : public plugin_parser {
public:
  syslog_parser() = default;

  syslog_parser(multi_series_builder::options opts, bool octet_counting,
                std::optional<ast::field_path> raw_message_field = std::nullopt)
    : opts_{std::move(opts)},
      octet_counting_{octet_counting},
      raw_message_field_{std::move(raw_message_field)} {
  }

  auto name() const -> std::string override {
    return "syslog";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    if (octet_counting_) {
      return parse_loop(split_octet(std::move(input), ctrl.diagnostics()), ctrl,
                        opts_, raw_message_field_);
    }
    return parse_loop(to_lines(std::move(input)), ctrl, opts_,
                      raw_message_field_);
  }

  friend auto inspect(auto& f, syslog_parser& x) -> bool {
    return f.object(x).fields(
      f.field("octet", x.octet_counting_), f.field("opts", x.opts_),
      f.field("raw_message_field", x.raw_message_field_));
  }

private:
  multi_series_builder::options opts_;
  bool octet_counting_ = false;
  std::optional<ast::field_path> raw_message_field_;
};

auto make_root_field(std::string field) -> ast::root_field {
  return ast::root_field{
    ast::identifier{std::move(field), location::unknown},
  };
}

struct printer_args final {
  ast::expression facility{make_root_field("facility")};
  ast::expression severity{make_root_field("severity")};
  ast::expression timestamp{make_root_field("timestamp")};
  ast::expression hostname{make_root_field("hostname")};
  ast::expression app_name{make_root_field("app_name")};
  ast::expression process_id{make_root_field("process_id")};
  ast::expression message_id{make_root_field("message_id")};
  ast::expression structured_data{make_root_field("structured_data")};
  ast::expression message{make_root_field("message")};
  location op;

  auto add_to(argument_parser2& p) -> void {
    p.named_optional("facility", facility, "int");
    p.named_optional("severity", severity, "int");
    p.named_optional("timestamp", timestamp, "time");
    p.named_optional("hostname", hostname, "string");
    p.named_optional("app_name", app_name, "string");
    p.named_optional("process_id", process_id, "string");
    p.named_optional("message_id", message_id, "string");
    p.named_optional("structured_data", structured_data, "record");
    p.named_optional("message", message, "string");
  }

  auto loc(into_location loc) const -> location {
    return loc ? loc : op;
  }

  friend auto inspect(auto& f, printer_args& x) -> bool {
    return f.object(x).fields(
      f.field("facility", x.facility), f.field("severity", x.severity),
      f.field("timestamp", x.timestamp), f.field("hostname", x.hostname),
      f.field("app_name", x.app_name), f.field("process_id", x.process_id),
      f.field("message_id", x.message_id),
      f.field("structured_data", x.structured_data),
      f.field("message", x.message), f.field("op", x.op));
  }
};

class syslog_printer final : public crtp_operator<syslog_printer> {
public:
  syslog_printer() = default;

  syslog_printer(printer_args args) : args_{std::move(args)} {
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto& dh = ctrl.diagnostics();
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto ty = as<record_type>(slice.schema());
      auto facility = eval_as<uint64_type>(
        "facility", args_.facility, slice, dh, [&, warned = false] mutable {
          if (not warned) {
            warned = true;
            diagnostic::warning("`facility` evaluated to `null`")
              .primary(args_.loc(args_.facility))
              .note("defaulting to `1`")
              .emit(dh);
          }
          return 1;
        });
      auto severity = eval_as<uint64_type>(
        "severity", args_.severity, slice, dh, [&, warned = false] mutable {
          if (not warned) {
            warned = true;
            diagnostic::warning("`severity` evaluated to `null`")
              .primary(args_.loc(args_.severity))
              .note("defaulting to `6`")
              .emit(dh);
          }
          return 6;
        });
      auto timestamp
        = eval_as<time_type>("timestamp", args_.timestamp, slice, dh);
      auto hostname
        = eval_as<string_type>("hostname", args_.hostname, slice, dh);
      auto app_name
        = eval_as<string_type>("app_name", args_.app_name, slice, dh);
      auto process_id
        = eval_as<string_type>("process_id", args_.process_id, slice, dh);
      auto message_id
        = eval_as<string_type>("message_id", args_.message_id, slice, dh);
      auto structured_data = eval_as<record_type>(
        "structured_data", args_.structured_data, slice, dh);
      auto message = eval_as<string_type>("message", args_.message, slice, dh);
      auto buffer = std::vector<char>{};
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        auto f = facility.next().value();
        auto s = severity.next().value();
        auto t = timestamp.next().value();
        auto host = hostname.next().value();
        auto app = app_name.next().value();
        auto pid = process_id.next().value();
        auto mid = message_id.next().value();
        auto sd = structured_data.next().value();
        auto msg = message.next().value();
        TENZIR_ASSERT(f);
        TENZIR_ASSERT(s);
        if (*f > 23u) {
          diagnostic::warning(
            "`facility` must be in the range 0 to 23, got `{}`", *f)
            .primary(args_.loc(args_.facility))
            .note("defaulting to `1`")
            .emit(dh);
          *f = 1;
        }
        if (*s > 7u) {
          diagnostic::warning(
            "`severity` must be in the range 0 to 7, got `{}`", *s)
            .primary(args_.loc(args_.severity))
            .note("defaulting to `6`")
            .emit(dh);
          *s = 6;
        }
        auto it = std::back_inserter(buffer);
        const auto format_n = [&](std::string_view name,
                                  std::optional<std::string_view> str,
                                  size_t count, const ast::expression& expr) {
          if (not str or str->empty()) {
            fmt::format_to(it, " -");
            return;
          }
          if (str->size() > count) {
            diagnostic::warning("`{}` must not be longer than {} characters",
                                name, count)
              .primary(args_.loc(expr))
              .emit(dh);
          }
          fmt::format_to(it, " {}", std::views::take(*str, count));
        };
        fmt::format_to(it, "<{}>{}", (*f * 8) + *s, 1);
        if (t) {
          fmt::format_to(
            it, " {:%FT%TZ}",
            std::chrono::time_point_cast<std::chrono::microseconds>(*t));
        } else {
          fmt::format_to(it, " -");
        }
        format_n("hostname", host, 255, args_.hostname);
        format_n("app_name", app, 48, args_.app_name);
        format_n("process_id", pid, 128, args_.process_id);
        format_n("message_id", mid, 32, args_.message_id);
        if (sd and sd->begin() != sd->end()) {
          fmt::format_to(it, " ");
          for (const auto& [name, val] : *sd) {
            const auto* params = try_as<view3<record>>(val);
            if (not params) {
              diagnostic::warning(
                "structured data `{}` must be of type `record`", name)
                .primary(args_.loc(args_.structured_data))
                .note("skipping structured data `{}`", name)
                .emit(dh);
              continue;
            }
            fmt::format_to(it, "[{}", name);
            for (const auto& [k, v] : *params) {
              fmt::format_to(it, " {}=", k);
              format_val(it, k, v, dh);
            }
            fmt::format_to(it, "]");
          }
        } else {
          fmt::format_to(it, " -");
        }
        if (msg) {
          fmt::format_to(it, " {}", *msg);
        }
        buffer.push_back('\n');
      }
      co_yield chunk::make(std::move(buffer));
    }
  }

  auto format_val(auto& it, std::string_view k, data_view3 v,
                  diagnostic_handler& dh) const -> void {
    match(
      v,
      [&](const caf::none_t&) {
        fmt::format_to(it, "\"\"");
      },
      [&](const concepts::integer auto& x) {
        fmt::format_to(it, "\"{}\"", x);
      },
      [&](const view3<record>&) {
        diagnostic::warning("`structured_data` field `{}` has type `record`", k)
          .primary(args_.loc(args_.structured_data))
          .emit(dh);
        fmt::format_to(it, "\"\"");
      },
      [&](const view3<list>&) {
        diagnostic::warning("`structured_data` field `{}` has type `list`", k)
          .primary(args_.loc(args_.structured_data))
          .emit(dh);
        fmt::format_to(it, "\"\"");
      },
      [&](const std::string_view& x) {
        *it = '"';
        ++it;
        for (const auto& c : x) {
          if (c == '\\' or c == '"' or c == ']') {
            *it = '\\';
            ++it;
          }
          *it = c;
          ++it;
        }
        *it = '"';
        ++it;
      },
      [&](const auto& x) {
        format_val(it, k, fmt::format("{}", x), dh);
      });
  }

  template <typename T>
  auto eval_as(std::string_view name, const ast::expression& expr,
               const table_slice& slice, diagnostic_handler& dh,
               auto make_default) const
    -> generator<std::optional<view3<type_to_data_t<T>>>> {
    auto ms = std::invoke([&] {
      if (expr.get_location()) {
        return eval(expr, slice, dh);
      }
      auto ndh = null_diagnostic_handler{};
      return eval(expr, slice, ndh);
    });
    for (const auto& s : ms.parts()) {
      if (s.type.kind().template is<null_type>()) {
        for (auto i = int64_t{}; i < s.length(); ++i) {
          co_yield make_default();
        }
        continue;
      }
      if (s.type.kind().template is<T>()) {
        for (auto val : s.template values<T>()) {
          if (val) {
            co_yield std::move(*val);
          } else {
            co_yield make_default();
          }
        }
        continue;
      }
      if constexpr (concepts::one_of<T, int64_type, uint64_type>) {
        using alt_type = std::conditional_t<std::same_as<T, int64_type>,
                                            uint64_type, int64_type>;
        if (s.type.kind().template is<alt_type>()) {
          auto overflow_warned = false;
          for (auto val : s.template values<alt_type>()) {
            if (not val) {
              co_yield make_default();
              continue;
            }
            if (not std::in_range<decltype(T::construct())>(*val)) {
              if (not overflow_warned) {
                overflow_warned = true;
                diagnostic::warning("overflow in `{}`, got `{}`", name, *val)
                  .primary(args_.loc(expr))
                  .emit(dh);
              }
              co_yield make_default();
              continue;
            }
            co_yield *val;
          }
          continue;
        }
      }
      diagnostic::warning("`{}` must be `{}`, got `{}`", name, T{},
                          s.type.kind())
        .primary(args_.loc(expr))
        .emit(dh);
      for (auto i = int64_t{}; i < s.length(); ++i) {
        co_yield make_default();
      }
    }
  }

  template <typename T>
  auto eval_as(std::string_view name, const ast::expression& expr,
               const table_slice& slice, diagnostic_handler& dh) const
    -> generator<std::optional<view3<type_to_data_t<T>>>> {
    return eval_as<T>(name, expr, slice, dh, [] {
      return std::nullopt;
    });
  }

  auto name() const -> std::string override {
    return "write_syslog";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, syslog_printer& x) -> bool {
    return f.apply(x.args_);
  }

private:
  printer_args args_;
};

class plugin final : public virtual parser_plugin<syslog_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto msb_opts = msb_parser.get_options(dh);
    msb_opts->settings.default_schema_name = "syslog.unknown";
    for (auto&& diag : std::move(dh).collect()) {
      if (diag.severity == severity::error) {
        throw diag;
      }
    }
    return std::make_unique<syslog_parser>(std::move(*msb_opts), false);
  }
};

class read_syslog final
  : public virtual operator_plugin2<parser_adapter<syslog_parser>> {
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_("read_syslog");
    bool octet_counting = false;
    std::optional<ast::field_path> raw_message_field;
    parser.named_optional("octet_counting", octet_counting);
    parser.named("raw_message", raw_message_field);
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<syslog_parser>>(syslog_parser{
      std::move(opts), octet_counting, std::move(raw_message_field)});
  }
};

class parse_syslog final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_syslog";
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
    auto octet_counting = std::optional<bool>{};
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
            auto legacy_structured_builder
              = legacy_syslog_builder{infuse_legacy_structured_schema(msb_opts),
                                      ctx, std::nullopt, true};
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
            /// if parsing failed.
            const auto try_parse
              = [&](std::string_view input, message& msg,
                    legacy_message& legacy_msg) -> builder_tag {
              auto f = input.begin();
              auto l = input.end();
              if (message_parser{}.parse(f, l, msg)) {
                return builder_tag::syslog_builder;
              }
              f = input.begin();
              if (legacy_message_parser{}.parse(f, l, legacy_msg)) {
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

class write_syslog final : public operator_plugin2<syslog_printer> {
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = printer_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_("write_syslog");
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    return std::make_unique<syslog_printer>(std::move(args));
  }
};

} // namespace
} // namespace tenzir::plugins::syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::read_syslog)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::parse_syslog)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::write_syslog)
