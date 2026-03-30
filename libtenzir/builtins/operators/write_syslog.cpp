//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/assert.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>

#include <chrono>
#include <functional>
#include <optional>
#include <ranges>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::write_syslog {

namespace {

auto make_root_field(std::string field) -> ast::root_field {
  return ast::root_field{
    ast::identifier{std::move(field), location::unknown},
  };
}

struct WriteSyslogArgs {
  ast::expression facility{make_root_field("facility")};
  ast::expression severity{make_root_field("severity")};
  ast::expression timestamp{make_root_field("timestamp")};
  ast::expression hostname{make_root_field("hostname")};
  ast::expression app_name{make_root_field("app_name")};
  ast::expression process_id{make_root_field("process_id")};
  ast::expression message_id{make_root_field("message_id")};
  ast::expression structured_data{make_root_field("structured_data")};
  ast::expression message{make_root_field("message")};
};

class WriteSyslog final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteSyslog(WriteSyslogArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice slice, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (slice.rows() == 0) {
      co_await push({});
      co_return;
    }
    auto& dh = ctx.dh();
    auto facility = eval_as<uint64_type>(
      "facility", args_.facility, slice, dh, [&, warned = false] mutable {
        if (not warned) {
          warned = true;
          diagnostic::warning("`facility` evaluated to `null`")
            .primary(args_.facility)
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
            .primary(args_.severity)
            .note("defaulting to `6`")
            .emit(dh);
        }
        return 6;
      });
    auto timestamp
      = eval_as<time_type>("timestamp", args_.timestamp, slice, dh);
    auto hostname = eval_as<string_type>("hostname", args_.hostname, slice, dh);
    auto app_name = eval_as<string_type>("app_name", args_.app_name, slice, dh);
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
        diagnostic::warning("`facility` must be in the range 0 to 23, got `{}`",
                            *f)
          .primary(args_.facility)
          .note("defaulting to `1`")
          .emit(dh);
        *f = 1;
      }
      if (*s > 7u) {
        diagnostic::warning("`severity` must be in the range 0 to 7, got `{}`",
                            *s)
          .primary(args_.severity)
          .note("defaulting to `6`")
          .emit(dh);
        *s = 6;
      }
      auto it = std::back_inserter(buffer);
      const auto format_n
        = [&](std::string_view name, std::optional<std::string_view> str,
              size_t count, const ast::expression& expr) {
            if (not str or str->empty()) {
              fmt::format_to(it, " -");
              return;
            }
            if (str->size() > count) {
              diagnostic::warning("`{}` must not be longer than {} characters",
                                  name, count)
                .primary(expr)
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
            diagnostic::warning("structured data `{}` must be of type `record`",
                                name)
              .primary(args_.structured_data)
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
    co_await push(chunk::make(std::move(buffer)));
  }

private:
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
          .primary(args_.structured_data)
          .emit(dh);
        fmt::format_to(it, "\"\"");
      },
      [&](const view3<list>&) {
        diagnostic::warning("`structured_data` field `{}` has type `list`", k)
          .primary(args_.structured_data)
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
                  .primary(expr)
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
        .primary(expr)
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

  WriteSyslogArgs args_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.write_syslog";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteSyslogArgs, WriteSyslog>{};
    d.named_optional("facility", &WriteSyslogArgs::facility, "int");
    d.named_optional("severity", &WriteSyslogArgs::severity, "int");
    d.named_optional("timestamp", &WriteSyslogArgs::timestamp, "time");
    d.named_optional("hostname", &WriteSyslogArgs::hostname, "string");
    d.named_optional("app_name", &WriteSyslogArgs::app_name, "string");
    d.named_optional("process_id", &WriteSyslogArgs::process_id, "string");
    d.named_optional("message_id", &WriteSyslogArgs::message_id, "string");
    d.named_optional("structured_data", &WriteSyslogArgs::structured_data,
                     "record");
    d.named_optional("message", &WriteSyslogArgs::message, "string");
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::write_syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_syslog::plugin)
