//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/eval_as.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>

#include <array>
#include <chrono>
#include <ranges>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

#include "write_bytes.hpp"

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
    auto& dh = ctx.dh();
    auto facility = detail::eval_as<uint64_type>(
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
    auto severity = detail::eval_as<uint64_type>(
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
      = detail::eval_as<time_type>("timestamp", args_.timestamp, slice, dh);
    auto hostname
      = detail::eval_as<string_type>("hostname", args_.hostname, slice, dh);
    auto app_name
      = detail::eval_as<string_type>("app_name", args_.app_name, slice, dh);
    auto process_id
      = detail::eval_as<string_type>("process_id", args_.process_id, slice, dh);
    auto message_id
      = detail::eval_as<string_type>("message_id", args_.message_id, slice, dh);
    auto structured_data = detail::eval_as<record_type>(
      "structured_data", args_.structured_data, slice, dh);
    auto message
      = detail::eval_as<string_type>("message", args_.message, slice, dh);

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
        = [&](std::string_view name, Option<std::string_view> str, size_t count,
              const ast::expression& expr) {
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

  WriteSyslogArgs args_;
};

/// One evaluated argument of a batch. Warns about unexpected types at most
/// once per type and about `null` at most once, if the argument has a default.
class SyslogColumn {
public:
  SyslogColumn(std::string_view name, ast::expression const& expr,
               nova::Array<nova::Data> values, Option<uint64_t> fallback)
    : name_{name},
      expr_{expr},
      values_{std::move(values)},
      fallback_{fallback} {
  }

  /// Returns the value at `row` if it has type `T`, and `None` otherwise.
  template <class T>
  auto get(nova::storage::Index row, diagnostic_handler& dh)
    -> Option<typename nova::Type<T>::ViewType> {
    using Result = Option<typename nova::Type<T>::ViewType>;
    auto value = values_.get(row);
    return match(
      value,
      [&](nova::RowView<nova::Null>) -> Result {
        warn_null(dh);
        return None{};
      },
      [](nova::RowView<T> const& x) -> Result {
        if constexpr (nova::structured_type<T>) {
          return x;
        } else {
          return *x;
        }
      },
      [&](auto const&) -> Result {
        warn_type(nova::Type<T>::static_name, write_bytes::kind(value), dh);
        return None{};
      });
  }

  /// Returns the value at `row` as an unsigned integer, accepting
  /// non-negative signed integers as well.
  auto get_uint(nova::storage::Index row, diagnostic_handler& dh) -> uint64_t {
    TENZIR_ASSERT(fallback_);
    auto value = values_.get(row);
    return match(
      value,
      [&](nova::RowView<nova::Null>) {
        warn_null(dh);
        return *fallback_;
      },
      [](nova::RowView<nova::UInt> x) {
        return *x;
      },
      [&](nova::RowView<nova::Int> x) {
        if (*x < 0) {
          if (not std::exchange(warned_overflow_, true)) {
            diagnostic::warning("overflow in `{}`, got `{}`", name_, *x)
              .primary(expr_.get())
              .note("defaulting to `{}`", *fallback_)
              .emit(dh);
          }
          return *fallback_;
        }
        return static_cast<uint64_t>(*x);
      },
      [&](auto const&) {
        warn_type("int", write_bytes::kind(value), dh);
        return *fallback_;
      });
  }

  auto name() const -> std::string_view {
    return name_;
  }

  auto expr() const -> ast::expression const& {
    return *expr_;
  }

private:
  auto warn_null(diagnostic_handler& dh) -> void {
    if (not fallback_ or std::exchange(warned_null_, true)) {
      return;
    }
    diagnostic::warning("`{}` evaluated to `null`", name_)
      .primary(expr_.get())
      .note("defaulting to `{}`", *fallback_)
      .emit(dh);
  }

  auto warn_type(std::string_view expected, std::string_view actual,
                 diagnostic_handler& dh) -> void {
    if (not warned_types_.insert(actual).second) {
      return;
    }
    auto d = diagnostic::warning("`{}` must be `{}`, got `{}`", name_, expected,
                                 actual)
               .primary(expr_.get());
    if (fallback_) {
      d = std::move(d).note("defaulting to `{}`", *fallback_);
    }
    std::move(d).emit(dh);
  }

  std::string_view name_;
  Ref<ast::expression const> expr_;
  nova::Array<nova::Data> values_;
  Option<uint64_t> fallback_;
  bool warned_null_ = false;
  bool warned_overflow_ = false;
  std::set<std::string_view> warned_types_;
};

class WriteSyslogEvents final : public Operator<nova::Events, chunk_ptr> {
public:
  explicit WriteSyslogEvents(WriteSyslogArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    for (auto i = size_t{0}; i < fields.size(); ++i) {
      auto evaluator
        = co_await nova::Evaluator::make(args_.*fields[i].expr, ctx);
      if (not evaluator) {
        co_return;
      }
      evaluators_[i].emplace(std::move(*evaluator));
    }
  }

  auto process(nova::Events input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not evaluators_.back()) {
      co_return;
    }
    auto& dh = ctx.dh();
    auto column = [&](size_t i, Option<uint64_t> fallback = None{}) {
      auto const& expr = args_.*fields[i].expr;
      // Suppress diagnostics like missing fields for the implicit defaults.
      auto ndh = null_diagnostic_handler{};
      auto& eval_dh
        = expr.get_location() ? dh : static_cast<diagnostic_handler&>(ndh);
      auto values = evaluators_[i]->eval(input, nova::EvalCtx{eval_dh});
      return SyslogColumn{fields[i].name, expr, std::move(values), fallback};
    };
    auto facility = column(0, 1);
    auto severity = column(1, 6);
    auto timestamp = column(2);
    auto hostname = column(3);
    auto app_name = column(4);
    auto process_id = column(5);
    auto message_id = column(6);
    auto structured_data = column(7);
    auto message = column(8);
    auto buffer = std::vector<char>{};
    auto it = std::back_inserter(buffer);
    auto format_n
      = [&](SyslogColumn& column, nova::storage::Index row, size_t count) {
          auto str = column.get<nova::String>(row, dh);
          if (not str or str->empty()) {
            fmt::format_to(it, " -");
            return;
          }
          if (str->size() > count) {
            diagnostic::warning("`{}` must not be longer than {} characters",
                                column.name(), count)
              .primary(column.expr())
              .emit(dh);
          }
          fmt::format_to(it, " {}", str->substr(0, count));
        };
    for (auto row : nova::storage::true_bits(input.mask)) {
      // PRI and VERSION
      auto f = facility.get_uint(row, dh);
      auto s = severity.get_uint(row, dh);
      if (f > 23u) {
        diagnostic::warning("`facility` must be in the range 0 to 23, got `{}`",
                            f)
          .primary(args_.facility)
          .note("defaulting to `1`")
          .emit(dh);
        f = 1;
      }
      if (s > 7u) {
        diagnostic::warning("`severity` must be in the range 0 to 7, got `{}`",
                            s)
          .primary(args_.severity)
          .note("defaulting to `6`")
          .emit(dh);
        s = 6;
      }
      fmt::format_to(it, "<{}>{}", (f * 8) + s, 1);
      // TIMESTAMP
      if (auto t = timestamp.get<nova::Time>(row, dh)) {
        fmt::format_to(
          it, " {:%FT%TZ}",
          std::chrono::time_point_cast<std::chrono::microseconds>(*t));
      } else {
        fmt::format_to(it, " -");
      }
      // HOSTNAME, APP-NAME, PROCID, and MSGID
      format_n(hostname, row, 255);
      format_n(app_name, row, 48);
      format_n(process_id, row, 128);
      format_n(message_id, row, 32);
      // STRUCTURED-DATA
      auto sd = structured_data.get<nova::Record>(row, dh);
      if (sd and sd->begin() != sd->end()) {
        fmt::format_to(it, " ");
        for (auto [name, value] : *sd) {
          auto params = try_as<nova::RowView<nova::Record>>(value);
          if (not params) {
            diagnostic::warning("structured data `{}` must be of type `record`",
                                name)
              .primary(args_.structured_data)
              .note("skipping structured data `{}`", name)
              .emit(dh);
            continue;
          }
          fmt::format_to(it, "[{}", name);
          for (auto [k, v] : *params) {
            fmt::format_to(it, " {}=", k);
            format_param(it, k, v, dh);
          }
          fmt::format_to(it, "]");
        }
      } else {
        fmt::format_to(it, " -");
      }
      // MSG
      if (auto msg = message.get<nova::String>(row, dh)) {
        fmt::format_to(it, " {}", *msg);
      }
      buffer.push_back('\n');
    }
    if (not buffer.empty()) {
      co_await push(chunk::make(std::move(buffer)));
    }
  }

private:
  struct Field {
    std::string_view name;
    ast::expression WriteSyslogArgs::* expr;
  };

  /// The arguments in the order of the evaluators.
  static constexpr auto fields = std::array{
    Field{"facility", &WriteSyslogArgs::facility},
    Field{"severity", &WriteSyslogArgs::severity},
    Field{"timestamp", &WriteSyslogArgs::timestamp},
    Field{"hostname", &WriteSyslogArgs::hostname},
    Field{"app_name", &WriteSyslogArgs::app_name},
    Field{"process_id", &WriteSyslogArgs::process_id},
    Field{"message_id", &WriteSyslogArgs::message_id},
    Field{"structured_data", &WriteSyslogArgs::structured_data},
    Field{"message", &WriteSyslogArgs::message},
  };

  static auto format_escaped(auto& it, std::string_view x) -> void {
    *it++ = '"';
    for (auto c : x) {
      if (c == '\\' or c == '"' or c == ']') {
        *it++ = '\\';
      }
      *it++ = c;
    }
    *it++ = '"';
  }

  auto
  format_param(auto& it, std::string_view k, nova::RowView<nova::Data> const& v,
               diagnostic_handler& dh) const -> void {
    match(
      v,
      [&](nova::RowView<nova::Null>) {
        fmt::format_to(it, "\"\"");
      },
      [&](nova::RowView<nova::Int> x) {
        fmt::format_to(it, "\"{}\"", *x);
      },
      [&](nova::RowView<nova::UInt> x) {
        fmt::format_to(it, "\"{}\"", *x);
      },
      [&](nova::RowView<nova::String> x) {
        format_escaped(it, *x);
      },
      [&](nova::RowView<nova::Record> const&) {
        diagnostic::warning("`structured_data` field `{}` has type `record`", k)
          .primary(args_.structured_data)
          .emit(dh);
        fmt::format_to(it, "\"\"");
      },
      [&](nova::RowView<nova::List> const&) {
        diagnostic::warning("`structured_data` field `{}` has type `list`", k)
          .primary(args_.structured_data)
          .emit(dh);
        fmt::format_to(it, "\"\"");
      },
      [&](auto const& x) {
        format_escaped(it, fmt::format("{}", *x));
      });
  }

  WriteSyslogArgs args_;
  std::array<Option<nova::Evaluator>, fields.size()> evaluators_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_syslog";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteSyslogArgs, WriteSyslog, WriteSyslogEvents>{};
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
