//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::time_ {

// TODO: gcc emits a bogus -Wunused-function warning for this macro when used
// inside an anonymous namespace.
TENZIR_ENUM(ymd_subtype, year, month, day);

namespace {

class time_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.time";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(
      argument_parser2::function("time").add(expr, "<string>").parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            return series::null(time_type{}, arg.length());
          },
          [&](const arrow::TimestampArray&) {
            return arg;
          },
          [](const arrow::StringArray& arg) {
            auto b = arrow::TimestampBuilder{
              std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
              arrow::default_memory_pool()};
            check(b.Reserve(arg.length()));
            for (auto i = 0; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto result = tenzir::time{};
              if (parsers::time(arg.GetView(i), result)) {
                check(b.Append(result.time_since_epoch().count()));
              } else {
                // TODO: ?
                check(b.AppendNull());
              }
            }
            return series{time_type{}, finish(b)};
          },
          [&](const auto&) {
            diagnostic::warning("`time` expected `string`, but got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(time_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

class since_epoch final : public method_plugin {
public:
  auto name() const -> std::string override {
    return "since_epoch";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<time>").parse(inv, ctx));
    return function_use::make([expr = std::move(expr),
                               this](evaluator eval, session ctx) -> series {
      auto arg = eval(expr);
      auto f = detail::overload{
        [](const arrow::NullArray& arg) {
          return series::null(duration_type{}, arg.length());
        },
        [&](const arrow::TimestampArray& arg) {
          auto& ty = caf::get<arrow::TimestampType>(*arg.type());
          TENZIR_ASSERT(ty.timezone().empty());
          auto b
            = duration_type::make_arrow_builder(arrow::default_memory_pool());
          check(b->Reserve(arg.length()));
          for (auto i = 0; i < arg.length(); ++i) {
            if (arg.IsNull(i)) {
              check(b->AppendNull());
              continue;
            }
            check(
              append_builder(duration_type{}, *b,
                             value_at(time_type{}, arg, i).time_since_epoch()));
          }
          return series{duration_type{}, finish(*b)};
        },
        [&](const auto&) {
          diagnostic::warning("`{}` expected `time`, but got `{}`", name(),
                              arg.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(duration_type{}, arg.length());
        },
      };
      return caf::visit(f, *arg.array);
    });
  }
};

class year_month_day final : public method_plugin {
public:
  explicit year_month_day(ymd_subtype field) : ymd_subtype_(field) {
  }

  auto name() const -> std::string override {
    return std::string{to_string(ymd_subtype_)};
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<time>").parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), this](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            return series::null(int64_type{}, arg.length());
          },
          [&](const arrow::TimestampArray& arg) {
            auto& ty = caf::get<arrow::TimestampType>(*arg.type());
            TENZIR_ASSERT(ty.timezone().empty());
            auto b = arrow::Int64Builder{};
            check(b.Reserve(arg.length()));
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto&& value = value_at(time_type{}, arg, i);
              const std::chrono::year_month_day ymd{
                std::chrono::floor<std::chrono::days>(value)};
              auto result = int64_t{0};
              switch (ymd_subtype_) {
                case ymd_subtype::year:
                  result = static_cast<int>(ymd.year());
                  break;
                case ymd_subtype::month:
                  result = static_cast<unsigned>(ymd.month());
                  break;
                case ymd_subtype::day:
                  result = static_cast<unsigned>(ymd.day());
                  break;
              }
              check(append_builder(int64_type{}, b, result));
            }
            return series{int64_type{}, finish(b)};
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `time`, but got `{}`", name(),
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(int64_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }

private:
  ymd_subtype ymd_subtype_;
};

class as_secs final : public method_plugin {
public:
  auto name() const -> std::string override {
    return "as_secs";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .add(expr, "<duration>")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), this](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            return series::null(double_type{}, arg.length());
          },
          [&](const arrow::DurationArray& arg) {
            auto& ty = caf::get<arrow::DurationType>(*arg.type());
            TENZIR_ASSERT(ty.unit() == arrow::TimeUnit::NANO);
            auto factor = 1000 * 1000 * 1000;
            auto b = arrow::DoubleBuilder{};
            check(b.Reserve(arg.length()));
            for (auto i = 0; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto val = arg.Value(i);
              auto pre = static_cast<double>(val / factor);
              auto post = static_cast<double>(val % factor) / factor;
              check(b.Append(pre + post));
            }
            return series{double_type{}, finish(b)};
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `duration`, but got `{}`",
                                name(), arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(double_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

class now final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.now";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    TRY(argument_parser2::function("now").parse(inv, ctx));
    return function_use::make([](evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto result = time{time::clock::now()};
      auto b = series_builder{type{time_type{}}};
      for (auto i = int64_t{0}; i < eval.length(); ++i) {
        b.data(result);
      }
      return b.finish_assert_one_array();
    });
  }
};

} // namespace

} // namespace tenzir::plugins::time_

using namespace tenzir::plugins::time_;

TENZIR_REGISTER_PLUGIN(time_)
TENZIR_REGISTER_PLUGIN(since_epoch)
TENZIR_REGISTER_PLUGIN(as_secs)
TENZIR_REGISTER_PLUGIN(year_month_day{ymd_subtype::year});
TENZIR_REGISTER_PLUGIN(year_month_day{ymd_subtype::month});
TENZIR_REGISTER_PLUGIN(year_month_day{ymd_subtype::day});
TENZIR_REGISTER_PLUGIN(now)
