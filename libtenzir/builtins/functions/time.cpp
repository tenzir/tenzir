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

#include <arrow/compute/api.h>

namespace tenzir::plugins::time_ {

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

class strftime : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.strftime";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto format = located<std::string>{};
    auto locale = std::optional<located<std::string>>{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<time>")
          .add(format, "<format>")
          .add("locale", locale)
          .parse(inv, ctx));
    return function_use::make(
      [this, subject_expr = std::move(subject_expr), format = std::move(format),
       locale = std::move(locale)](evaluator eval, session ctx) -> series {
        auto result_type = string_type{};
        auto result_arrow_type
          = std::shared_ptr<arrow::DataType>{result_type.to_arrow_type()};
        auto subject = eval(subject_expr);
        auto f = detail::overload{
          [&](const arrow::TimestampArray& array) {
            auto options = arrow::compute::StrftimeOptions(
              format.inner,
              locale ? locale->inner : "C");
            auto result = arrow::compute::CallFunction("strftime",
                                                       {array}, &options);
            if (not result.ok()) {
              diagnostic::warning("{}", result.status().ToString())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            }
            return series{result_type, result.MoveValueUnsafe().make_array()};
          },
          [&](const arrow::NullArray& array) {
            return series::null(result_type, array.length());
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `time`, but got `{}`", name(),
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(result_type, subject.length());
          },
        };
        return caf::visit(f, *subject.array);
      });
  }
};

class strptime : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.strptime";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto format = located<std::string>{};
    auto locale = std::optional<located<std::string>>{};
    TRY(argument_parser2::method(name())
          .add(subject_expr, "<string>")
          .add(format, "<format>")
          .parse(inv, ctx));
    return function_use::make(
      [this, subject_expr = std::move(subject_expr), format = std::move(format),
       locale = std::move(locale)](evaluator eval, session ctx) -> series {
        auto result_type = time_type{};
        auto result_arrow_type
          = std::shared_ptr<arrow::DataType>{result_type.to_arrow_type()};
        auto subject = eval(subject_expr);
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            constexpr auto error_is_null = true;
            auto options = arrow::compute::StrptimeOptions(
              format.inner, arrow::TimeUnit::NANO, error_is_null);
            auto result = arrow::compute::CallFunction("strptime",
                                                       {array}, &options);
            if (not result.ok()) {
              diagnostic::warning("{}", result.status().ToString())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            }
            return series{result_type, result.MoveValueUnsafe().make_array()};
          },
          [&](const arrow::NullArray& array) {
            return series::null(result_type, array.length());
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `time`, but got `{}`", name(),
                                subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(result_type, subject.length());
          },
        };
        return caf::visit(f, *subject.array);
      });
  }
};
} // namespace

} // namespace tenzir::plugins::time_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::time_)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::since_epoch)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::as_secs)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::now)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::strftime)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::strptime)
