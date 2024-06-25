//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/arrow_utils.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::time_ {

namespace {

class time_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.time";
  }

  auto make_function(invocation inv, session ctx) const
    -> std::unique_ptr<function_use> override {
    auto expr = ast::expression{};
    argument_parser2::function("time").add(expr, "<string>").parse(inv, ctx);
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
              .primary(expr.get_location())
              .emit(ctx);
            return series::null(time_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

class seconds_since_epoch final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.seconds_since_epoch";
  }

  auto make_function(invocation inv, session ctx) const
    -> std::unique_ptr<function_use> override {
    auto expr = ast::expression{};
    argument_parser2::function("seconds_since_epoch")
      .add(expr, "<time>")
      .parse(inv, ctx);
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            return series::null(double_type{}, arg.length());
          },
          [&](const arrow::TimestampArray& arg) {
            auto& ty = caf::get<arrow::TimestampType>(*arg.type());
            TENZIR_ASSERT(ty.unit() == arrow::TimeUnit::NANO);
            TENZIR_ASSERT(ty.timezone().empty());
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
            diagnostic::warning("`time` expected `time`, but got `{}`",
                                arg.type.kind())
              .primary(expr.get_location())
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
    -> std::unique_ptr<function_use> override {
    argument_parser2::function("now").parse(inv, ctx);
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::time_)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::seconds_since_epoch)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::now)
