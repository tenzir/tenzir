//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/detail/zip_iterator.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::secret {

namespace {

class from_string final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "secret::from_string";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("secret::from_string")
          .positional("value", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = secret_type::builder_type{};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::StringArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(append_builder(secret_type{}, b,
                                     secret_view{array.GetView(i)}));
              }
            },
            [&](const arrow::NullArray&) {
              check(b.AppendNulls(value.length()));
            },
            [&](const auto&) {
              diagnostic::warning("expected `string`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(value.length()));
            },
          };
          match(*value.array, f);
        }
        return series{secret_type{}, finish(b)};
      });
  }
};

class lookup final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "secret::_lookup";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("secret")
          .positional("name", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = secret_type::builder_type{};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::StringArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(append_builder(secret_type{}, b,
                                     {array.GetView(i),
                                      secret_source_type::managed}));
              }
            },
            [&](const auto&) {
              diagnostic::warning("expected `string`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(value.length()));
            },
          };
          match(*value.array, f);
        }
        return series{secret_type{}, finish(b)};
      });
  }
};

class secret final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.secret";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto name = std::string{};
    TRY(argument_parser2::function("secret")
          .positional("name", name)
          .parse(inv, ctx));
    return function_use::make(
      [name = std::move(name)](evaluator eval, session) -> series {
        auto b = secret_type::builder_type{};
        check(b.Reserve(eval.length()));
        for (int64_t i = 0; i < eval.length(); ++i) {
          check(append_builder(secret_type{}, b,
                               {name, secret_source_type::managed}));
        }
        return series{secret_type{}, finish(b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::secret

TENZIR_REGISTER_PLUGIN(tenzir::plugins::secret::from_string)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::secret::lookup)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::secret::secret)
