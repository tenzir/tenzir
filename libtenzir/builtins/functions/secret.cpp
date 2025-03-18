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

#include <ranges>

namespace tenzir::plugins::secret {

namespace {

class from_config final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "secret::from_config";
  }

  auto initialize(const record& plugin_config,
                  const record& global_config) -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    auto secrets = try_get_or(global_config, "tenzir.secrets", record{});
    if (not secrets) {
      return diagnostic::error(secrets.error())
        .note("configuration key `tenzir.secrets` must be a record")
        .to_error();
    }
    for (const auto& [key, value] : *secrets) {
      const auto* str = try_as<std::string>(&value);
      if (not str) {
        return diagnostic::error("secrets must be strings")
          .note("configuration key `tenzir.secrets.{}` is of type `{}`", key,
                type::infer(value).value_or(type{}).kind())
          .to_error();
      }
      secrets_.emplace(key, *str);
    }
    return {};
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("secret::from_config")
          .positional("key", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [this, expr = std::move(expr)](evaluator eval, session ctx) -> series {
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
                const auto it = secrets_.find(array.GetView(i));
                if (it == secrets_.end()) {
                  diagnostic::warning("unknown secret `{}`", array.GetView(i))
                    .primary(expr)
                    .emit(ctx);
                  check(b.AppendNull());
                  continue;
                }
                check(append_builder(
                  secret_type{}, b, secret_view{it->second, array.GetView(i)}));
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

private:
  detail::heterogeneous_string_hashmap<std::string> secrets_ = {};
};

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

class to_plain_string final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "secret::_to_plain_string";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("secret::_to_plain_string")
          .positional("value", expr, "secret")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = type_to_arrow_builder_t<string_type>{};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const secret_type::array_type& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
#ifdef NDEBUG
                diagnostic::warning(
                  "`secret::_to_plain_string` is onl available in debug mode")
                  .primary(inv.call.get_location());
                check(b.AppendNull());
#else
                check(b.Append(value_at(secret_type{}, array, i)
                                 .unsafe_get_value_be_careful()));
#endif
              }
            },
            [&](const arrow::NullArray&) {
              check(b.AppendNulls(value.length()));
            },
            [&](const auto&) {
              diagnostic::warning("expected `secret`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(value.length()));
            },
          };
          match(*value.array, f);
        }
        return series{string_type{}, finish(b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::secret

TENZIR_REGISTER_PLUGIN(tenzir::plugins::secret::from_config)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::secret::from_string)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::secret::to_plain_string)
