//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

namespace tenzir::plugins::secrets {

namespace {

class secret final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.secret";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto initialize(const record&, const record& global_config)
    -> caf::error override {
    const auto v
      = try_get_or(global_config, "tenzir.legacy-secret-model", false);
    if (not v) {
      return diagnostic::error("`tenzir.legacy-secret-model` must be a boolean")
        .to_error();
    }
    legacy_ = *v;
    if (not legacy_) {
      return {};
    }
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

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto name = ast::expression{};
    auto literal = false;
    TRY(argument_parser2::function("secret")
          .positional("name", name, "string")
          .named_optional("_literal", literal)
          .parse(inv, ctx));
    if (legacy_) {
      return function_use::make([this, expr = std::move(name), literal](
                                  evaluator eval, session ctx) -> series {
        auto b = arrow::StringBuilder{};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::StringArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                if (literal) {
                  check(b.Append(array.GetView(i)));
                }
                const auto it = secrets_.find(array.GetView(i));
                if (it == secrets_.end()) {
                  diagnostic::warning("unknown secret `{}`", array.GetView(i))
                    .primary(expr)
                    .emit(ctx);
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(it->second));
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
        return series{string_type{}, finish(b)};
      });
    } else {
      return function_use::make([expr = std::move(name), literal](
                                  evaluator eval, session ctx) -> series {
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
                check(append_builder(
                  secret_type{}, b,
                  literal ? ::tenzir::secret::make_literal(array.GetView(i))
                          : ::tenzir::secret::make_managed(array.GetView(i))));
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
  }

private:
  bool legacy_ = false;
  detail::heterogeneous_string_hashmap<std::string> secrets_ = {};
};

class dump_repr final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "_dump_repr";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  static auto dump_repr_impl(const tenzir::secret_view& s) -> std::string {
    const auto f = detail::overload{
      [&](const fbs::data::SecretLiteral& x) -> std::string {
        return fmt::format("lit({})", x.value()->string_view());
      },
      [&](const fbs::data::SecretName& x) -> std::string {
        return fmt::format("name({})", x.value()->string_view());
      },
      [&](this const auto& self,
          const fbs::data::SecretConcatenation& x) -> std::string {
        auto res = std::string{};
        res += fmt::format("concat(");
        for (const auto* e : *x.secrets()) {
          res += match(*e, self);
          res += fmt::format(",");
        }
        res += fmt::format(")");
        return res;
      },
      [&](this const auto& self,
          const fbs::data::SecretTransformed& x) -> std::string {
        auto res = std::string{};
        res += fmt::format("trafo(");
        res += match(*x.secret(), self);
        res += fmt::format(
          ",{})", fbs::data::EnumNameSecretTransformations(x.transformation()));
        return res;
      },
    };
    return match(s, f);
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("s", expr, "secret")
          .parse(inv, ctx));
    return function_use::make([expr](evaluator eval, session ctx) -> series {
      auto b = arrow::StringBuilder{};
      for (auto& value : eval(expr)) {
        auto f = detail::overload{
          [&](const secret_type::array_type& array) {
            for (auto v : values3(array)) {
              if (not v) {
                check(b.AppendNull());
                continue;
              }
              check(b.Append(dump_repr_impl(*v)));
            }
          },
          [&](const arrow::NullArray&) {
            check(b.AppendNulls(eval.length()));
          },
          [&](const auto&) {
            diagnostic::warning("expected `secret`, got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
          },
        };
        match(*value.array, f);
      }
      return series{string_type{}, finish(b)};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::secrets

TENZIR_REGISTER_PLUGIN(tenzir::plugins::secrets::secret)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::secrets::dump_repr)
