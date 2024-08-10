//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <boost/process/environment.hpp>

namespace tenzir::plugins::misc {

namespace {

class type_id final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.type_id";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("type_id")
          .add(expr, "<value>")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        TENZIR_UNUSED(ctx);
        auto value = eval(expr);
        // TODO: This is a 64-bit hex-encoded hash. We could also use just use
        // an integer for this.
        auto type_id = value.type.make_fingerprint();
        auto b = arrow::StringBuilder{};
        check(b.Reserve(value.length()));
        for (auto i = int64_t{0}; i < value.length(); ++i) {
          check(b.Append(type_id));
        }
        return {string_type{}, finish(b)};
      });
  }
};

class secret final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.secret";
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    auto secrets = try_get_or(global_config, "tenzir.secrets", record{});
    if (not secrets) {
      return diagnostic::error(secrets.error())
        .note("configuration key `tenzir.secrets` must be a record")
        .to_error();
    }
    secrets_ = std::move(*secrets);
    for (auto& [key, value] : secrets_) {
      if (not caf::holds_alternative<std::string>(value)) {
        return diagnostic::error("secrets must be strings")
          .note("configuration key `tenzir.secrets.{}` is of type `{}`", key,
                type::infer(value).value_or(type{}).kind())
          .to_error();
      }
    }
    return {};
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(
      argument_parser2::function("secret").add(expr, "<key>").parse(inv, ctx));
    return function_use::make([this, expr = std::move(expr)](
                                evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto value = eval(expr);
      auto f = detail::overload{
        [&](const arrow::StringArray& array) {
          auto b = arrow::StringBuilder{};
          check(b.Reserve(value.length()));
          for (auto i = int64_t{0}; i < array.length(); ++i) {
            if (array.IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            const auto it = secrets_.find(array.GetString(i));
            if (it == secrets_.end()) {
              diagnostic::warning("unknown secret `{}`", array.GetView(i))
                .primary(expr)
                .emit(ctx);
              check(b.AppendNull());
              continue;
            }
            const auto* secret = caf::get_if<std::string>(&it->second);
            TENZIR_ASSERT(secret);
            check(b.Append(*secret));
          }
          return series{string_type{}, finish(b)};
        },
        [&](const arrow::NullArray&) {
          return series::null(string_type{}, value.length());
        },
        [&](const auto&) {
          diagnostic::warning("expected `string`, got `{}`", value.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(string_type{}, value.length());
        },
      };
      return caf::visit(f, *value.array);
    });
  }

private:
  record secrets_ = {};
};

class env final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.env";
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    TENZIR_UNUSED(global_config);
    env_ = boost::this_process::environment();
    return {};
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("env").add(expr, "<key>").parse(inv, ctx));
    return function_use::make([this, expr = std::move(expr)](
                                evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto value = eval(expr);
      auto f = detail::overload{
        [&](const arrow::StringArray& array) {
          auto b = arrow::StringBuilder{};
          check(b.Reserve(value.length()));
          for (auto i = int64_t{0}; i < array.length(); ++i) {
            if (array.IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            const auto it = env_.find(array.GetString(i));
            if (it == env_.end()) {
              check(b.AppendNull());
              continue;
            }
            check(b.Append(it->to_string()));
          }
          return series{string_type{}, finish(b)};
        },
        [&](const arrow::NullArray&) {
          return series::null(string_type{}, value.length());
        },
        [&](const auto&) {
          diagnostic::warning("expected `string`, got `{}`", value.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(string_type{}, value.length());
        },
      };
      return caf::visit(f, *value.array);
    });
  }

private:
  boost::process::native_environment env_ = {};
};

class length final : public method_plugin {
public:
  auto name() const -> std::string override {
    return "length";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::method(name()).add(expr, "<expr>").parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        TENZIR_UNUSED(ctx);
        auto value = eval(expr);
        auto f = detail::overload{
          [&]<concepts::one_of<arrow::StringArray, arrow::ListArray> T>(
            const T& array) {
            auto b = arrow::Int64Builder{};
            check(b.Reserve(value.length()));
            for (auto i = int64_t{0}; i < array.length(); ++i) {
              if (array.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              check(b.Append(array.value_length(i)));
            }
            return series{int64_type{}, finish(b)};
          },
          [&](const arrow::NullArray&) {
            return series::null(int64_type{}, value.length());
          },
          [&](const auto&) {
            diagnostic::warning("expected `list` or `string`, got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(int64_type{}, value.length());
          },
        };
        return caf::visit(f, *value.array);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::misc

TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::type_id)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::secret)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::env)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::length)
