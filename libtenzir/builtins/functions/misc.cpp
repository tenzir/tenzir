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
#include <tenzir/tql2/plugin.hpp>

#include <boost/process/environment.hpp>

#include <ranges>

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
    for (const auto& [key, value] : *secrets) {
      const auto* str = caf::get_if<std::string>(&value);
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
  detail::heterogeneous_string_hashmap<std::string> secrets_ = {};
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
    for (const auto& entry : boost::this_process::environment()) {
      env_.emplace(entry.get_name(), entry.to_string());
    }
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
            const auto it = env_.find(array.GetView(i));
            if (it == env_.end()) {
              check(b.AppendNull());
              continue;
            }
            check(b.Append(it->second));
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
  detail::heterogeneous_string_hashmap<std::string> env_ = {};
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
          [&](const arrow::ListArray& array) {
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
          [&]<class T>(const T&) {
            auto d = diagnostic::warning("expected `list`, got `{}`",
                                         value.type.kind())
                       .primary(expr);
            if constexpr (std::same_as<T, arrow::StringArray>) {
              d = std::move(d).hint(
                "use `.length_bytes()` or `.length_chars()` instead");
            }
            std::move(d).emit(ctx);
            return series::null(int64_type{}, value.length());
          },
        };
        return caf::visit(f, *value.array);
      });
  }
};

class has final : public method_plugin {
public:
  auto name() const -> std::string override {
    return "has";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto needle = located<std::string>{};
    TRY(argument_parser2::method(name())
          .add(expr, "<expr>")
          .add(needle, "<string>")
          .parse(inv, ctx));
    return function_use::make([needle = std::move(needle),
                               expr = std::move(expr)](evaluator eval,
                                                       session ctx) -> series {
      auto value = eval(expr);
      auto f = detail::overload{
        [&](const arrow::NullArray& array) -> series {
          return series::null(bool_type{}, array.length());
        },
        [&](const arrow::StructArray& array) -> series {
          const auto names = array.struct_type()->fields()
                             | std::views::transform(&arrow::Field::name);
          const auto result
            = std::ranges::find(names, needle.inner) != std::end(names);
          auto b = bool_type::make_arrow_builder(arrow::default_memory_pool());
          check(b->Reserve(array.length()));
          for (auto i = int64_t{0}; i < array.length(); i++) {
            if (array.IsNull(i)) {
              check(b->AppendNull());
              continue;
            }
            check(b->Append(result));
          }
          return {bool_type{}, finish(*b)};
        },
        [&](const auto&) -> series {
          diagnostic::warning("expected `record`, got `{}`", value.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(bool_type{}, value.length());
        }};
      return caf::visit(f, *value.array);
    });
  }
};

class select_drop_matching final : public method_plugin {
public:
  explicit select_drop_matching(bool select) : select_{select} {
  }

  auto name() const -> std::string override {
    return select_ ? "select_matching" : "drop_matching";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto str = located<std::string>{};
    TRY(argument_parser2::method(name())
          .add(expr, "<expr>")
          .add(str, "<pattern>")
          .parse(inv, ctx));
    auto pattern = pattern::make(str.inner);
    if (not pattern) {
      diagnostic::error(pattern.error()).primary(str.source).emit(ctx);
      return failure::promise();
    }
    return function_use::make([pattern = std::move(*pattern),
                               expr = std::move(expr), select = select_](
                                evaluator eval, session ctx) -> series {
      auto value = eval(expr);
      auto f = detail::overload{
        [&](const arrow::NullArray& array) -> series {
          return series::null(null_type{}, array.length());
        },
        [&](const arrow::StructArray& array) -> series {
          auto arrays = arrow::ArrayVector{};
          auto fields = arrow::FieldVector{};
          for (const auto& [field, array] :
               detail::zip(array.struct_type()->fields(), array.fields())) {
            if (pattern.search(field->name()) == select) {
              fields.push_back(field);
              arrays.push_back(array);
            }
          }
          auto result = std::make_shared<arrow::StructArray>(
            arrow::struct_(fields), array.length(), std::move(arrays),
            array.null_bitmap(), array.null_count(), array.offset());
          return {type::from_arrow(*result->type()), std::move(result)};
        },
        [&](const auto&) -> series {
          diagnostic::warning("expected `record`, got `{}`", value.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(null_type{}, value.length());
        }};
      return caf::visit(f, *value.array);
    });
  }

private:
  bool select_ = {};
};

} // namespace

} // namespace tenzir::plugins::misc

TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::type_id)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::secret)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::env)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::length)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::has)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::select_drop_matching{true})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::select_drop_matching{false})
