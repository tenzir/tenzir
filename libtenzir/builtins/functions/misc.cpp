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
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api_scalar.h>
#include <boost/process/v2/environment.hpp>

#include <ranges>

namespace tenzir::plugins::misc {

namespace {

class type_id final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.type_id";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("type_id")
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        TENZIR_UNUSED(ctx);
        auto value = eval(expr);
        // TODO: This is a 64-bit hex-encoded hash. We could also use just use
        // an integer for this.
        auto b = arrow::StringBuilder{};
        check(b.Reserve(eval.length()));
        for (auto& part : value.parts()) {
          auto type_id = part.type.make_fingerprint();
          for (auto i = int64_t{0}; i < value.length(); ++i) {
            check(b.Append(type_id));
          }
        }
        return {string_type{}, finish(b)};
      });
  }
};

class type_of final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "type_of";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("type_of")
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> multi_series {
        TENZIR_UNUSED(ctx);
        return map_series(eval(expr), [](auto&& x) {
          auto builder = series_builder{};
          auto definition = x.type.to_definition();
          for (auto i = int64_t{0}; i < x.length(); ++i) {
            builder.data(definition);
          }
          return builder.finish_assert_one_array();
        });
      });
  }
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
    for (const auto& entry : boost::process::v2::environment::current()) {
      env_.emplace(entry.key().string(), entry.value().string());
    }
    return {};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("env")
          .positional("key", expr, "string")
          .parse(inv, ctx));
    if (auto key = try_const_eval(expr, ctx)) {
      auto value = std::optional<std::string>{};
      const auto* typed_key = try_as<std::string>(*key);
      if (not typed_key) {
        diagnostic::warning("expected `string`, got `{}`",
                            type::infer(key).value_or(type{}).kind())
          .primary(expr)
          .emit(ctx);
      } else if (auto it = env_.find(*typed_key); it != env_.end()) {
        value = it->second;
      }
      return function_use::make(
        [value = std::move(value)](evaluator eval, session ctx) -> series {
          TENZIR_UNUSED(ctx);
          if (not value) {
            return series::null(string_type{}, eval.length());
          }
          return {
            string_type{},
            check(arrow::MakeArrayFromScalar(arrow::StringScalar{*value},
                                             eval.length(),
                                             tenzir::arrow_memory_pool())),
          };
        });
    }
    return function_use::make(
      [this, expr = std::move(expr)](evaluator eval, session ctx) -> series {
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
                const auto it = env_.find(array.GetView(i));
                if (it == env_.end()) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(it->second));
              }
            },
            [&](const arrow::NullArray& array) {
              check(b.AppendNulls(array.length()));
            },
            [&](const auto& array) {
              diagnostic::warning("expected `string`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(array.length()));
            },
          };
          match(*value.array, f);
        }
        return series{string_type{}, finish(b)};
      });
  }

private:
  detail::heterogeneous_string_hashmap<std::string> env_ = {};
};

class length final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "length";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "list")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::Int64Builder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::ListArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(array.value_length(i)));
              }
            },
            [&](const arrow::NullArray& array) {
              check(b.AppendNulls(array.length()));
            },
            [&]<class T>(const T& array) {
              auto d = diagnostic::warning("expected `list`, got `{}`",
                                           value.type.kind())
                         .primary(expr);
              if constexpr (std::same_as<T, arrow::StringArray>) {
                d = std::move(d).hint(
                  "use `.length_bytes()` or `.length_chars()` instead");
              }
              std::move(d).emit(ctx);
              check(b.AppendNulls(array.length()));
            },
          };
          match(*value.array, f);
        }
        return series{int64_type{}, finish(b)};
      });
  }
};

class is_empty final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "is_empty";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "string|list|record")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::StringArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(array.value_length(i) == 0));
              }
            },
            [&](const arrow::ListArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(array.value_length(i) == 0));
              }
            },
            [&](const arrow::StructArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                // Records are empty if they have no fields
                check(b.Append(array.num_fields() == 0));
              }
            },
            [&](const arrow::NullArray& array) {
              check(b.AppendNulls(array.length()));
            },
            [&]<class T>(const T& array) {
              diagnostic::warning("expected `string`, `list`, or `record`, got "
                                  "`{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(array.length()));
            },
          };
          match(*value.array, f);
        }
        return series{bool_type{}, finish(b)};
      });
  }
};

class network final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "network";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "subnet")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        TENZIR_UNUSED(ctx);
        auto value = eval(expr);
        if (value.parts().size() == 1) {
          if (auto subnets = value.part(0).as<subnet_type>()) {
            return series{
              ip_type{},
              check(subnets->array->storage()->GetFlattenedField(
                0, tenzir::arrow_memory_pool())),
            };
          }
        }
        auto b = ip_type::make_arrow_builder(arrow_memory_pool());
        check(b->Reserve(eval.length()));
        for (auto& value : value) {
          auto f = detail::overload{
            [&](const subnet_type::array_type& array) {
              check(append_array(*b, ip_type{},
                                 as<ip_type::array_type>(
                                   *check(array.storage()->GetFlattenedField(
                                     0, tenzir::arrow_memory_pool())))));
            },
            [&](const arrow::NullArray& array) {
              check(b->AppendNulls(array.length()));
            },
            [&]<class T>(const T& array) {
              diagnostic::warning("expected `subnet`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b->AppendNulls(array.length()));
            },
          };
          match(*value.array, f);
        }
        return series{ip_type{}, finish(*b)};
      });
  }
};

class has final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "has";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto needle = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .positional("field", needle, "string")
          .parse(inv, ctx));
    if (auto const_needle = try_const_eval(needle, ctx)) {
      auto* str = try_as<std::string>(*const_needle);
      if (not str) {
        diagnostic::error("expected `string`, but got `{}`",
                          type::infer(*const_needle).value_or(type{}).kind())
          .primary(needle)
          .emit(ctx);
      }
      return function_use::make(
        [needle = located{std::move(*str), needle.get_location()},
         expr = std::move(expr)](evaluator eval, session ctx) -> series {
          auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
          check(b.Reserve(eval.length()));
          for (auto value : eval(expr)) {
            auto f = detail::overload{
              [&](const arrow::NullArray& array) {
                check(b.AppendNulls(array.length()));
              },
              [&](const arrow::StructArray& array) {
                const auto names = array.struct_type()->fields()
                                   | std::views::transform(&arrow::Field::name);
                const auto result
                  = std::ranges::find(names, needle.inner) != std::end(names);
                for (auto i = int64_t{0}; i < array.length(); i++) {
                  if (array.IsNull(i)) {
                    check(b.AppendNull());
                    continue;
                  }
                  check(b.Append(result));
                }
              },
              [&](const auto& array) {
                diagnostic::warning("expected `record`, got `{}`",
                                    value.type.kind())
                  .primary(expr)
                  .emit(ctx);
                check(b.AppendNulls(array.length()));
              }};
            match(*value.array, f);
          }
          return series{bool_type{}, finish(b)};
        });
    }
    return function_use::make([needle = std::move(needle), expr
                                                           = std::move(expr)](
                                evaluator eval, session ctx) -> multi_series {
      TENZIR_UNUSED(ctx);
      const auto expr_location = expr.get_location();
      const auto needle_location = needle.get_location();
      auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(builder.Reserve(eval.length()));
      for (auto split : split_multi_series(eval(expr), eval(needle))) {
        const auto& expr = split[0];
        const auto& needle = split[1];
        const auto* expr_type = try_as<record_type>(expr.type);
        if (not expr_type) {
          if (not is<null_type>(expr.type)) {
            diagnostic::warning("expected `record`, got `{}`", expr.type.kind())
              .primary(expr_location)
              .emit(ctx);
          }
          check(builder.AppendNulls(expr.length()));
          continue;
        }
        const auto typed_needle = needle.as<string_type>();
        if (not typed_needle) {
          diagnostic::warning("expected `string`, got `{}`", needle.type.kind())
            .primary(needle_location)
            .emit(ctx);
          check(builder.AppendNulls(expr.length()));
          continue;
        }
        if (typed_needle->array->null_count() > 0) {
          diagnostic::warning("expected `string`, got `null`")
            .primary(needle_location)
            .emit(ctx);
        }
        for (auto value : *typed_needle->array) {
          if (not value) {
            check(builder.AppendNull());
            continue;
          }
          check(builder.Append(expr_type->has_field(*value)));
        }
      }
      return series{bool_type{}, finish(builder)};
    });
  }
};

class contains_null final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "contains_null";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session) -> series {
        auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (const auto& s : eval(expr)) {
          const auto& array = *s.array;
          auto mask = check(arrow::compute::IsNull(array));
          update_mask(mask, array);
          auto bool_array = mask.array_as<arrow::BooleanArray>();
          check(append_array(b, bool_type{}, *bool_array));
        }
        return series{bool_type{}, finish(b)};
      });
  }

  constexpr static auto
  update_mask(arrow::Datum& mask, const arrow::Array& array) -> void {
    if (const auto* sub = try_as<arrow::StructArray>(array)) {
      for (const auto& field : sub->fields()) {
        const auto fmask = check(arrow::compute::IsNull(*field));
        mask = check(arrow::compute::Or(mask, fmask));
        update_mask(mask, *field);
      }
      return;
    }
    if (const auto* sub = try_as<arrow::ListArray>(array)) {
      auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(b.Reserve(sub->length()));
      for (auto i = int64_t{}; i < sub->length(); ++i) {
        if (sub->IsValid(i)) {
          const auto& slice = sub->value_slice(i);
          b.UnsafeAppend(has_null(*slice));
          continue;
        }
        b.UnsafeAppend(true);
      }
      const auto lmask = finish(b);
      mask = check(arrow::compute::Or(mask, *lmask));
    }
  }

  constexpr static auto has_null(const arrow::Array& array) -> bool {
    if (array.null_count() != 0) {
      return true;
    }
    if (const auto* sub = try_as<arrow::StructArray>(array)) {
      for (const auto& field : sub->fields()) {
        if (has_null(*field)) {
          return true;
        }
      }
    }
    if (const auto* sub = try_as<arrow::ListArray>(array)) {
      for (auto i = int64_t{}; i < sub->length(); ++i) {
        const auto& slice = (sub->value_slice(i));
        if (has_null(*slice)) {
          return true;
        }
      }
    }
    return false;
  }
};

class keys final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "keys";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> multi_series {
        const auto result_type = list_type{string_type{}};
        return map_series(eval(expr), [&](auto&& subject) -> series {
          return match(
            subject.type,
            [&](const null_type&) {
              return series::null(result_type, eval.length());
            },
            [&](const record_type& type) {
              auto keys_builder = arrow::StringBuilder{};
              check(keys_builder.Reserve(
                detail::narrow<int64_t>(type.num_fields())));
              for (const auto& field : type.fields()) {
                check(keys_builder.Append(field.name));
              }
              return series{
                result_type,
                check(arrow::MakeArrayFromScalar(
                  arrow::ListScalar{
                    finish(keys_builder),
                    result_type.to_arrow_type(),
                  },
                  eval.length(), tenzir::arrow_memory_pool())),
              };
            },
            [&](const auto&) {
              diagnostic::warning("expected `record`, got `{}`",
                                  subject.type.kind())
                .primary(expr)
                .emit(ctx);
              return series::null(result_type, eval.length());
            });
        });
      });
  }
};

class select_drop_matching final : public function_plugin {
public:
  explicit select_drop_matching(bool select) : select_{select} {
  }

  auto name() const -> std::string override {
    return select_ ? "select_matching" : "drop_matching";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto str = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .positional("regex", str)
          .parse(inv, ctx));
    auto pattern = pattern::make(str.inner);
    if (not pattern) {
      diagnostic::error(pattern.error()).primary(str.source).emit(ctx);
      return failure::promise();
    }
    return function_use::make([pattern = std::move(*pattern),
                               expr = std::move(expr),
                               select = select_](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series value) {
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
            diagnostic::warning("expected `record`, got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, value.length());
          }};
        return match(*value.array, f);
      });
    });
  }

private:
  bool select_ = {};
};

class merge final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "merge";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto record1 = ast::expression{};
    auto record2 = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", record1, "record")
          .positional("y", record2, "record")
          .parse(inv, ctx));
    return function_use::make(
      [record1 = std::move(record1),
       record2 = std::move(record2)](evaluator eval, session) {
        return eval(ast::record{
          location::unknown,
          {
            ast::spread{
              location::unknown,
              record1,
            },
            ast::spread{
              location::unknown,
              record2,
            },
          },
          location::unknown,
        });
      });
  }
};

class get final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "get";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject = ast::expression{};
    auto field = ast::expression{};
    auto fallback = std::optional<ast::expression>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject, "record|list")
          .positional("field", field, "string|int")
          .positional("fallback", fallback, "any")
          .parse(inv, ctx));
    return function_use::make(
      [subject = std::move(subject), field = std::move(field),
       fallback = std::move(fallback)](evaluator eval,
                                       session ctx) mutable -> multi_series {
        TENZIR_UNUSED(ctx);
        auto expr = ast::expression{
          ast::index_expr{
            subject,
            location::unknown,
            field,
            location::unknown,
            // We suppress warnings iff there is a fallback value provided.
            fallback.has_value(),
          },
        };
        if (fallback) {
          expr = ast::expression{
            ast::binary_expr{
              std::move(expr),
              located{ast::binary_op::else_, location::unknown},
              std::move(*fallback),
            },
          };
        }
        return eval(expr);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::misc

TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::env)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::get)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::has)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::contains_null)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::is_empty)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::keys)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::length)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::merge)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::network)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::select_drop_matching{false})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::select_drop_matching{true})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::type_id)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::type_of)
