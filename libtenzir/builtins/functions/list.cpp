//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/series_builder_view3.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view.hpp>
#include <tenzir/view3.hpp>

#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

namespace tenzir::plugins::list {

namespace {

enum class add_type_classification {
  ok,
  contains_enumeration,
  stringifies,
};

auto classify_add_types(const type& element_type) -> add_type_classification {
  if (element_type.kind().is<enumeration_type>()) {
    return add_type_classification::contains_enumeration;
  }
  if (const auto* rec = try_as<record_type>(element_type)) {
    for (const auto& field : rec->fields()) {
      if (classify_add_types(field.type)
          == add_type_classification::contains_enumeration) {
        return add_type_classification::contains_enumeration;
      }
    }
    return add_type_classification::ok;
  }
  if (const auto* list = try_as<list_type>(element_type)) {
    return classify_add_types(list->value_type());
  }
  if (const auto* map = try_as<map_type>(element_type)) {
    if (classify_add_types(map->key_type())
          == add_type_classification::contains_enumeration
        or classify_add_types(map->value_type())
             == add_type_classification::contains_enumeration) {
      return add_type_classification::contains_enumeration;
    }
  }
  return add_type_classification::ok;
}

auto classify_add_types(const type& list_element_type, const type& element_type)
  -> add_type_classification {
  if (classify_add_types(list_element_type)
        == add_type_classification::contains_enumeration
      or classify_add_types(element_type)
           == add_type_classification::contains_enumeration) {
    return add_type_classification::contains_enumeration;
  }
  auto result = add_type_classification::ok;
  const auto list_kind = list_element_type.kind();
  const auto element_kind = element_type.kind();
  const auto list_is_integer
    = list_kind == tag_v<int64_type> or list_kind == tag_v<uint64_type>;
  const auto element_is_integer
    = element_kind == tag_v<int64_type> or element_kind == tag_v<uint64_type>;
  if (list_is_integer and element_is_integer) {
    return result;
  }
  if (const auto* list_record = try_as<record_type>(list_element_type)) {
    const auto* element_record = try_as<record_type>(element_type);
    if (not element_record) {
      return add_type_classification::stringifies;
    }
    for (auto [name, element_field_type] : element_record->fields()) {
      const auto list_field = list_record->resolve_key(name);
      if (not list_field) {
        continue;
      }
      const auto nested_result = classify_add_types(
        list_record->field(*list_field).type, element_field_type);
      if (nested_result == add_type_classification::contains_enumeration) {
        return add_type_classification::contains_enumeration;
      }
      if (nested_result == add_type_classification::stringifies) {
        result = add_type_classification::stringifies;
      }
    }
    return result;
  }
  if (const auto* list = try_as<list_type>(list_element_type)) {
    const auto* element = try_as<list_type>(element_type);
    if (not element) {
      return add_type_classification::stringifies;
    }
    return classify_add_types(list->value_type(), element->value_type());
  }
  if (unify(list_element_type, element_type)) {
    return result;
  }
  return add_type_classification::stringifies;
}

auto emit_add_type_warning(add_type_classification classification,
                           const ast::expression* list_expr,
                           const ast::expression& element_expr, session ctx)
  -> bool {
  if (classification == add_type_classification::contains_enumeration) {
    auto warning
      = diagnostic::warning("`add` does not support enumeration elements");
    if (list_expr) {
      warning = std::move(warning).primary(*list_expr);
    }
    std::move(warning).primary(element_expr).emit(ctx);
    return false;
  }
  if (classification == add_type_classification::stringifies) {
    TENZIR_ASSERT(list_expr);
    diagnostic::warning("`add` may turn conflicting types into string")
      .primary(*list_expr, "list")
      .primary(element_expr, "element")
      .emit(ctx);
  }
  return true;
}

auto check_add_types(const type& element_type,
                     const ast::expression& element_expr, session ctx) -> bool {
  const auto classification = classify_add_types(element_type);
  return emit_add_type_warning(classification, nullptr, element_expr, ctx);
}

auto check_add_types(const type& list_element_type, const type& element_type,
                     const ast::expression& list_expr,
                     const ast::expression& element_expr, session ctx) -> bool {
  const auto classification
    = classify_add_types(list_element_type, element_type);
  return emit_add_type_warning(classification, &list_expr, element_expr, ctx);
}

class prepend : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "prepend";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto list = ast::expression{};
    auto element = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", list, "list")
          .positional("x", element, "any")
          .parse(inv, ctx));
    return function_use::make(
      [list = std::move(list), element
                               = std::move(element)](evaluator eval, session) {
        return eval(ast::list{
          location::unknown,
          {
            element,
            ast::spread{
              location::unknown,
              list,
            },
          },
          location::unknown,
        });
      });
  }
};

class append : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "append";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto list = ast::expression{};
    auto element = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", list, "list")
          .positional("x", element, "any")
          .parse(inv, ctx));
    return function_use::make(
      [list = std::move(list), element
                               = std::move(element)](evaluator eval, session) {
        return eval(ast::list{
          location::unknown,
          {
            ast::spread{
              location::unknown,
              list,
            },
            element,
          },
          location::unknown,
        });
      });
  }
};

class concatenate : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "concatenate";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto list1 = ast::expression{};
    auto list2 = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", list1, "list")
          .positional("ys", list2, "list")
          .parse(inv, ctx));
    return function_use::make(
      [list1 = std::move(list1), list2
                                 = std::move(list2)](evaluator eval, session) {
        return eval(ast::list{
          location::unknown,
          {
            ast::spread{
              location::unknown,
              list1,
            },
            ast::spread{
              location::unknown,
              list2,
            },
          },
          location::unknown,
        });
      });
  }
};

class add : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "add";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto list_expr = ast::expression{};
    auto element_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", list_expr, "list")
          .positional("x", element_expr, "any")
          .parse(inv, ctx));

    return function_use::make([list_expr = std::move(list_expr),
                               element_expr = std::move(element_expr)](
                                evaluator eval, session ctx) -> multi_series {
      const auto add_impl = [&](series list, series element) -> multi_series {
        // Handle null list case
        if (is<null_type>(list.type)) {
          if (not check_add_types(element.type, element_expr, ctx)) {
            return multi_series{list};
          }
          // If list is null, create a new list with just the element
          auto builder = series_builder{};
          for (const auto& v : values3(*element.array)) {
            add_to_builder(builder.list(), v);
          }
          return multi_series{builder.finish()};
        }
        auto list_list = list.as<list_type>();
        if (not list_list) {
          diagnostic::warning("expected `list`, but got `{}`", list.type.kind())
            .primary(list_expr)
            .emit(ctx);
          return multi_series{list};
        }
        if (not check_add_types(list_list->type.value_type(), element.type,
                                list_expr, element_expr, ctx)) {
          return multi_series{list};
        }
        auto builder = series_builder{};
        auto list_generator = values3(*list_list->array);
        auto element_generator = values3(*element.array);
        for (auto i = int64_t{0}; i < list.length(); ++i) {
          const auto l = *list_generator.next();
          auto e = *element_generator.next();
          if (not l) {
            builder.null();
            continue;
          }
          auto lb = builder.list();
          auto already_found = false;
          for (const auto& v : *l) {
            add_to_builder(lb, v);
            if (not already_found) {
              already_found
                = partial_order(v, e) == std::partial_ordering::equivalent;
            }
          }
          if (not already_found) {
            add_to_builder(lb, e);
          }
        }
        return multi_series{builder.finish()};
      };
      return map_series(eval(list_expr), eval(element_expr), add_impl);
    });
  }
};

class remove : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "remove";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto list_expr = ast::expression{};
    auto element_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", list_expr, "list")
          .positional("x", element_expr, "any")
          .parse(inv, ctx));

    return function_use::make([list_expr = std::move(list_expr),
                               element_expr = std::move(element_expr)](
                                evaluator eval, session ctx) -> multi_series {
      auto remove_impl = [&](series list, series element) -> series {
        // Handle null list case
        if (is<null_type>(list.type)) {
          return series::null(null_type{}, list.length());
        }
        // Get the list type
        auto list_list = list.as<list_type>();
        if (not list_list) {
          diagnostic::warning("expected `list`, but got `{}`", list.type.kind())
            .primary(list_expr)
            .emit(ctx);
          return list;
        }
        auto builder = series_builder{list.type};
        auto list_generator = values3(*list_list->array);
        auto element_generator = values3(*element.array);
        for (auto i = int64_t{0}; i < list.length(); ++i) {
          const auto l = *list_generator.next();
          const auto e = *element_generator.next();
          if (not l) {
            builder.null();
            continue;
          }
          auto lb = builder.list();
          for (const auto& v : *l) {
            const auto matches
              = partial_order(v, e) == std::partial_ordering::equivalent;
            if (not matches) {
              add_to_builder(lb, v);
            }
          }
        }
        return builder.finish_assert_one_array();
      };
      return map_series(eval(list_expr), eval(element_expr), remove_impl);
    });
  }
};

class zip final : public function_plugin {
public:
  struct arguments {
    ast::expression left;
    ast::expression right;
  };

  auto name() const -> std::string override {
    return "tql2.zip";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto args = arguments{};
    TRY(argument_parser2::function("zip")
          .positional("left", args.left, "list")
          .positional("right", args.right, "list")
          .parse(inv, ctx));
    return function_use::make([args = std::move(args)](
                                function_plugin::evaluator eval,
                                session ctx) -> multi_series {
      return map_series(
        eval(args.left), eval(args.right), [&](series left, series right) {
          const auto left_null = is<null_type>(left.type);
          const auto right_null = is<null_type>(right.type);
          if (left_null and right_null) {
            return series::null(list_type{null_type{}}, left.length());
          }
          auto left_list = left.as<list_type>();
          auto right_list = right.as<list_type>();
          if ((not left_list and not left_null)
              or (not right_list and not right_null)) {
            if (not left_list and not left_null) {
              diagnostic::warning("expected `list`, but got `{}`",
                                  left.type.kind())
                .primary(args.left)
                .emit(ctx);
            }
            if (not right_list and not right_null) {
              diagnostic::warning("expected `list`, but got `{}`",
                                  right.type.kind())
                .primary(args.right)
                .emit(ctx);
            }
            return series::null(list_type{null_type{}}, left.length());
          }
          auto builder = series_builder{type{list_type{record_type{
            {"left", left_null ? type{} : left_list->type.value_type()},
            {"right", right_null ? type{} : right_list->type.value_type()},
          }}}};
          const auto make_nulls =
            [](int64_t count) -> generator<std::optional<view3<tenzir::list>>> {
            for (auto i = int64_t{0}; i < count; ++i) {
              co_yield {};
            }
          };
          auto left_values = left_null ? make_nulls(right_list->length())
                                       : left_list->values();
          auto right_values = right_null ? make_nulls(left_list->length())
                                         : right_list->values();
          bool warn = false;
          for (auto i = int64_t{0}; i < left.length(); ++i) {
            auto left_value = check(left_values.next());
            auto right_value = check(right_values.next());
            if (not left_value and not right_value) {
              builder.null();
              continue;
            }
            auto list_builder = builder.list();
            warn = warn or not left_value or not right_value
                   or left_value->size() != right_value->size();
            const auto max_length
              = std::max(left_value ? left_value->size() : 0,
                         right_value ? right_value->size() : 0);
            for (auto i = size_t{0}; i < max_length; ++i) {
              auto record_builder = list_builder.record();
              if (left_value and i < left_value->size()) {
                record_builder.field("left").data(left_value->at(i));
              }
              if (right_value and i < right_value->size()) {
                record_builder.field("right").data(right_value->at(i));
              }
            }
          }
          TENZIR_ASSERT(not left_values.next());
          TENZIR_ASSERT(not right_values.next());
          if (warn) {
            diagnostic::warning("lists have different lengths")
              .note("filling missing values with `null`")
              .primary(args.left)
              .primary(args.right)
              .emit(ctx);
          }
          return builder.finish_assert_one_array();
        });
    });
  }
};

class deltas final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "deltas";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", expr, "list")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> multi_series {
        return map_series(eval(expr), [&](series subject) -> series {
          if (is<null_type>(subject.type)) {
            return series::null(list_type{null_type{}}, subject.length());
          }
          auto const lists = subject.as<list_type>();
          if (not lists) {
            diagnostic::warning("expected `list`, but got `{}`",
                                subject.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, subject.length());
          }
          auto const value_type = lists->type.value_type();
          auto const value_kind = value_type.kind();
          if (not value_kind.is_any<null_type, int64_type, uint64_type,
                                    double_type, duration_type, time_type>()) {
            diagnostic::warning("expected a list of numbers, durations, or "
                                "times, but got a list of `{}`",
                                value_kind)
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, subject.length());
          }
          auto const& array = *lists->array;
          auto const num_rows = array.length();
          // Gather the operands of all within-row adjacent pairs, so that
          // pairs spanning list boundaries are never evaluated and cannot
          // emit spurious warnings.
          auto lhs_indices = arrow::Int64Builder{};
          auto rhs_indices = arrow::Int64Builder{};
          for (auto row = int64_t{0}; row < num_rows; ++row) {
            if (array.IsNull(row)) {
              continue;
            }
            auto const row_begin = array.value_offset(row);
            auto const row_length = array.value_length(row);
            for (auto i = int64_t{1}; i < row_length; ++i) {
              check(lhs_indices.Append(row_begin + i));
              check(rhs_indices.Append(row_begin + i - 1));
            }
          }
          auto const num_deltas = lhs_indices.length();
          // Compute all differences in one shot by evaluating `l - r` over
          // the gathered operands. This reuses the binary operator kernels,
          // including `time - time -> duration` and the overflow checks.
          auto delta = series{};
          if (num_deltas > 0 and not value_kind.is<null_type>()) {
            auto const flat = array.values();
            auto const lhs
              = check(arrow::compute::Take(flat, finish(lhs_indices)));
            auto const rhs
              = check(arrow::compute::Take(flat, finish(rhs_indices)));
            auto const schema = type{"tenzir.deltas", record_type{
                                                        {"l", value_type},
                                                        {"r", value_type},
                                                      }};
            auto const slice = table_slice{
              arrow::RecordBatch::Make(schema.to_arrow_schema(), num_deltas,
                                       arrow::ArrayVector{lhs.make_array(),
                                                          rhs.make_array()}),
              schema,
            };
            auto const loc = expr.get_location();
            auto const sub = ast::expression{ast::binary_expr{
              ast::expression{ast::root_field{ast::identifier{"l", loc}}},
              ast::binary_op::sub,
              ast::expression{ast::root_field{ast::identifier{"r", loc}}},
            }};
            auto result = tenzir::eval(sub, slice, ctx.dh());
            TENZIR_ASSERT(result.parts().size() == 1);
            delta = std::move(result.part(0));
            TENZIR_ASSERT(delta.length() == num_deltas);
          }
          // When no pairs exist, derive the same bare type that the
          // subtraction kernels would produce, so that the output schema does
          // not depend on list lengths even for aliased element types.
          auto const delta_type = [&] {
            if (delta.array) {
              return delta.type;
            }
            if (value_kind.is_any<time_type, duration_type>()) {
              return type{duration_type{}};
            }
            if (value_kind.is<int64_type>()) {
              return type{int64_type{}};
            }
            if (value_kind.is<uint64_type>()) {
              return type{uint64_type{}};
            }
            if (value_kind.is<double_type>()) {
              return type{double_type{}};
            }
            return type{null_type{}};
          }();
          auto const result_type = list_type{delta_type};
          auto const builder
            = result_type.make_arrow_builder(arrow_memory_pool());
          auto delta_pos = int64_t{0};
          for (auto row = int64_t{0}; row < num_rows; ++row) {
            if (array.IsNull(row)) {
              check(builder->AppendNull());
              continue;
            }
            check(builder->Append());
            auto const row_length = array.value_length(row);
            if (row_length < 2) {
              continue;
            }
            if (value_kind.is<null_type>()) {
              check(builder->value_builder()->AppendNulls(row_length - 1));
              continue;
            }
            check(append_array_slice(*builder->value_builder(), delta_type,
                                     *delta.array, delta_pos, row_length - 1));
            delta_pos += row_length - 1;
          }
          return series{type{result_type}, finish(*builder)};
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::list

using namespace tenzir::plugins::list;
TENZIR_REGISTER_PLUGIN(prepend)
TENZIR_REGISTER_PLUGIN(append)
TENZIR_REGISTER_PLUGIN(concatenate)
TENZIR_REGISTER_PLUGIN(add)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::list::remove)
TENZIR_REGISTER_PLUGIN(zip)
TENZIR_REGISTER_PLUGIN(deltas)
