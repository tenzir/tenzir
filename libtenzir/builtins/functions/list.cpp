//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view.hpp>
#include <tenzir/view3.hpp>

#include <arrow/compute/api.h>

namespace tenzir::plugins::list {

namespace {

class prepend : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "prepend";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
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

  auto make_function(invocation inv, session ctx) const
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

  auto make_function(invocation inv, session ctx) const
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

  auto make_function(invocation inv, session ctx) const
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
      auto add_impl = [&](series list, series element) -> series {
        // Handle null list case
        if (is<null_type>(list.type)) {
          // If list is null, create a new list with just the element
          auto builder = series_builder{type{list_type{element.type}}};
          for (auto i = int64_t{0}; i < list.length(); ++i) {
            builder.list();
            auto elem_idx = i < element.length() ? i : element.length() - 1;
            auto elem_values = values3(*element.array);
            auto elem_it = elem_values.begin();
            std::advance(elem_it, elem_idx);
            if (elem_it != elem_values.end()) {
              auto val = *elem_it;
              if (! is<caf::none_t>(val)) {
                // TODO: Convert data_view3 to data_view2 for series_builder
                // For now, skip adding the element
              }
            }
          }
          return builder.finish_assert_one_array();
        }
        // Get the list type
        auto list_type_ptr = try_as<list_type>(list.type);
        if (! list_type_ptr) {
          diagnostic::warning("expected `list`, but got `{}`", list.type.kind())
            .primary(list_expr)
            .emit(ctx);
          return list;
        }
        // Determine output type - if list contains only nulls, promote to
        // element type
        auto output_type = list.type;
        if (is<null_type>(list_type_ptr->value_type())) {
          output_type = type{list_type{element.type}};
        }
        // Build result list with the appropriate type
        auto builder = series_builder{output_type};
        // TODO: Implement the actual add logic
        return list;
      };
      return map_series(eval(list_expr), eval(element_expr), add_impl);
    });
  }
};

class remove_from_list : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "remove";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
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
        auto list_type_ptr = try_as<list_type>(list.type);
        if (! list_type_ptr) {
          diagnostic::warning("expected `list`, but got `{}`", list.type.kind())
            .primary(list_expr)
            .emit(ctx);
          return list;
        }
        // Build result list with the same type as input
        auto builder = series_builder{list.type};
        // TODO: Implement the actual remove logic
        // For now, just return the original list
        return list;
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

  auto make_function(invocation inv, session ctx) const
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
            [](int64_t count) -> generator<std::optional<view<tenzir::list>>> {
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
                record_builder.field("left").data((*left_value)->at(i));
              }
              if (right_value and i < right_value->size()) {
                record_builder.field("right").data((*right_value)->at(i));
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

} // namespace

} // namespace tenzir::plugins::list

using namespace tenzir::plugins::list;
TENZIR_REGISTER_PLUGIN(prepend)
TENZIR_REGISTER_PLUGIN(append)
TENZIR_REGISTER_PLUGIN(concatenate)
TENZIR_REGISTER_PLUGIN(add)
TENZIR_REGISTER_PLUGIN(remove_from_list)
TENZIR_REGISTER_PLUGIN(zip)
