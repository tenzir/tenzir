//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

namespace tenzir::plugins::list {

namespace {

class prepend : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "prepend";
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
      [list = std::move(list),
       element = std::move(element)](evaluator eval, session) -> series {
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

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto list = ast::expression{};
    auto element = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", list, "list")
          .positional("x", element, "any")
          .parse(inv, ctx));
    return function_use::make(
      [list = std::move(list),
       element = std::move(element)](evaluator eval, session) -> series {
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

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto list1 = ast::expression{};
    auto list2 = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", list1, "list")
          .positional("ys", list2, "list")
          .parse(inv, ctx));
    return function_use::make(
      [list1 = std::move(list1), list2 = std::move(list2)](evaluator eval,
                                                           session) -> series {
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

} // namespace

} // namespace tenzir::plugins::list

using namespace tenzir::plugins::list;
TENZIR_REGISTER_PLUGIN(prepend)
TENZIR_REGISTER_PLUGIN(append)
TENZIR_REGISTER_PLUGIN(concatenate)
