//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/tql2/ast.hpp"

#include <utility>
#include <vector>

namespace tenzir {

class function_plugin;
class function_use;
class operator_compiler_plugin;
class operator_factory_plugin;

struct operator_factory_invocation {
  operator_factory_invocation(ast::entity self,
                              std::vector<ast::expression> args)
    : self{std::move(self)}, args{std::move(args)} {
  }

  ast::entity self;
  std::vector<ast::expression> args;
};

struct function_invocation {
  explicit function_invocation(const ast::function_call& call) : call{call} {
  }
  ~function_invocation() = default;
  function_invocation(const function_invocation&) = delete;
  function_invocation(function_invocation&&) = default;
  auto operator=(const function_invocation&) -> function_invocation& = delete;
  auto operator=(function_invocation&&) -> function_invocation& = delete;

  const ast::function_call& call;
};

} // namespace tenzir
