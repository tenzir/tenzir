//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

class operator_factory_plugin : public virtual plugin {
public:
  // Separate from `ast::expression` in case we want to add things.
  struct invocation {
    ast::entity self;
    std::vector<ast::expression> args;
  };

  virtual auto make(invocation inv, session ctx) const -> operator_ptr = 0;
};

} // namespace tenzir

namespace tenzir::tql2 {

template <class Operator>
class operator_plugin : public virtual operator_factory_plugin,
                        public virtual operator_inspection_plugin<Operator> {};

class function_plugin : public virtual plugin {
public:
  struct invocation {
    invocation(const ast::function_call& self, int64_t length,
               std::vector<series> args)
      : self{self}, length{length}, args{std::move(args)} {
    }

    const ast::function_call& self;
    int64_t length;
    std::vector<series> args;
  };

  virtual auto eval(invocation inv, diagnostic_handler& dh) const -> series = 0;
};

class aggregation_instance {
public:
  struct add_info {
    add_info(const ast::entity& self, located<series> arg)
      : self{self}, arg{std::move(arg)} {
    }

    const ast::entity& self;
    located<series> arg;
  };

  virtual ~aggregation_instance() = default;

  /// Can return error string (TODO: Not the best).
  virtual void add(add_info info, diagnostic_handler& dh) = 0;

  virtual auto finish() -> data = 0;
};

class aggregation_function_plugin : public virtual function_plugin {
public:
  auto eval(invocation inv, diagnostic_handler& dh) const -> series final;

  virtual auto make_aggregation() const -> std::unique_ptr<aggregation_instance>
    = 0;
};

} // namespace tenzir::tql2
