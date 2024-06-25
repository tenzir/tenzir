//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

class operator_factory_plugin : public virtual plugin {
public:
  // Separate from `ast::invocation` in case we want to add things.
  struct invocation {
    ast::entity self;
    std::vector<ast::expression> args;
  };

  virtual auto make(invocation inv, session ctx) const -> operator_ptr = 0;
};

template <class Operator>
class operator_plugin2 : public virtual operator_factory_plugin,
                         public virtual operator_inspection_plugin<Operator> {};

class function_use {
public:
  virtual ~function_use() = default;

  // TODO: Improve this.
  class evaluator {
  public:
    evaluator(void* self) : self_{self} {};

    auto operator()(const ast::expression& expr) const -> series;

    auto length() const -> int64_t;

  private:
    void* self_;
  };

  virtual auto run(evaluator eval, session ctx) const -> series = 0;

  static auto make(std::function<auto(evaluator eval, session ctx)->series> f)
    -> std::unique_ptr<function_use>;
};

class function_plugin : public virtual plugin {
public:
  using evaluator = function_use::evaluator;

  struct invocation {
    const ast::function_call& call;
  };

  virtual auto make_function(invocation inv, session ctx) const
    -> std::unique_ptr<function_use>
    = 0;
};

class aggregation_instance {
public:
  virtual ~aggregation_instance() = default;

  virtual void update(const table_slice& input, session ctx) = 0;

  virtual auto finish() -> data = 0;
};

class aggregation_plugin : public virtual function_plugin {
public:
  auto make_function(invocation inv, session ctx) const
    -> std::unique_ptr<function_use> override;

  virtual auto make_aggregation(invocation inv, session ctx) const
    -> std::unique_ptr<aggregation_instance>
    = 0;
};

} // namespace tenzir

// TODO: Change this.
#include "tenzir/argument_parser2.hpp"
#include "tenzir/session.hpp"
