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
#include "tenzir/tql2/context.hpp"

namespace tenzir::tql2 {

class operator_factory_plugin : public virtual plugin {
public:
  virtual auto
  make_operator(ast::entity self, std::vector<ast::expression> args,
                tql2::context& ctx) const -> operator_ptr
    = 0;
};

template <class Operator>
class operator_plugin : public virtual operator_factory_plugin,
                        public virtual operator_inspection_plugin<Operator> {};

} // namespace tenzir::tql2
