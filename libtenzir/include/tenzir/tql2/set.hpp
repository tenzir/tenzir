//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

class set_operator final : public crtp_operator<set_operator> {
public:
  set_operator() = default;
  ~set_operator() = default;
  set_operator(const set_operator&) = delete;
  set_operator(set_operator&&) = delete;
  auto operator=(const set_operator&) -> set_operator& = delete;
  auto operator=(set_operator&&) -> set_operator& = delete;

  explicit set_operator(std::vector<ast::assignment> assignments)
    : assignments_{std::move(assignments)} {
  }

  auto name() const -> std::string override {
    return "tql2.set";
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<table_slice>;

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, set_operator& x) -> bool {
    return f.apply(x.assignments_);
  }

private:
  std::vector<ast::assignment> assignments_;
};

} // namespace tenzir
