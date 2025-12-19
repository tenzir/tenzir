//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plan/operator.hpp"
#include "tenzir/uuid.hpp"

namespace tenzir::plan {

/// An executable pipeline is just a sequence of executable operators.
///
/// TODO: Can we assume that it is well-typed?
class pipeline {
public:
  pipeline() = default;

  explicit(false) pipeline(std::vector<operator_ptr> operators)
    : operators_{std::move(operators)} {
  }

  template <std::derived_from<operator_base> T>
  explicit(false) pipeline(std::unique_ptr<T> ptr) {
    operators_.push_back(std::move(ptr));
  }

  auto begin() {
    return operators_.begin();
  }

  auto end() {
    return operators_.end();
  }

  auto unwrap() && -> std::vector<operator_ptr> {
    return std::move(operators_);
  }

  auto operator[](size_t index) -> operator_ptr& {
    return operators_[index];
  }

  auto id() const -> uuid {
    // FIXME: Remove?
    return uuid{};
  }

  auto size() const -> size_t {
    return operators_.size();
  }

  friend auto inspect(auto& f, pipeline& x) -> bool {
    // TODO: Tests?
    return f.apply(x.operators_);
    // return f.object(x).fields(f.field("id", x.id_),
    //                           f.field("operators", x.operators_));
  }

private:
  // uuid id_ = uuid::random();
  std::vector<operator_ptr> operators_;
};

}; // namespace tenzir::plan
