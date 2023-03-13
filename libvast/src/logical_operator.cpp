//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/logical_operator.hpp"

#include "vast/logical_pipeline.hpp"

namespace vast {

auto runtime_logical_operator::detached() const noexcept -> bool {
  return false;
}

auto runtime_logical_operator::done() const noexcept -> bool {
  return false;
}

auto runtime_logical_operator::copy() const noexcept -> logical_operator_ptr {
  const auto repr = to_string();
  auto result = logical_pipeline::parse(repr);
  if (not result)
    VAST_WARN("failed to copy logical pipeline '{}': {}", repr, result.error());
  return std::make_unique<logical_pipeline>(std::move(*result));
}

} // namespace vast
