//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/subnet.hpp>

namespace tenzir::detail {

class subnet_tree {
public:
  /// Constructs an empty tree.
  subnet_tree();

  /// Destroys a tree.
  ~subnet_tree();

  /// Looks for a value for a given key.
  /// @note Unlike `search`, this performs an exact match and not a
  /// longest-prefix match.
  auto lookup(subnet key) const -> const data*;

  /// Looks for the longest-prefix match of a subnet in which the given IP
  /// address occurs.
  auto match(ip key) const -> const data*;

  /// Looks for the longest-prefix match of a subnet.
  auto match(subnet key) const -> const data*;

  /// Performs a prefix-search for a given IP address, returning all subnets
  /// that contain it.
  auto search(ip key) const
    -> generator<std::pair<subnet, const data*>>;

  /// Performs a prefix-search for a given subnet, returning all subnets that
  /// contain it.
  auto search(subnet key) const
    -> generator<std::pair<subnet, const data*>>;

  /// Retrieves all nodes in the tree.
  auto nodes() const -> generator<std::pair<subnet, const data*>>;

  /// Inserts a key-value pair.
  auto insert(subnet key, data value) -> bool;

  /// Removes a node.
  auto erase(subnet key) -> bool;

  /// Removes all elements from the tree.
  auto clear() -> void;

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace tenzir::detail
