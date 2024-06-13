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

#include <any>

namespace tenzir::detail {

class type_erased_subnet_tree {
public:
  type_erased_subnet_tree() noexcept;
  type_erased_subnet_tree(const type_erased_subnet_tree& other) noexcept
    = delete;
  auto operator=(const type_erased_subnet_tree& other) noexcept
    -> type_erased_subnet_tree& = delete;
  type_erased_subnet_tree(type_erased_subnet_tree&& other) noexcept;
  auto operator=(type_erased_subnet_tree&& other) noexcept
    -> type_erased_subnet_tree&;
  ~type_erased_subnet_tree() noexcept;
  auto lookup(subnet key) const -> const std::any*;
  auto lookup(subnet key) -> std::any*;
  auto match(ip key) const -> std::pair<subnet, const std::any*>;
  auto match(ip key) -> std::pair<subnet, std::any*>;
  auto match(subnet key) const -> std::pair<subnet, const std::any*>;
  auto match(subnet key) -> std::pair<subnet, std::any*>;
  auto search(ip key) const -> generator<std::pair<subnet, const std::any*>>;
  auto search(ip key) -> generator<std::pair<subnet, std::any*>>;
  auto search(subnet key) const
    -> generator<std::pair<subnet, const std::any*>>;
  auto search(subnet key) -> generator<std::pair<subnet, std::any*>>;
  auto nodes() const -> generator<std::pair<subnet, const std::any*>>;
  auto nodes() -> generator<std::pair<subnet, std::any*>>;
  auto insert(subnet key, std::any value) -> bool;
  auto erase(subnet key) -> bool;
  auto clear() -> void;

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

template <class T = data>
class subnet_tree {
public:
  subnet_tree() noexcept = default;
  subnet_tree(const subnet_tree& other) = delete;
  auto operator=(const subnet_tree& other) -> subnet_tree& = delete;
  subnet_tree(subnet_tree&& other) noexcept = default;
  auto operator=(subnet_tree&& other) noexcept -> subnet_tree& = default;
  ~subnet_tree() noexcept = default;

  /// Looks for a value for a given key.
  /// @note Unlike `search`, this performs an exact match and not a
  /// longest-prefix match.
  auto lookup(subnet key) const -> const T* {
    return std::any_cast<T>(impl_.lookup(key));
  }

  /// Looks for a value for a given key.
  /// @note Unlike `search`, this performs an exact match and not a
  /// longest-prefix match.
  auto lookup(subnet key) -> T* {
    return std::any_cast<T>(impl_.lookup(key));
  }

  /// Looks for the longest-prefix match of a subnet in which the given IP
  /// address occurs.
  auto match(ip key) const -> std::pair<subnet, const T*> {
    auto [subnet, value] = impl_.match(key);
    return {subnet, std::any_cast<T>(value)};
  }

  /// Looks for the longest-prefix match of a subnet in which the given IP
  /// address occurs.
  auto match(ip key) -> std::pair<subnet, T*> {
    auto [subnet, value] = impl_.match(key);
    return {subnet, std::any_cast<T>(value)};
  }

  /// Looks for the longest-prefix match of a subnet.
  auto match(subnet key) const -> std::pair<subnet, const T*> {
    auto [subnet, value] = impl_.match(key);
    return {subnet, std::any_cast<T>(value)};
  }

  /// Looks for the longest-prefix match of a subnet.
  auto match(subnet key) -> std::pair<subnet, T*> {
    auto [subnet, value] = impl_.match(key);
    return {subnet, std::any_cast<T>(value)};
  }

  /// Performs a prefix-search for a given IP address, returning all subnets
  /// that contain it.
  auto search(ip key) const -> generator<std::pair<subnet, const T*>> {
    for (auto&& [key, value] : impl_.search(key)) {
      co_yield {key, std::any_cast<T>(value)};
    }
  }

  /// Performs a prefix-search for a given IP address, returning all subnets
  /// that contain it.
  auto search(ip key) -> generator<std::pair<subnet, T*>> {
    for (auto&& [key, value] : impl_.search(key)) {
      co_yield {key, std::any_cast<T>(value)};
    }
  }

  /// Performs a prefix-search for a given subnet, returning all subnets that
  /// contain it.
  auto search(subnet key) const -> generator<std::pair<subnet, const T*>> {
    for (auto&& [key, value] : impl_.search(key)) {
      co_yield {key, std::any_cast<T>(value)};
    }
  }

  /// Performs a prefix-search for a given subnet, returning all subnets that
  /// contain it.
  auto search(subnet key) -> generator<std::pair<subnet, T*>> {
    for (auto&& [key, value] : impl_.search(key)) {
      co_yield {key, std::any_cast<T>(value)};
    }
  }

  /// Retrieves all nodes in the tree.
  auto nodes() const -> generator<std::pair<subnet, const T*>> {
    for (auto&& [key, value] : impl_.nodes()) {
      co_yield {key, std::any_cast<T>(value)};
    }
  }

  /// Retrieves all nodes in the tree.
  auto nodes() -> generator<std::pair<subnet, T*>> {
    for (auto&& [key, value] : impl_.nodes()) {
      co_yield {key, std::any_cast<T>(value)};
    }
  }

  /// Inserts a key-value pair.
  auto insert(subnet key, T value) -> bool {
    return impl_.insert(key, std::move(value));
  }

  /// Removes a node.
  auto erase(subnet key) -> bool {
    return impl_.erase(key);
  }

  /// Removes all elements from the tree.
  auto clear() -> void {
    impl_.clear();
  }

private:
  type_erased_subnet_tree impl_;
};

} // namespace tenzir::detail
