//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/generator.hpp>

namespace vast {

/// A utility function to collect all results produced by a
/// `vast::generator<T>` into a suitable container.
/// Example:
///     auto g = vast::generator<string_view>{};
///     auto v = vast::collect<std::vector<std::string>>(g);
template <class Container, class T>
  requires requires(Container c, T& t) {
             c.reserve(size_t{});
             c.emplace(c.end(), t);
           }
Container collect(vast::generator<T> g, size_t size_hint = 0) {
  Container result = {};
  if (size_hint)
    result.reserve(size_hint);
  for (auto&& x : g)
    result.emplace(result.end(), std::move(x));
  return result;
}

/// A utility function to collect all results produced by a
/// `vast::generator<T>` into a `std::vector<T>`.
/// Example:
///     auto g = vast::generator<int>{};
///     auto v = vast::collect(g);
template <class T>
std::vector<T> collect(vast::generator<T> g, size_t size_hint = 0) {
  return collect<std::vector<T>>(std::move(g), size_hint);
}

} // namespace vast
