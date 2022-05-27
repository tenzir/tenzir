//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/detail/generator.hpp>

namespace vast::detail {

/// A utility function to collect all results produced by a
/// `vast::detail::generator<T>` into a suitable container.
/// Example:
///     auto g = vast::detail::generator<string_view>{};
///     auto v = vast::detail::collect<std::vector<std::string>>(g);
template <class Container, class T>
  requires requires(Container c, T& t) {
    c.reserve(size_t{});
    c.emplace(c.end(), t);
  }
Container collect(vast::detail::generator<T> g, size_t size_hint = 0) {
  Container result = {};
  if (size_hint)
    result.reserve(result.size() + size_hint);
  for (auto&& x : g)
    result.emplace(result.end(), std::move(x));
  return result;
}

/// A utility function to collect all results produced by a
/// `vast::detail::generator<T>` into a `std::vector<T>`.
/// Example:
///     auto g = vast::detail::generator<int>{};
///     auto v = vast::detail::collect(g);
template <class T>
std::vector<T> collect(vast::detail::generator<T> g, size_t size_hint = 0) {
  return collect<std::vector<T>>(std::move(g), size_hint);
}

} // namespace vast::detail
