//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/detail/generator.hpp>

namespace vast::detail {

/// A utility function to collect all results produced by a
/// `vast::detail::generator<T>` into a suitable standard container.
/// Example:
///     auto g = vast::detail::generator<int>{};
///     auto v = vast::detail::collect<std::vector>(g);
template <template <class, class> class Container, class Generated>
Container<Generated, std::allocator<Generated>>
collect(vast::detail::generator<Generated> g) {
  Container<Generated, std::allocator<Generated>> result;
  for (auto& x : g)
    result.push_back(x);
  return result;
}

} // namespace vast::detail
