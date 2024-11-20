//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"

#include <algorithm>
#include <vector>

namespace tenzir::detail {

template <class T>
concept has_contains = requires(T& t, typename T::value_type& x) {
  requires concepts::container<T>;
  { t.contains(x) } -> std::convertible_to<bool>;
};

template <std::ranges::range T, class U>
bool contains(const T& t, const U& x) {
  if constexpr (has_contains<T>) {
    return t.contains(x);
  } else {
    return std::find(t.begin(), t.end(), x) != t.end();
  }
}

template <class Collection>
auto unique_values(const Collection& xs) {
  std::vector<typename Collection::mapped_type> result;
  result.reserve(xs.size());
  for (auto& x : xs) {
    result.emplace_back(x.second);
  }
  std::sort(result.begin(), result.end());
  auto e = std::unique(result.begin(), result.end());
  if (e != result.end()) {
    result.erase(e, result.end());
  }
  return result;
}

} // namespace tenzir::detail
