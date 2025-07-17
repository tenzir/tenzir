//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <algorithm>
#include <iterator>

namespace tenzir::detail {

template <class T>
T intersect(const T& xs, const T& ys) {
  T result;
  std::set_intersection(xs.begin(), xs.end(), ys.begin(), ys.end(),
                        std::back_inserter(result));
  return result;
}

template <class T>
void inplace_intersect(T& result, const T& xs) {
  // Adapted from https://stackoverflow.com/a/1773620/1170277.
  auto i = result.begin();
  auto j = xs.begin();
  while (i != result.end() && j != xs.end()) {
    if (*i < *j) {
      i = result.erase(i);
    } else if (*i > *j) {
      ++j;
    } else {
      ++i;
      ++j;
    }
  }
  result.erase(i, result.end());
}

template <class T>
auto unify(const T& xs, const T& ys) {
  T result;
  result.reserve(std::min(xs.size(), ys.size()));
  std::set_union(xs.begin(), xs.end(), ys.begin(), ys.end(),
                 std::back_inserter(result));
  return result;
}

template <class T>
void inplace_unify(T& result, T xs) {
  // Adapted from https://stackoverflow.com/a/3633142/1170277.
  auto n = result.size();
  result.insert(result.end(),
                std::make_move_iterator(xs.begin()),
                std::make_move_iterator(xs.end()));
  std::inplace_merge(result.begin(), result.begin() + n, result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
}

} // namespace tenzir::detail
