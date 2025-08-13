//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/enumerate.hpp"

#include <algorithm>
#include <string_view>
#include <vector>

namespace tenzir::detail {

constexpr auto levenshtein
  = [](const std::string_view xs, const std::string_view ys) -> size_t {
  if (xs.empty() or ys.empty()) {
    return std::max(xs.size(), ys.size());
  }
  auto vs = std::vector(xs.size() + 1, std::vector<size_t>(ys.size() + 1, 0));
  for (auto&& [i, v] : detail::enumerate(vs)) {
    v.front() = i;
  }
  for (auto&& [i, v] : detail::enumerate(vs.front())) {
    v = i;
  }
  for (auto&& [i, x] : detail::enumerate(xs)) {
    for (auto&& [j, y] : detail::enumerate(ys)) {
      vs[i + 1][j + 1] = std::min({
        vs[i][j + 1] + 1,    // Deletion
        vs[i + 1][j] + 1,    // Insertion
        vs[i][j] + (x != y), // Substitution
      });
    }
  }
  return vs.back().back();
};

inline constexpr auto calculate_similarity(const std::string_view actual,
                                           const std::string_view guess)
  -> int64_t {
  auto score = -static_cast<int64_t>(levenshtein(actual, guess));
  if (const auto pos = guess.find(actual); pos != guess.npos) {
    score += actual.length();
  }
  return score;
};

} // namespace tenzir::detail
