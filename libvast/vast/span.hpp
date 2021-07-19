//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <algorithm>
#include <span>

namespace vast {

// TODO: Did not implement all comparison and relational operators as it is
// tedious and not needed yet in this codebase.
// See https://brevzin.github.io/c++/2020/03/30/span-comparisons/
template <class T, std::size_t LHSExtent, class U, std::size_t RHSExtent>
constexpr bool operator==(const std::span<T, LHSExtent>& lhs,
                          const std::span<U, RHSExtent>& rhs) {
  return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

template <class T, std::size_t LHSExtent, class U, std::size_t RHSExtent>
constexpr bool operator!=(const std::span<T, LHSExtent>& lhs,
                          const std::span<U, RHSExtent>& rhs) {
  return !(lhs == rhs);
}
} // namespace vast
