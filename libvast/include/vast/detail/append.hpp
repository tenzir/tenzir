//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace vast::detail {

/// Appends the contents of the second containers to the first.
template <class Container>
void append(Container& xs, Container&& ys) {
  auto begin = std::make_move_iterator(ys.begin());
  auto end = std::make_move_iterator(ys.end());
  xs.insert(xs.end(), begin, end);
}

} // namespace vast::detail
