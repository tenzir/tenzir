//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/generator.hpp>

#include <ranges>

namespace tenzir {

template <class T>
struct group_result {
  T value;
  int64_t begin;
  int64_t end;
};

// Adapts a generator to produce groups of consecutive elements.
template <std::ranges::forward_range Rng>
auto group(Rng&& values) // NOLINT(cppcoreguidelines-missing-std-forward)
  -> generator<group_result<std::ranges::range_value_t<Rng>>> {
  auto it = std::ranges::begin(values);
  const auto end = std::ranges::end(values);
  if (it == end) {
    co_return;
  }
  auto current_begin = int64_t{0};
  auto current_pos = current_begin;
  auto current_value = *it;
  ++it;
  ++current_pos;
  while (it != end) {
    if (*it != current_value) {
      co_yield group_result<std::ranges::range_value_t<Rng>>{
        .value = current_value,
        .begin = current_begin,
        .end = current_pos,
      };
      current_value = *it;
      current_begin = current_pos;
    }
    ++it;
    ++current_pos;
  }
  co_yield group_result<std::ranges::range_value_t<Rng>>{
    .value = current_value,
    .begin = current_begin,
    .end = current_pos,
  };
}

} // namespace tenzir
