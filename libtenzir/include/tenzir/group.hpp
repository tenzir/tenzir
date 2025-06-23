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

template <class Rng>
struct group_result {
  using value_type = typename std::ranges::range_value_t<Rng>;
  using difference_type = typename std::ranges::range_difference_t<Rng>;

  value_type value;
  difference_type begin;
  difference_type end;
};

// Adapts a forward range to produce groups of consecutive elements.
template <std::ranges::forward_range Rng>
auto group(Rng&& values) // NOLINT(cppcoreguidelines-missing-std-forward)
  -> generator<group_result<Rng>> {
  using result_type = group_result<Rng>;
  using difference_type = typename result_type::difference_type;
  auto it = std::ranges::begin(values);
  const auto end = std::ranges::end(values);
  if (it == end) {
    co_return;
  }
  auto current_begin = difference_type{0};
  auto current_pos = current_begin;
  auto current_value = *it;
  ++it;
  ++current_pos;
  while (it != end) {
    auto next_value = std::forward_like<Rng>(*it);
    if (next_value != current_value) {
      co_yield result_type{
        .value = current_value,
        .begin = current_begin,
        .end = current_pos,
      };
      current_value = std::move(next_value);
      current_begin = current_pos;
    }
    ++it;
    ++current_pos;
  }
  co_yield result_type{
    .value = current_value,
    .begin = current_begin,
    .end = current_pos,
  };
}

} // namespace tenzir
