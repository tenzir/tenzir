//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/table_slice.hpp"

#include <ranges>
#include <vector>

namespace tenzir {

template <template <class...> class Container = std::vector,
          std::ranges::forward_range Rng>
  requires(std::same_as<std::ranges::range_value_t<Rng>, table_slice>)
auto rebatch(Rng events, size_t max_size = defaults::import::table_slice_size)
  -> Container<table_slice> {
  TENZIR_ASSERT(max_size > 0);
  auto results = Container<table_slice>{};
  auto start = events.begin();
  auto rows = start->rows();
  const auto end = events.end();
  for (auto it = std::next(start); it < end; ++it) {
    rows += it->rows();
    if (it->schema() == start->schema() and rows < max_size) {
      continue;
    }
    results.push_back(concatenate({start, it}));
    start = it;
    rows = start->rows();
  }
  results.push_back(concatenate({start, end}));
  return results;
}

} // namespace tenzir
