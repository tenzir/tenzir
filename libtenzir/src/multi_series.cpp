//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series.hpp"

namespace tenzir {

auto split_multi_series(std::span<const multi_series> input,
                        std::span<series> output) -> generator<std::monostate> {
  TENZIR_ASSERT(input.size() == output.size());
  if (input.empty()) {
    co_yield {};
    co_return;
  }
  auto length = input[0].length();
  for (auto& ms : input) {
    TENZIR_ASSERT(ms.length() == length);
  }
  // This vectors holds pairs `(part_index, row_index)`, where `row_index` is
  // relative to the beginning of the part itself.
  auto positions = std::vector<std::pair<size_t, int64_t>>{};
  positions.resize(input.size());
  while (true) {
    // Find the shortest remaining length.
    // TODO: min_element.
    auto shortest_length = std::numeric_limits<int64_t>::max();
    for (auto i = size_t{0}; i < input.size(); ++i) {
      auto [part, start] = positions[i];
      if (part >= input[i].parts().size()) {
        // TODO: Assert that everything is done.
        co_return;
      }
      auto length = input[i].part(part).length() - start;
      if (length < shortest_length) {
        shortest_length = length;
      }
    }
    // Split everything to the shortest length.
    for (auto i = size_t{0}; i < input.size(); ++i) {
      auto& [part, start] = positions[i];
      output[i] = input[i].part(part).slice(start, shortest_length);
      // Adjust the position.
      auto length = input[i].part(part).length() - start;
      if (length > shortest_length) {
        start += shortest_length;
      } else {
        TENZIR_ASSERT(length == shortest_length);
        part += 1;
        start = 0;
      }
    }
    co_yield {};
  }
}

auto split_multi_series(std::span<const multi_series> input)
  -> generator<std::span<series>> {
  auto output = std::vector<series>{};
  output.resize(input.size());
  for (auto _ : split_multi_series(input, output)) {
    TENZIR_UNUSED(_);
    co_yield output;
  }
}

auto map_series(std::span<const multi_series> input,
                detail::function_view<auto(std::span<series>)->multi_series> f)
  -> multi_series {
  auto result = std::vector<series>{};
  for (auto x : split_multi_series(input)) {
    auto y = f(x);
    result.insert(result.end(), std::move_iterator{y.begin()},
                  std::move_iterator{y.end()});
  }
  return multi_series{std::move(result)};
}

auto map_series(multi_series x,
                detail::function_view<auto(series)->multi_series> f)
  -> multi_series {
  auto result = std::vector<series>{};
  for (auto& part : x) {
    auto mapped = f(std::move(part));
    result.insert(result.end(), std::move_iterator{mapped.begin()},
                  std::move_iterator{mapped.end()});
  }
  return multi_series{std::move(result)};
}

auto map_series(multi_series x, multi_series y,
                detail::function_view<auto(series, series)->multi_series> f)
  -> multi_series {
  auto input = std::array<multi_series, 2>{std::move(x), std::move(y)};
  return map_series(input, [&](std::span<series> output) {
    TENZIR_ASSERT(output.size() == 2);
    return f(std::move(output[0]), std::move(output[1]));
  });
}

} // namespace tenzir
