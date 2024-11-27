//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series.hpp"

#include "tenzir/checked_math.hpp"

namespace tenzir {

auto map_series(multi_series x, std::function<auto(series)->multi_series> f)
  -> multi_series {
  auto input = std::vector<multi_series>{};
  input.push_back(std::move(x));
  return map_series(input, [&](std::vector<series> x) {
    TENZIR_ASSERT(x.size() == 1);
    return f(std::move(x[0]));
  });
}

void iter_parts(const multi_series& x, std::function<void(series)> f) {
  // TODO: This is not even a map, is it?
  for (auto& part : x.parts()) {
    f(part);
  }
}

auto map_series(multi_series input1, multi_series input2,
                std::function<auto(series, series)->multi_series> f)
  -> multi_series {
  auto input = std::vector<multi_series>{};
  input.reserve(2);
  input.push_back(std::move(input1));
  input.push_back(std::move(input2));
  return map_series(input, [&](std::vector<series> x) {
    TENZIR_ASSERT(x.size() == 2);
    return f(std::move(x[0]), std::move(x[1]));
  });
}

auto map_series(std::span<const multi_series> input,
                std::function<auto(std::vector<series>)->multi_series> f)
  -> multi_series {
  auto result = std::vector<series>{};
  for (auto x : split_multi_series(input)) {
    auto y = f(std::move(x));
    result.insert(result.end(), std::move_iterator{y.parts().begin()},
                  std::move_iterator{y.parts().end()});
  }
  return multi_series{std::move(result)};
}

auto split_multi_series(std::span<const multi_series> input)
  -> generator<std::vector<series>> {
  TENZIR_ASSERT(not input.empty());
  auto length = input[0].length();
  for (auto& ms : input) {
    TENZIR_ASSERT(ms.length() == length);
  }
  // Initialize.
  auto positions = std::vector<std::pair<size_t, int64_t>>{};
  positions.resize(input.size());
  while (true) {
    // Find the shortest remaining length.
    // TODO: min_element.
    auto shortest_length = max<int64_t>;
    for (auto i = size_t{0}; i < input.size(); ++i) {
      auto [part, start] = positions[i];
      if (part > input[i].parts().size()) {
        // TODO: Assert that everything is done.
        co_return;
      }
      auto length = input[i].part(part).length() - start;
      if (length < shortest_length) {
        shortest_length = length;
      }
    }
    // Split everything to the shortest length.
    auto result = std::vector<series>{};
    for (auto i = size_t{0}; i < input.size(); ++i) {
      auto& [part, start] = positions[i];
      result.push_back(input[i].part(part).slice(start, shortest_length));
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
    co_yield std::move(result);
  }
}

auto split_multi_series(multi_series input) -> generator<series> {
  for (auto& part : input.parts()) {
    co_yield part;
  }
}

} // namespace tenzir
