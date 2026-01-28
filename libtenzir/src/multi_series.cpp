//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series.hpp"

#include "tenzir/detail/flat_map.hpp"
#include "tenzir/series_builder.hpp"

#include <ranges>

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
    auto shortest_length = std::numeric_limits<int64_t>::max();
    for (auto i = size_t{0}; i < input.size(); ++i) {
      auto [part, start] = positions[i];
      TENZIR_ASSERT(part <= input[i].parts().size());
      if (part == input[i].parts().size()) {
        // We assert that everything else is done as well.
        for (auto j = size_t{0}; j < input.size(); ++j) {
          std::tie(part, start) = positions[j];
          TENZIR_ASSERT(part == input[j].parts().size());
          TENZIR_ASSERT(start == 0);
        }
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
      output[i] = input[i].part(part).slice(start, start + shortest_length);
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

auto map_series(std::span<const multi_series> args,
                detail::function_view<auto(std::span<series>)->multi_series> f)
  -> multi_series {
  auto result = std::vector<series>{};
  for (auto x : split_multi_series(args)) {
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
    [[maybe_unused]] const auto input_part_length = part.length();
    auto mapped = f(std::move(part));
    TENZIR_ASSERT_EQ(mapped.length(), input_part_length);
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

auto multi_series::to_series(multi_series::to_series_strategy strategy) const
  -> to_series_result {
  if (length() == 0) {
    return {{}, to_series_result::status::ok};
  }
  if (length() == 1) {
    return {
      parts_.front(),
      to_series_result::status::ok,
    };
  }
  auto selected_group_index = size_t{0};
  auto part_groups = std::vector<size_t>(parts_.size());
  struct group_info_t {
    tenzir::type type;
    int64_t size = 0;
  };
  // This map needs to be ordered, because we need to iterate the groups in order
  auto groups = detail::flat_map<size_t, group_info_t>{
    {0, {parts_.front().type, parts_.front().length()}},
  };
  groups.reserve(parts_.size());
  /// FIXME. This does not actually find the largest group in general. Given [A,
  /// B, C, C], where [A,B] and [A,C] can be merged, this would creates [A+B,
  /// A+B, null, null] because the merging of A and B happens early. The correct
  /// *largest* merge would be [A+C, null, A+C, A+C].
  /// However, this requires a full fledged combinatoric explosion check, which
  /// currently does not seem necessary of advisable.
  for (size_t i = 1; i < parts_.size(); ++i) {
    auto& part = parts_[i];
    // Check all groups.
    part_groups[i] = groups.size();
    for (auto& [group_index, group] : groups) {
      if (group.type == part.type) {
        part_groups[i] = group_index;
        group.size += part.length();
        break;
      }
      auto unified_type = unify(group.type, part.type);
      if (unified_type) {
        part_groups[i] = group_index;
        group.type = std::move(*unified_type);
        group.size += part.length();
        break;
      } else if (strategy == to_series_strategy::fail) {
        return {{}, to_series_result::status::fail, {group.type, part.type}};
      }
    }
    // If we are going to take the first type anyways, there is no need to
    // update the rest.
    if (strategy == to_series_strategy::take_first_null_rest) {
      continue;
    }
    // Potentially update the selected, i.e. largest group.
    if (part_groups[i] != groups.size()) {
      // Potentially update the selected (largest) group.
      if (selected_group_index != part_groups[i]
          and groups[selected_group_index].size < groups[part_groups[i]].size) {
        selected_group_index = part_groups[i];
      }
      // No need to create a new group, we found one.
      continue;
    }
    // If we arrive here, it has to be a new group.
    groups.try_emplace(part_groups[i], part.type, part.length());
    // Potentially update the selected, i.e. largest group.
    if (part.length() > groups[selected_group_index].size) {
      selected_group_index = part_groups[i];
    }
  }
  auto b = series_builder{groups[selected_group_index].type};
  for (size_t i = 0; i < parts_.size(); ++i) {
    auto& part = parts_[i];
    if (part_groups[i] != selected_group_index) {
      for (int64_t j = 0; j < part.length(); ++j) {
        b.null();
      }
      continue;
    }
    for (auto event : part.values()) {
      if (not b.try_data(event)) {
        return {{}, to_series_result::status::fail};
      }
    }
  }
  if (groups.size() > 1) {
    auto conflicting_types = std::vector<type>{};
    for (auto& [_, group] : groups) {
      conflicting_types.push_back(std::move(group.type));
    }
    return {
      b.finish_assert_one_array(),
      to_series_result::status::conflict,
      std::move(conflicting_types),
    };
  }
  return {
    b.finish_assert_one_array(),
    to_series_result::status::ok,
  };
}

} // namespace tenzir
