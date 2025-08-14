//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/function.hpp"
#include "tenzir/series.hpp"

namespace tenzir {

/// A potentially heterogenous series type.
class multi_series {
public:
  multi_series() = default;

  template <class Ty>
  explicit(false) multi_series(basic_series<Ty> s) {
    parts_.push_back(std::move(s));
  }

  explicit multi_series(std::vector<series> parts) : parts_{std::move(parts)} {
  }

  auto length() const -> int64_t {
    auto result = int64_t{0};
    for (auto& part : parts_) {
      result += part.length();
    }
    return result;
  }

  auto value_at(int64_t row) const -> data_view {
    auto [part, part_row] = resolve(row);
    return tenzir::value_at(part.get().type, *part.get().array, part_row);
  }

  auto view3_at(int64_t row) const -> data_view3 {
    const auto [part, part_row] = resolve(row);
    return view_at(*part.get().array, part_row);
  }

  auto is_null(int64_t row) const -> bool {
    auto [part, part_row] = resolve(row);
    return part.get().array->IsNull(part_row);
  }

  auto part(size_t idx) -> series& {
    TENZIR_ASSERT(idx < parts_.size());
    return parts_[idx];
  }

  auto part(size_t idx) const -> const series& {
    TENZIR_ASSERT(idx < parts_.size());
    return parts_[idx];
  }

  auto begin() {
    return parts_.begin();
  }

  auto begin() const {
    return parts_.begin();
  }

  auto end() {
    return parts_.end();
  }

  auto end() const {
    return parts_.end();
  }

  auto parts() -> std::span<series> {
    return parts_;
  }

  auto parts() const -> std::span<const series> {
    return parts_;
  }

  auto values() const -> generator<data_view> {
    for (auto& part : parts_) {
      for (auto value : part.values()) {
        co_yield std::move(value);
      }
    }
  }

  auto null_count() const -> int64_t {
    auto result = int64_t{0};
    for (auto& part : parts_) {
      result += part.array->null_count();
    }
    return result;
  }

  auto clear() -> void {
    return parts_.clear();
  }

  auto append(series s) -> void {
    parts_.push_back(std::move(s));
  }

  auto append(multi_series s) -> void {
    parts_.insert(parts_.end(), std::make_move_iterator(s.parts_.begin()),
                  std::make_move_iterator(s.parts_.end()));
  }

  // What to do on join conflict in `to_series`
  enum class to_series_strategy {
    // Fail the join
    fail,
    // Take the first type, null the mismatches
    take_first_null_rest,
    // Try to from the largest join, null the mismatches
    // This does not find the truly largest merge, but only optimistically goes
    // from the start, merging eagerly.
    take_largest_from_start_null_rest,
  };

  struct to_series_result {
    enum class status {
      // join succeeded
      ok,
      // join succeeded, but nulled out some values
      conflict,
      // join failed
      fail,
    };
    tenzir::series series;
    enum status status;
    std::vector<type> conflicting_types{};
  };

  /// Tries to join a `multi_series` into a single `series` by performing type
  /// unification, using a `series_builder`.
  /// Checks are performed using `unify( type, type ) -> std::optional<type>`
  /// @ref multi_series::join_conflict_strategy
  auto to_series(to_series_strategy strategy
                 = to_series_strategy::fail) const -> to_series_result;

private:
  auto resolve(int64_t row) const
    -> std::pair<std::reference_wrapper<const series>, int64_t> {
    for (auto& part : parts_) {
      if (row < part.length()) {
        return {part, row};
      }
      row -= part.length();
    }
    TENZIR_UNREACHABLE();
  }

  std::vector<series> parts_;
};

/// Splits any number of multi-series a sequence of the same number of series.
///
/// Given a single multi-series, this functions just yields the series that make
/// up the multi-series. For more than one series, the individual parts of the
/// multiple series are sliced such that we get equally-typed windows.
///
/// There is also an overload for a static number of arguments below.
auto split_multi_series(std::span<const multi_series> input)
  -> generator<std::span<series>>;

auto split_multi_series(std::span<const multi_series> input,
                        std::span<series> output) -> generator<std::monostate>;

template <std::same_as<multi_series>... Ts>
auto split_multi_series(Ts... xs)
  -> generator<std::array<series, sizeof...(Ts)>> {
  auto input = std::array<multi_series, sizeof...(Ts)>{std::move(xs)...};
  auto output = std::array<series, sizeof...(Ts)>{};
  for ([[maybe_unused]] auto _ : split_multi_series(input, output)) {
    co_yield std::move(output);
  }
}

/// Applies a function that takes series to a multi-series.
///
/// This overload accepts a dynamic number of arguments. The function is called
/// potentially multiple times with equally-typed slices of the given arguments.
/// Thus, the number of series passed to the function is always the same as the
/// number of given multi-series.
///
/// See below for overloads that accept a static number of arguments.
auto map_series(std::span<const multi_series> args,
                detail::function_view<auto(std::span<series>)->multi_series> f)
  -> multi_series;

auto map_series(multi_series x,
                detail::function_view<auto(series)->multi_series> f)
  -> multi_series;

auto map_series(multi_series x, multi_series y,
                detail::function_view<auto(series, series)->multi_series> f)
  -> multi_series;
} // namespace tenzir
