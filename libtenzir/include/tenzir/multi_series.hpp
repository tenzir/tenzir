//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/series.hpp"

namespace tenzir {

/// A potentially heterogenous series type.
class multi_series {
public:
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

  auto part(size_t idx) const -> const series& {
    TENZIR_ASSERT(idx < parts_.size());
    return parts_[idx];
  }

  auto parts() -> std::span<series> {
    return parts_;
  }

  auto parts() const -> std::span<const series> {
    return parts_;
  }

  // TODO: Lifetime?
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

private:
  std::vector<series> parts_;
};

// TODO: Lifetime??
auto split_multi_series(std::span<const multi_series> input)
  -> generator<std::vector<series>>;

auto split_multi_series(multi_series input) -> generator<series>;

auto map_series(std::span<const multi_series> input,
                std::function<auto(std::vector<series>)->multi_series> f)
  -> multi_series;

auto map_series(multi_series x, std::function<auto(series)->multi_series> f)
  -> multi_series;

template <std::same_as<multi_series>... Ts>
auto iter_series(std::tuple<Ts...> args,
                 std::function<void(std::conditional_t<true, series, Ts>...)> f)
  -> multi_series {
  auto vec = std::vector<multi_series>{};
  vec.reserve(sizeof...(Ts));
  std::invoke(
    [&]<size_t... Is>(std::index_sequence<Is...>) {
      (vec.push_back(std::move(std::get<Is>(args))), ...);
    },
    std::make_index_sequence<sizeof...(Ts)>());
  for (auto segment : split_multi_series(vec)) {
    TENZIR_ASSERT(segment.size() == sizeof...(Ts));
    std::invoke(
      [&]<size_t... Is>(std::index_sequence<Is...>) {
        f(std::move(std::get<Is>(segment))...);
      },
      std::make_index_sequence<sizeof...(Ts)>());
  }
}

// TODO: This doesn't work.
template <std::same_as<multi_series>... Ts>
auto iter_series(Ts... args,
                 std::function<void(std::conditional_t<true, series, Ts>...)> f)
  -> multi_series {
  return iter_series(std::tuple{std::move(args)...}, std::move(f));
}

template <std::same_as<multi_series>... Ts>
auto map_series(
  std::tuple<Ts...> args,
  std::function<auto(std::conditional_t<true, series, Ts>...)->multi_series> f)
  -> multi_series {
  auto output = std::vector<series>{};
  iter_series(std::move(args),
              [&](std::conditional_t<true, series, Ts>... args) {
                auto result = f(std::move(args)...);
                output.insert(output.end(), std::move_iterator{output.begin()},
                              std::move_iterator{output.end()});
              });
  return multi_series{std::move(output)};
  // auto vec = std::vector<multi_series>{};
  // vec.reserve(sizeof...(Ts));
  // std::invoke(
  //   [&]<size_t... Is>(std::index_sequence<Is...>) {
  //     (vec.push_back(std::move(std::get<Is>(args))), ...);
  //   },
  //   std::make_index_sequence<sizeof...(Ts)>());
  // return map_series(vec, [&](std::vector<series> input) {
  //   TENZIR_ASSERT(input.size() == sizeof...(Ts));
  //   std::invoke(
  //     [&]<size_t... Is>(std::index_sequence<Is...>) {
  //       return f(std::move(std::get<Is>(input))...);
  //     },
  //     std::make_index_sequence<sizeof...(Ts)>());
  // });
}

void iter_series(multi_series x, std::function<void(series)> f);

auto map_series(multi_series input1, multi_series input2,
                std::function<auto(series, series)->multi_series> f)
  -> multi_series;

} // namespace tenzir
