//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/distribution.hpp"

#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <cmath>
#include <concepts>
#include <numbers>

namespace tenzir::detail {

jensen_shannon_accumulator::jensen_shannon_accumulator(double lhs_weight,
                                                       double rhs_weight)
  : lhs_weight_{lhs_weight}, rhs_weight_{rhs_weight} {
  TENZIR_ASSERT(std::isfinite(lhs_weight));
  TENZIR_ASSERT(std::isfinite(rhs_weight));
  TENZIR_ASSERT(lhs_weight > 0.0);
  TENZIR_ASSERT(rhs_weight > 0.0);
}

auto jensen_shannon_accumulator::add(double lhs, double rhs) -> void {
  TENZIR_ASSERT(std::isfinite(lhs));
  TENZIR_ASSERT(std::isfinite(rhs));
  TENZIR_ASSERT(lhs >= 0.0);
  TENZIR_ASSERT(rhs >= 0.0);
  auto const p = lhs / lhs_weight_;
  auto const q = rhs / rhs_weight_;
  if (p == 0.0 or q == 0.0) {
    result_ += (p + q) * (0.5 * std::numbers::ln2);
    return;
  }
  auto const midpoint = (p + q) / 2.0;
  result_ += 0.5 * p * std::log(p / midpoint);
  result_ += 0.5 * q * std::log(q / midpoint);
}

auto jensen_shannon_accumulator::value() const -> double {
  return std::max(result_, 0.0);
}

auto jensen_shannon(std::span<double const> lhs, std::span<double const> rhs)
  -> double {
  TENZIR_ASSERT(lhs.size() == rhs.size());
  auto const lhs_scale = *std::ranges::max_element(lhs);
  auto const rhs_scale = *std::ranges::max_element(rhs);
  TENZIR_ASSERT(lhs_scale > 0.0);
  TENZIR_ASSERT(rhs_scale > 0.0);
  auto lhs_weight = 0.0;
  auto rhs_weight = 0.0;
  for (auto i = size_t{0}; i < lhs.size(); ++i) {
    lhs_weight += lhs[i] / lhs_scale;
    rhs_weight += rhs[i] / rhs_scale;
  }
  auto result = jensen_shannon_accumulator{lhs_weight, rhs_weight};
  for (auto i = size_t{0}; i < lhs.size(); ++i) {
    result.add(lhs[i] / lhs_scale, rhs[i] / rhs_scale);
  }
  return result.value();
}

namespace {

template <class T>
auto ecdf_impl(std::span<T const> samples, T x) -> double {
  TENZIR_ASSERT(not samples.empty());
  auto const count = std::ranges::count_if(samples, [x](T sample) {
    return sample <= x;
  });
  return static_cast<double>(count) / static_cast<double>(samples.size());
}

template <class T>
auto kolmogorov_smirnov_impl(std::span<T const> lhs, std::span<T const> rhs)
  -> double {
  TENZIR_ASSERT(not lhs.empty());
  TENZIR_ASSERT(not rhs.empty());
  TENZIR_ASSERT(std::ranges::is_sorted(lhs));
  TENZIR_ASSERT(std::ranges::is_sorted(rhs));
  auto lhs_index = size_t{0};
  auto rhs_index = size_t{0};
  auto result = 0.0;
  while (lhs_index < lhs.size() or rhs_index < rhs.size()) {
    auto const value = lhs_index == lhs.size() ? rhs[rhs_index]
                       : rhs_index == rhs.size()
                         ? lhs[lhs_index]
                         : std::min(lhs[lhs_index], rhs[rhs_index]);
    while (lhs_index < lhs.size() and lhs[lhs_index] <= value) {
      ++lhs_index;
    }
    while (rhs_index < rhs.size() and rhs[rhs_index] <= value) {
      ++rhs_index;
    }
    auto const lhs_cdf
      = static_cast<double>(lhs_index) / static_cast<double>(lhs.size());
    auto const rhs_cdf
      = static_cast<double>(rhs_index) / static_cast<double>(rhs.size());
    result = std::max(result, std::abs(lhs_cdf - rhs_cdf));
  }
  return std::min(result, 1.0);
}

template <class T>
auto wasserstein_impl(std::span<T const> lhs, std::span<T const> rhs)
  -> double {
  TENZIR_ASSERT(not lhs.empty());
  TENZIR_ASSERT(not rhs.empty());
  TENZIR_ASSERT(std::ranges::is_sorted(lhs));
  TENZIR_ASSERT(std::ranges::is_sorted(rhs));
  auto lhs_index = size_t{0};
  auto rhs_index = size_t{0};
  auto previous = std::min(lhs.front(), rhs.front());
  auto result = 0.0;
  while (lhs_index < lhs.size() or rhs_index < rhs.size()) {
    auto const value = lhs_index == lhs.size() ? rhs[rhs_index]
                       : rhs_index == rhs.size()
                         ? lhs[lhs_index]
                         : std::min(lhs[lhs_index], rhs[rhs_index]);
    auto const lhs_cdf
      = static_cast<double>(lhs_index) / static_cast<double>(lhs.size());
    auto const rhs_cdf
      = static_cast<double>(rhs_index) / static_cast<double>(rhs.size());
    auto const cdf_difference = std::abs(lhs_cdf - rhs_cdf);
    if (cdf_difference != 0.0) {
      if constexpr (std::same_as<T, int64_t>) {
        auto const width = static_cast<double>(
          static_cast<uint64_t>(value) - static_cast<uint64_t>(previous));
        result += width * cdf_difference;
      } else {
        auto const width = value - previous;
        result += std::isfinite(width)
                    ? width * cdf_difference
                    : value * cdf_difference - previous * cdf_difference;
      }
    }
    while (lhs_index < lhs.size() and lhs[lhs_index] <= value) {
      ++lhs_index;
    }
    while (rhs_index < rhs.size() and rhs[rhs_index] <= value) {
      ++rhs_index;
    }
    previous = value;
  }
  return result;
}

} // namespace

auto ecdf(std::span<double const> samples, double x) -> double {
  return ecdf_impl(samples, x);
}

auto ecdf(std::span<int64_t const> samples, int64_t x) -> double {
  return ecdf_impl(samples, x);
}

auto kolmogorov_smirnov(std::span<double const> lhs,
                        std::span<double const> rhs) -> double {
  return kolmogorov_smirnov_impl(lhs, rhs);
}

auto kolmogorov_smirnov(std::span<int64_t const> lhs,
                        std::span<int64_t const> rhs) -> double {
  return kolmogorov_smirnov_impl(lhs, rhs);
}

auto wasserstein(std::span<double const> lhs, std::span<double const> rhs)
  -> double {
  return wasserstein_impl(lhs, rhs);
}

auto wasserstein(std::span<int64_t const> lhs, std::span<int64_t const> rhs)
  -> double {
  return wasserstein_impl(lhs, rhs);
}

} // namespace tenzir::detail
