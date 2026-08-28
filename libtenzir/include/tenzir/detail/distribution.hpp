//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>
#include <span>

namespace tenzir::detail {

class jensen_shannon_accumulator {
public:
  jensen_shannon_accumulator(double lhs_weight, double rhs_weight);

  auto add(double lhs, double rhs) -> void;

  auto value() const -> double;

private:
  double lhs_weight_ = 0.0;
  double rhs_weight_ = 0.0;
  double result_ = 0.0;
};

/// Computes the Jensen-Shannon divergence in nats between two aligned weight
/// vectors. Both vectors must have the same length, contain finite non-negative
/// weights, and have positive total weight.
auto jensen_shannon(std::span<double const> lhs, std::span<double const> rhs)
  -> double;

/// Evaluates the empirical cumulative distribution function of `samples` at
/// `x`. The samples must be finite and nonempty.
auto ecdf(std::span<double const> samples, double x) -> double;
auto ecdf(std::span<int64_t const> samples, int64_t x) -> double;

/// Computes the two-sample Kolmogorov-Smirnov distance. Both sample vectors
/// must be sorted, finite, and nonempty.
auto kolmogorov_smirnov(std::span<double const> lhs,
                        std::span<double const> rhs) -> double;
auto kolmogorov_smirnov(std::span<int64_t const> lhs,
                        std::span<int64_t const> rhs) -> double;

/// Computes the first Wasserstein distance between two empirical
/// distributions. Both sample vectors must be sorted, finite, and nonempty.
auto wasserstein(std::span<double const> lhs, std::span<double const> rhs)
  -> double;
auto wasserstein(std::span<int64_t const> lhs, std::span<int64_t const> rhs)
  -> double;

} // namespace tenzir::detail
