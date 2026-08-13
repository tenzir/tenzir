//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Periodicity-detection primitives: autocorrelation, periodogram, and
// dominant-lag recovery. These back the TQL functions `autocorrelation`,
// `periodogram`, and `dominant_period` used for beacon detection.

#pragma once

#include "tenzir/option.hpp"

#include <complex>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace tenzir::detail {

/// In-place iterative radix-2 Cooley-Tukey FFT. The size of `data` must be
/// zero or a power of two.
void fft(std::span<std::complex<double>> data);

/// Computes the mean-centered, biased autocorrelation
///   r_k = sum_{i=0}^{n-1-k} (x_i - mu)(x_{i+k} - mu) / sum_i (x_i - mu)^2
/// for lags 0 through `max_lag` inclusive, so the result has `max_lag + 1`
/// elements with `result[0] == 1.0`. `max_lag` is clamped to `n - 1`. The
/// biased estimator keeps `|r_k| <= 1` and damps long lags, so the
/// fundamental period dominates its harmonics. Inputs are normalized before
/// accumulation to preserve scale invariance across finite magnitudes. The
/// implementation uses direct sums for small inputs and an FFT for larger
/// inputs, avoiding quadratic work on long series.
/// Returns nullopt if `xs` is empty, constant, or contains non-finite values.
auto autocorrelation(std::span<double const> xs, int64_t max_lag)
  -> Option<std::vector<double>>;

struct periodogram_result {
  /// The zero-padded FFT size N (next power of two >= xs.size()).
  size_t fft_size = 0;
  /// Power |X_k|^2 / n for bins k = 1 .. N/2, ascending frequency. The
  /// period of bin k in samples is `fft_size / k`.
  std::vector<double> power;
};

/// Computes the classical periodogram of `xs`: demean, zero-pad to the next
/// power of two, FFT, and return the power of the non-DC bins up to Nyquist.
/// Fewer than two elements yield an empty power vector. Returns nullopt if an
/// input sample or computed power is non-finite.
auto periodogram(std::span<double const> xs) -> Option<periodogram_result>;

/// Finds the dominant periodic lag among lags `min_lag .. n/2`. Lags are
/// ranked by the sum of positive autocorrelation coefficients in a complete
/// +/-1 window around them, which keeps narrow peaks from being canceled by
/// anti-correlated neighbors and keeps the fundamental period ahead of its
/// harmonics when jitter spreads the peak across adjacent lags; the returned
/// lag is the strongest single lag within the winning window, ties broken
/// toward the smaller lag. Returns the lag and its autocorrelation coefficient,
/// or nullopt if the series is degenerate (see `autocorrelation`) or the peak
/// coefficient is not positive.
auto dominant_lag(std::span<double const> xs, int64_t min_lag = 2)
  -> Option<std::pair<int64_t, double>>;

} // namespace tenzir::detail
