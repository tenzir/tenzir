//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/periodicity.hpp"

#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <numbers>
#include <numeric>

namespace tenzir::detail {

void fft(std::span<std::complex<double>> data) {
  auto const n = data.size();
  TENZIR_ASSERT(std::has_single_bit(n) or n == 0);
  if (n < 2) {
    return;
  }
  // Bit-reversal permutation.
  for (size_t i = 1, j = 0; i < n; ++i) {
    auto bit = n >> 1;
    for (; (j & bit) != 0; bit >>= 1) {
      j ^= bit;
    }
    j ^= bit;
    if (i < j) {
      std::swap(data[i], data[j]);
    }
  }
  // Butterfly passes.
  for (size_t len = 2; len <= n; len <<= 1) {
    auto const angle = -2.0 * std::numbers::pi / static_cast<double>(len);
    auto const w_len = std::complex<double>{std::cos(angle), std::sin(angle)};
    for (size_t i = 0; i < n; i += len) {
      auto w = std::complex<double>{1.0, 0.0};
      for (size_t k = 0; k < len / 2; ++k) {
        auto const even = data[i + k];
        auto const odd = data[i + k + len / 2] * w;
        data[i + k] = even + odd;
        data[i + k + len / 2] = even - odd;
        w *= w_len;
      }
    }
  }
}

namespace {

constexpr auto max_direct_autocorrelation_work = int64_t{1} << 20;

void inverse_fft(std::span<std::complex<double>> data) {
  for (auto& x : data) {
    x = std::conj(x);
  }
  fft(data);
  auto const scale = 1.0 / static_cast<double>(data.size());
  for (auto& x : data) {
    x = std::conj(x) * scale;
  }
}

} // namespace

auto autocorrelation(std::span<double const> xs, int64_t max_lag)
  -> std::optional<std::vector<double>> {
  auto const n = static_cast<int64_t>(xs.size());
  if (n == 0) {
    return std::nullopt;
  }
  max_lag = std::min(max_lag, n - 1);
  TENZIR_ASSERT(max_lag >= 0);
  auto scale = 0.0;
  for (auto const x : xs) {
    if (not std::isfinite(x)) {
      return std::nullopt;
    }
    scale = std::max(scale, std::abs(x));
  }
  if (scale == 0.0) {
    return std::nullopt;
  }
  auto normalized = std::vector<double>{};
  normalized.reserve(xs.size());
  for (auto const x : xs) {
    normalized.push_back(x / scale);
  }
  auto const mean = std::reduce(normalized.begin(), normalized.end())
                    / static_cast<double>(n);
  auto c0 = 0.0;
  for (auto const x : normalized) {
    c0 += (x - mean) * (x - mean);
  }
  if (c0 == 0.0 or not std::isfinite(c0)) {
    return std::nullopt;
  }
  auto result = std::vector<double>{};
  result.reserve(max_lag + 1);
  auto const direct_work = (static_cast<__int128>(max_lag) + 1)
                           * (2 * static_cast<__int128>(n) - max_lag) / 2;
  if (direct_work <= max_direct_autocorrelation_work) {
    for (auto k = int64_t{0}; k <= max_lag; ++k) {
      auto sum = 0.0;
      for (auto i = int64_t{0}; i < n - k; ++i) {
        sum += (normalized[i] - mean) * (normalized[i + k] - mean);
      }
      result.push_back(sum / c0);
    }
    return result;
  }
  auto const padded = std::bit_ceil(xs.size() * 2);
  auto data = std::vector<std::complex<double>>(padded);
  for (size_t i = 0; i < normalized.size(); ++i) {
    data[i] = {normalized[i] - mean, 0.0};
  }
  fft(data);
  for (auto& x : data) {
    auto const power = std::norm(x);
    if (not std::isfinite(power)) {
      return std::nullopt;
    }
    x = {power, 0.0};
  }
  inverse_fft(data);
  for (auto k = int64_t{0}; k <= max_lag; ++k) {
    auto const coefficient
      = k == 0 ? 1.0 : std::clamp(data[k].real() / c0, -1.0, 1.0);
    if (not std::isfinite(coefficient)) {
      return std::nullopt;
    }
    result.push_back(coefficient);
  }
  return result;
}

auto periodogram(std::span<double const> xs)
  -> std::optional<periodogram_result> {
  auto const n = xs.size();
  auto result = periodogram_result{};
  auto scale = 0.0;
  for (auto const x : xs) {
    if (not std::isfinite(x)) {
      return std::nullopt;
    }
    scale = std::max(scale, std::abs(x));
  }
  if (n < 2) {
    return result;
  }
  auto const padded = std::bit_ceil(n);
  auto const mean = std::reduce(xs.begin(), xs.end()) / static_cast<double>(n);
  auto normalized_mean = 0.0;
  auto const use_normalized_mean = not std::isfinite(mean);
  if (use_normalized_mean) {
    TENZIR_ASSERT(scale > 0.0);
    for (auto const x : xs) {
      normalized_mean += x / scale;
    }
    normalized_mean /= static_cast<double>(n);
  }
  auto data = std::vector<std::complex<double>>{};
  data.reserve(padded);
  for (auto const x : xs) {
    auto const centered
      = use_normalized_mean ? (x / scale - normalized_mean) * scale : x - mean;
    if (not std::isfinite(centered)) {
      return std::nullopt;
    }
    data.emplace_back(centered, 0.0);
  }
  data.resize(padded);
  fft(data);
  result.fft_size = padded;
  result.power.reserve(padded / 2);
  for (size_t k = 1; k <= padded / 2; ++k) {
    auto const power = std::norm(data[k]) / static_cast<double>(n);
    if (not std::isfinite(power)) {
      return std::nullopt;
    }
    result.power.push_back(power);
  }
  return result;
}

auto dominant_lag(std::span<double const> xs, int64_t min_lag)
  -> std::optional<std::pair<int64_t, double>> {
  TENZIR_ASSERT(min_lag >= 1);
  auto const n = static_cast<int64_t>(xs.size());
  auto const max_lag = n / 2;
  if (max_lag < min_lag) {
    return std::nullopt;
  }
  // Compute one support coefficient past the candidate range so that the
  // upper-bound lag also has a complete scoring window.
  auto const acf = autocorrelation(xs, max_lag + 1);
  if (not acf) {
    return std::nullopt;
  }
  auto const& r = *acf;
  // Score each lag by the sum of positive coefficients in a complete +/-1
  // window: jitter spreads a peak across adjacent lags, so a strict argmax
  // drifts to a harmonic while the windowed score keeps the fundamental on
  // top. Negative neighbors must not cancel an isolated narrow peak.
  auto best_score = 0.0;
  auto best_lag = int64_t{0};
  for (auto k = std::max(min_lag, int64_t{2}); k <= max_lag; ++k) {
    auto const score
      = std::max(r[k - 1], 0.0) + std::max(r[k], 0.0) + std::max(r[k + 1], 0.0);
    if (score > best_score) {
      best_score = score;
      best_lag = k;
    }
  }
  if (best_lag == 0) {
    return std::nullopt;
  }
  // Refine to the strongest single lag within the winning window.
  auto peak = best_lag;
  for (auto k = std::max(best_lag - 1, min_lag);
       k <= std::min(best_lag + 1, max_lag); ++k) {
    if (r[k] > r[peak]) {
      peak = k;
    }
  }
  if (r[peak] <= 0.0) {
    return std::nullopt;
  }
  return std::pair{peak, r[peak]};
}

} // namespace tenzir::detail
