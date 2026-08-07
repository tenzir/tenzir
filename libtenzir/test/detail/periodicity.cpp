//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/periodicity.hpp"

#include "tenzir/test/test.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <complex>
#include <limits>
#include <numbers>
#include <vector>

using namespace tenzir;

namespace {

constexpr auto epsilon = 1e-9;

auto approx(double x, double y, double eps = epsilon) -> bool {
  return std::abs(x - y) < eps;
}

// A naive O(n^2) reference DFT to validate the FFT against.
auto reference_dft(std::vector<std::complex<double>> const& xs)
  -> std::vector<std::complex<double>> {
  auto const n = xs.size();
  auto result = std::vector<std::complex<double>>(n);
  for (size_t k = 0; k < n; ++k) {
    for (size_t i = 0; i < n; ++i) {
      auto const angle = -2.0 * std::numbers::pi * static_cast<double>(k * i)
                         / static_cast<double>(n);
      result[k]
        += xs[i] * std::complex<double>{std::cos(angle), std::sin(angle)};
    }
  }
  return result;
}

// Deterministic pseudo-random doubles in [-1, 1] via a simple LCG.
auto pseudo_random(size_t n, uint64_t seed = 42) -> std::vector<double> {
  auto result = std::vector<double>{};
  result.reserve(n);
  auto state = seed;
  for (size_t i = 0; i < n; ++i) {
    state = state * 6364136223846793005ULL + 1442695040888963407ULL;
    auto const bits = state >> 11;
    result.push_back(static_cast<double>(bits) / 4503599627370496.0 - 1.0);
  }
  return result;
}

TEST("fft of impulse is flat") {
  auto data = std::vector<std::complex<double>>{{1, 0}, {0, 0}, {0, 0}, {0, 0}};
  detail::fft(data);
  for (auto const& x : data) {
    CHECK(approx(x.real(), 1.0));
    CHECK(approx(x.imag(), 0.0));
  }
}

TEST("fft of cosine concentrates in one bin pair") {
  constexpr auto n = size_t{8};
  auto data = std::vector<std::complex<double>>{};
  for (size_t i = 0; i < n; ++i) {
    data.emplace_back(
      std::cos(2.0 * std::numbers::pi * 2.0 * static_cast<double>(i) / 8.0),
      0.0);
  }
  detail::fft(data);
  for (size_t k = 0; k < n; ++k) {
    auto const expected = (k == 2 or k == 6) ? 4.0 : 0.0;
    CHECK(approx(std::abs(data[k]), expected));
  }
}

TEST("fft matches reference dft on random input") {
  auto const xs = pseudo_random(256);
  auto data = std::vector<std::complex<double>>{};
  for (auto const x : xs) {
    data.emplace_back(x, 0.0);
  }
  auto const expected = reference_dft(data);
  detail::fft(data);
  for (size_t k = 0; k < data.size(); ++k) {
    CHECK(approx(data[k].real(), expected[k].real(), 1e-8));
    CHECK(approx(data[k].imag(), expected[k].imag(), 1e-8));
  }
}

TEST("fft handles trivial sizes") {
  auto empty = std::vector<std::complex<double>>{};
  detail::fft(empty);
  CHECK_EQUAL(empty.size(), size_t{0});
  auto one = std::vector<std::complex<double>>{{3, 0}};
  detail::fft(one);
  CHECK(approx(one[0].real(), 3.0));
  auto two = std::vector<std::complex<double>>{{1, 0}, {2, 0}};
  detail::fft(two);
  CHECK(approx(two[0].real(), 3.0));
  CHECK(approx(two[1].real(), -1.0));
}

TEST("autocorrelation of alternating series") {
  auto const xs = std::vector<double>{1, -1, 1, -1, 1, -1, 1, -1};
  auto const acf = detail::autocorrelation(xs, 2);
  REQUIRE(acf.has_value());
  REQUIRE_EQUAL(acf->size(), size_t{3});
  CHECK(approx((*acf)[0], 1.0));
  CHECK(approx((*acf)[1], -0.875));
  CHECK(approx((*acf)[2], 0.75));
}

TEST("autocorrelation is invariant across finite scales") {
  auto const tiny = std::vector<double>{1e-200, 2e-200, 1e-200, 2e-200};
  auto const huge = std::vector<double>{1e200, 2e200, 1e200, 2e200};
  for (auto const& xs : {tiny, huge}) {
    auto const acf = detail::autocorrelation(xs, 2);
    REQUIRE(acf.has_value());
    REQUIRE_EQUAL(acf->size(), size_t{3});
    CHECK(approx((*acf)[0], 1.0));
    CHECK(approx((*acf)[1], -0.75));
    CHECK(approx((*acf)[2], 0.5));
  }
}

TEST("autocorrelation of impulse train prefers the fundamental") {
  // Period 4 over n=16.
  auto xs = std::vector<double>(16, 0.0);
  for (size_t i = 0; i < xs.size(); i += 4) {
    xs[i] = 1.0;
  }
  auto const acf = detail::autocorrelation(xs, 8);
  REQUIRE(acf.has_value());
  CHECK(approx((*acf)[4], 0.75));
  CHECK(approx((*acf)[8], 0.5));
  for (size_t k = 2; k <= 8; ++k) {
    if (k != 4) {
      CHECK_LESS((*acf)[k], (*acf)[4]);
    }
  }
}

TEST("autocorrelation of cosine at full period") {
  constexpr auto n = size_t{64};
  auto xs = std::vector<double>{};
  for (size_t i = 0; i < n; ++i) {
    xs.push_back(
      std::cos(2.0 * std::numbers::pi * static_cast<double>(i) / 8.0));
  }
  auto const acf = detail::autocorrelation(xs, 8);
  REQUIRE(acf.has_value());
  CHECK(approx((*acf)[8], 0.875));
}

TEST("autocorrelation degenerate inputs") {
  CHECK(not detail::autocorrelation({}, 5).has_value());
  auto const constant = std::vector<double>{5, 5, 5, 5};
  CHECK(not detail::autocorrelation(constant, 2).has_value());
  auto const single = std::vector<double>{1};
  CHECK(not detail::autocorrelation(single, 2).has_value());
  auto const with_nan
    = std::vector<double>{1, 2, std::numeric_limits<double>::quiet_NaN()};
  CHECK(not detail::autocorrelation(with_nan, 2).has_value());
}

TEST("autocorrelation clamps max_lag") {
  auto const xs = std::vector<double>{1, 2, 3, 4};
  auto const acf = detail::autocorrelation(xs, 100);
  REQUIRE(acf.has_value());
  CHECK_EQUAL(acf->size(), size_t{4});
}

TEST("autocorrelation scales to large inputs") {
  constexpr auto n = size_t{100'000};
  auto xs = std::vector<double>{};
  xs.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    xs.push_back(i % 2 == 0 ? 1.0 : -1.0);
  }
  auto const acf = detail::autocorrelation(xs, 50'000);
  REQUIRE(acf.has_value());
  REQUIRE_EQUAL(acf->size(), size_t{50'001});
  CHECK(approx((*acf)[1], -0.99999, 1e-8));
  CHECK(approx((*acf)[50'000], 0.5, 1e-8));
}

TEST("periodogram of cosine peaks at its bin") {
  constexpr auto n = size_t{64};
  constexpr auto k0 = size_t{8};
  auto xs = std::vector<double>{};
  for (size_t i = 0; i < n; ++i) {
    xs.push_back(std::cos(2.0 * std::numbers::pi * static_cast<double>(k0 * i)
                          / static_cast<double>(n)));
  }
  auto const result = detail::periodogram(xs);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->fft_size, n);
  REQUIRE_EQUAL(result->power.size(), n / 2);
  // Power |X_{k0}|^2 / n = (n/2)^2 / n = 16; bin k is at index k - 1.
  CHECK(approx(result->power[k0 - 1], 16.0));
  for (size_t k = 1; k <= n / 2; ++k) {
    if (k != k0) {
      CHECK(approx(result->power[k - 1], 0.0, 1e-8));
    }
  }
}

TEST("periodogram of constant series is flat zero") {
  auto const xs = std::vector<double>{3, 3, 3, 3, 3, 3, 3, 3};
  auto const result = detail::periodogram(xs);
  REQUIRE(result.has_value());
  for (auto const p : result->power) {
    CHECK(approx(p, 0.0));
  }
}

TEST("periodogram safely centers large finite constants") {
  auto const xs = std::vector<double>{1e308, 1e308};
  auto const result = detail::periodogram(xs);
  REQUIRE(result.has_value());
  REQUIRE_EQUAL(result->power.size(), size_t{1});
  CHECK(approx(result->power[0], 0.0));
}

TEST("periodogram pads to the next power of two") {
  auto const xs = pseudo_random(100);
  auto const result = detail::periodogram(xs);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->fft_size, size_t{128});
  CHECK_EQUAL(result->power.size(), size_t{64});
}

TEST("periodogram trivial sizes") {
  auto const empty = detail::periodogram({});
  REQUIRE(empty.has_value());
  CHECK_EQUAL(empty->power.size(), size_t{0});
  auto const single = std::vector<double>{42};
  auto const result = detail::periodogram(single);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->power.size(), size_t{0});
}

TEST("periodogram rejects non-finite samples") {
  auto const with_nan
    = std::vector<double>{1, std::numeric_limits<double>::quiet_NaN(), 2};
  CHECK(not detail::periodogram(with_nan).has_value());
  auto const with_infinity
    = std::vector<double>{1, std::numeric_limits<double>::infinity(), 2};
  CHECK(not detail::periodogram(with_infinity).has_value());
}

TEST("dominant_lag recovers impulse train period") {
  // Period 5 over 50 bins.
  auto xs = std::vector<double>(50, 0.0);
  for (size_t i = 0; i < xs.size(); i += 5) {
    xs[i] = 1.0;
  }
  auto const result = detail::dominant_lag(xs);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->first, int64_t{5});
  CHECK(approx(result->second, 0.9));
}

TEST("dominant_lag compares only complete scoring windows") {
  // Five impulses at period 3 fill 13 bins. The candidate at the maximum lag
  // has no right neighbor and would beat the fundamental if its incomplete
  // scoring window were compared against complete windows.
  auto xs = std::vector<double>(13, 0.0);
  for (size_t i = 0; i < xs.size(); i += 3) {
    xs[i] = 1.0;
  }
  auto const result = detail::dominant_lag(xs);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->first, int64_t{3});
  CHECK(approx(result->second, 0.7807692307692308));
}

TEST("dominant_lag preserves narrow peaks") {
  // Alternating bins have a strong lag-2 peak surrounded by negative
  // coefficients, which must not cancel the peak during windowed scoring.
  auto const xs = std::vector<double>{1, 0, 1, 0, 1, 0, 1};
  auto const result = detail::dominant_lag(xs);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->first, int64_t{2});
  CHECK(approx(result->second, 0.7023809523809523));
}

TEST("dominant_lag includes the upper-bound lag") {
  // Lag 2 is both the only periodic peak and the n/2 upper bound.
  auto const xs = std::vector<double>{1, 0, 1, 0, 1};
  auto const result = detail::dominant_lag(xs);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->first, int64_t{2});
  CHECK(approx(result->second, 0.5666666666666667));
}

TEST("dominant_lag prefers the fundamental over harmonics under jitter") {
  // A 60s beacon with +/-2s jitter, binned at 5s: impulses at lag 12 whose
  // peak mass spreads to adjacent lags. A strict argmax picks a harmonic
  // here; the windowed score must recover the fundamental.
  constexpr auto jitter = std::array{-1, -2, 2, 1,  1, 2,  0,  1, -2, 0,
                                     2,  0,  0, -1, 1, -2, -1, 0, -2, -1};
  auto bins = std::vector<double>{};
  for (size_t i = 0; i < jitter.size(); ++i) {
    auto const t = 60 * static_cast<int64_t>(i) + jitter[i] - (-1);
    auto const bin = static_cast<size_t>(t / 5);
    bins.resize(std::max(bins.size(), bin + 1), 0.0);
    bins[bin] += 1.0;
  }
  auto const result = detail::dominant_lag(bins);
  REQUIRE(result.has_value());
  CHECK_EQUAL(result->first, int64_t{12});
  CHECK_GREATER(result->second, 0.5);
}

TEST("dominant_lag rejects degenerate series") {
  auto const uniform = std::vector<double>(50, 1.0);
  CHECK(not detail::dominant_lag(uniform).has_value());
  auto const tiny = std::vector<double>{1, 0, 1};
  CHECK(not detail::dominant_lag(tiny).has_value());
  CHECK(not detail::dominant_lag({}).has_value());
}

TEST("dominant_lag stays weak on noise") {
  auto const xs = pseudo_random(200, 7);
  auto const result = detail::dominant_lag(xs);
  if (result) {
    CHECK_LESS(result->second, 0.5);
  }
}

} // namespace
