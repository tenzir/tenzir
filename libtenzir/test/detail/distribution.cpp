//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/distribution.hpp"

#include "tenzir/test/test.hpp"

#include <cmath>
#include <limits>
#include <numbers>
#include <vector>

namespace tenzir::detail {

namespace {

TEST("Jensen-Shannon divergence") {
  auto const p = std::vector{1.0, 0.0};
  auto const q = std::vector{0.0, 1.0};
  CHECK_EQUAL(jensen_shannon(p, p), 0.0);
  CHECK(std::abs(jensen_shannon(p, q) - std::numbers::ln2) < 1e-12);
  CHECK_EQUAL(jensen_shannon(p, q), jensen_shannon(q, p));

  auto const scaled_p = std::vector{10.0, 0.0};
  auto const scaled_q = std::vector{0.0, 20.0};
  CHECK_EQUAL(jensen_shannon(p, q), jensen_shannon(scaled_p, scaled_q));

  auto const overlapping_p = std::vector{2.0, 1.0, 0.0};
  auto const overlapping_q = std::vector{0.0, 1.0, 2.0};
  CHECK(std::abs(jensen_shannon(overlapping_p, overlapping_q)
                 - 2.0 / 3.0 * std::numbers::ln2)
        < 1e-12);

  auto const sparse_p
    = std::vector{std::numeric_limits<double>::denorm_min(), 1.0};
  auto const sparse_q = std::vector{0.0, 1.0};
  CHECK(std::isfinite(jensen_shannon(sparse_p, sparse_q)));

  auto const large = std::vector{1e308, 1e308};
  CHECK_EQUAL(jensen_shannon(large, large), 0.0);
}

TEST("empirical CDF") {
  auto const samples = std::vector{1.0, 2.0, 2.0, 4.0};
  CHECK_EQUAL(ecdf(samples, 0.0), 0.0);
  CHECK_EQUAL(ecdf(samples, 2.0), 0.75);
  CHECK_EQUAL(ecdf(samples, 4.0), 1.0);
  auto const integers = std::vector<int64_t>{1, 2, 2, 4};
  CHECK_EQUAL(ecdf(integers, int64_t{2}), 0.75);
}

TEST("Kolmogorov-Smirnov distance") {
  auto const x = std::vector{0.0, 1.0, 2.0};
  auto const y = std::vector{1.0, 2.0, 3.0};
  CHECK_EQUAL(kolmogorov_smirnov(x, x), 0.0);
  CHECK(std::abs(kolmogorov_smirnov(x, y) - 1.0 / 3.0) < 1e-12);
  CHECK_EQUAL(kolmogorov_smirnov(x, y), kolmogorov_smirnov(y, x));

  auto const integers = std::vector<int64_t>{0, 1, 2};
  auto const shifted = std::vector<int64_t>{1, 2, 3};
  CHECK(std::abs(kolmogorov_smirnov(integers, shifted) - 1.0 / 3.0) < 1e-12);

  auto const duplicates = std::vector{0.0, 0.0, 1.0, 1.0};
  auto const unequal = std::vector{0.0, 1.0};
  CHECK_EQUAL(kolmogorov_smirnov(duplicates, unequal), 0.0);
}

TEST("Wasserstein distance") {
  auto const x = std::vector{0.0, 1.0, 2.0};
  auto const y = std::vector{1.0, 2.0, 3.0};
  CHECK_EQUAL(wasserstein(x, x), 0.0);
  CHECK_EQUAL(wasserstein(x, y), 1.0);
  CHECK_EQUAL(wasserstein(x, y), wasserstein(y, x));

  auto const integers = std::vector<int64_t>{0, 1, 2};
  auto const shifted = std::vector<int64_t>{1, 2, 3};
  CHECK_EQUAL(wasserstein(integers, shifted), 1.0);

  auto const short_samples = std::vector{0.0, 2.0};
  CHECK(std::abs(wasserstein(short_samples, x) - 1.0 / 3.0) < 1e-12);
  auto const duplicates = std::vector{0.0, 0.0, 2.0, 2.0};
  CHECK_EQUAL(wasserstein(short_samples, duplicates), 0.0);
  auto const extremes = std::vector{-1e308, 1e308};
  CHECK_EQUAL(wasserstein(extremes, extremes), 0.0);
  auto const upper = std::vector{1e308, 1e308};
  CHECK_EQUAL(wasserstein(extremes, upper), 1e308);

  auto const a = 1e308;
  auto const b = std::nextafter(a, std::numeric_limits<double>::infinity());
  auto narrow = std::vector<double>(35, b);
  narrow.front() = a;
  auto const narrow_upper = std::vector<double>(35, b);
  CHECK_EQUAL(wasserstein(narrow, narrow_upper), (b - a) / 35.0);
}

} // namespace

} // namespace tenzir::detail
