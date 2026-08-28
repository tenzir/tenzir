//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/tdigest.hpp"

#include "tenzir/test/test.hpp"

#include <algorithm>
#include <cmath>

namespace tenzir::detail {

namespace {

TEST("tdigest copies buffered and merged state") {
  auto digest = tdigest{20, 2};
  digest.add(1.0);
  digest.add(2.0);
  digest.add(3.0);
  auto copy = digest;
  digest.add(4.0);
  CHECK_EQUAL(copy.quantile(1.0), 3.0);
  CHECK_EQUAL(digest.quantile(1.0), 4.0);

  auto assigned = tdigest{};
  assigned = copy;
  copy.add(5.0);
  CHECK_EQUAL(assigned.quantile(1.0), 3.0);
  CHECK_EQUAL(copy.quantile(1.0), 5.0);
}

TEST("tdigest save and restore round-trips quantiles") {
  auto digest = tdigest{};
  for (auto i = 0; i < 10'000; ++i) {
    digest.add(static_cast<double>(i));
  }
  const auto state = digest.save();
  auto restored = tdigest{};
  REQUIRE(restored.restore(state));
  CHECK(restored.validate().is_ok());
  for (auto q : {0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0}) {
    CHECK_EQUAL(restored.quantile(q), digest.quantile(q));
  }
}

TEST("tdigest save flushes buffered input") {
  auto digest = tdigest{};
  // Fewer values than the input buffer size, so nothing was merged yet.
  for (auto i = 0; i < 10; ++i) {
    digest.add(static_cast<double>(i));
  }
  const auto state = digest.save();
  auto restored = tdigest{};
  REQUIRE(restored.restore(state));
  CHECK_EQUAL(restored.quantile(0.5), digest.quantile(0.5));
}

TEST("tdigest skips non-finite floating-point input") {
  auto digest = tdigest{};
  digest.finite_add(-std::numeric_limits<double>::infinity());
  digest.finite_add(std::numeric_limits<double>::infinity());
  digest.finite_add(NAN);
  digest.finite_add(42.0);
  CHECK_EQUAL(digest.quantile(0.5), 42.0);
}

TEST("tdigest CDF handles tails, centroids, and boundaries") {
  auto digest = tdigest{};
  REQUIRE(digest.restore({{0.0, 10.0}, {1.0, 1.0}, 0.0, 10.0}));
  CHECK_EQUAL(digest.cdf(-1.0), 0.0);
  CHECK_EQUAL(digest.cdf(0.0), 0.25);
  CHECK_EQUAL(digest.cdf(5.0), 0.5);
  CHECK_EQUAL(digest.cdf(10.0), 0.75);
  CHECK_EQUAL(digest.cdf(11.0), 1.0);
  REQUIRE(digest.restore({{0.0, 10.0}, {1.0, 3.0}, 0.0, 10.0}));
  CHECK_EQUAL(digest.cdf(10.0), 0.625);
  CHECK(std::isnan(digest.cdf(NAN)));
  CHECK(std::isnan(digest.cdf(std::numeric_limits<double>::infinity())));
}

TEST("tdigest CDF handles one-centroid interpolation") {
  auto digest = tdigest{};
  REQUIRE(digest.restore({{5.0}, {2.0}, 0.0, 10.0}));
  CHECK_EQUAL(digest.cdf(0.0), 0.0);
  CHECK_EQUAL(digest.cdf(2.5), 0.25);
  CHECK_EQUAL(digest.cdf(5.0), 0.5);
  CHECK_EQUAL(digest.cdf(7.5), 0.75);
  CHECK_EQUAL(digest.cdf(10.0), 1.0);
}

TEST("tdigest native distances handle atoms and shifts") {
  auto p = tdigest{};
  auto q = tdigest{};
  REQUIRE(p.restore({{0.0}, {1.0}, 0.0, 0.0}));
  REQUIRE(q.restore({{10.0}, {1.0}, 10.0, 10.0}));
  CHECK_EQUAL(p.ks_distance(p), 0.0);
  CHECK_EQUAL(p.wasserstein_distance(p), 0.0);
  CHECK_EQUAL(p.ks_distance(q), 1.0);
  CHECK_EQUAL(q.ks_distance(p), 1.0);
  CHECK_EQUAL(p.wasserstein_distance(q), 10.0);
  CHECK_EQUAL(q.wasserstein_distance(p), 10.0);
  auto adjacent = tdigest{};
  adjacent.add(std::nextafter(1.0, 2.0));
  auto one = tdigest{};
  one.add(1.0);
  CHECK_EQUAL(one.ks_distance(adjacent), 1.0);
  CHECK_EQUAL(adjacent.ks_distance(one), 1.0);
  CHECK_EQUAL(one.wasserstein_distance(adjacent),
              std::nextafter(1.0, 2.0) - 1.0);
}

TEST("tdigest centroid merging handles extreme finite support") {
  auto digest = tdigest{10};
  digest.add(-1e308);
  for (auto i = 0; i < 99; ++i) {
    digest.add(1e308);
  }
  const auto state = digest.save();
  CHECK(digest.validate().is_ok());
  CHECK(std::all_of(state.means.begin(), state.means.end(), [](auto mean) {
    return std::isfinite(mean);
  }));
  CHECK(std::isfinite(digest.quantile(0.5)));
}

TEST("tdigest interpolation handles extreme finite support") {
  auto quantiles = tdigest{10};
  for (auto value : {-1e308, -1e308, 1e308, 1e308}) {
    quantiles.add(value);
  }
  CHECK_EQUAL(quantiles.quantile(0.5), 0.0);
  auto distribution = tdigest{10};
  REQUIRE(distribution.restore({
    {-1e308, -1e308, -1e308, -1e308, -1e308, 1e308, 1e308, 1e308, 1e308, 1e308},
    {1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 1.0},
    -1e308,
    1e308,
  }));
  CHECK_EQUAL(distribution.cdf(0.0), 0.5);
}

TEST("tdigest Wasserstein distance handles extreme finite support") {
  auto wide = tdigest{};
  wide.add(-1e308);
  wide.add(1e308);
  CHECK_EQUAL(wide.wasserstein_distance(wide), 0.0);
  auto left = tdigest{};
  left.add(-1e308);
  CHECK_EQUAL(left.wasserstein_distance(wide), 1e308);
  auto right = tdigest{};
  right.add(1e308);
  CHECK(std::isinf(left.wasserstein_distance(right)));
}

TEST("tdigest comparisons reject empty distributions") {
  auto empty = tdigest{};
  auto nonempty = tdigest{};
  nonempty.add(1.0);
  CHECK(std::isnan(empty.ks_distance(nonempty)));
  CHECK(std::isnan(nonempty.wasserstein_distance(empty)));
}

TEST("tdigest restore of an empty state yields an empty digest") {
  auto restored = tdigest{};
  REQUIRE(restored.restore(tdigest_state{}));
  CHECK(restored.is_empty());
  CHECK(std::isnan(restored.quantile(0.5)));
}

TEST("tdigest restore rejects invalid states") {
  auto restored = tdigest{};
  // Mismatched vector sizes.
  CHECK(not restored.restore(tdigest_state{{1.0, 2.0}, {1.0}, 1.0, 2.0}));
  // Decreasing centroid means.
  CHECK(not restored.restore(tdigest_state{{2.0, 1.0}, {1.0, 1.0}, 1.0, 2.0}));
  // Invalid centroid weight.
  CHECK(not restored.restore(tdigest_state{{1.0}, {0.5}, 1.0, 1.0}));
  // Extrema outside the centroid range.
  CHECK(not restored.restore(tdigest_state{{1.0}, {1.0}, 2.0, 2.0}));
  CHECK(not restored.restore(tdigest_state{{1.0}, {1.0}, 0.0, 0.0}));
  // Unit endpoint centroids must equal their extrema.
  CHECK(not restored.restore(tdigest_state{{1.0, 2.0}, {1.0, 1.0}, 0.0, 2.0}));
  CHECK(not restored.restore(tdigest_state{{1.0, 2.0}, {1.0, 1.0}, 1.0, 3.0}));
  // Non-finite values.
  const auto inf = std::numeric_limits<double>::infinity();
  CHECK(not restored.restore(tdigest_state{{NAN}, {1.0}, 1.0, 1.0}));
  CHECK(not restored.restore(tdigest_state{{inf}, {1.0}, 1.0, 1.0}));
  CHECK(not restored.restore(tdigest_state{{1.0}, {inf}, 1.0, 1.0}));
  CHECK(not restored.restore(tdigest_state{{1.0}, {1.0}, -inf, 1.0}));
  CHECK(not restored.restore(tdigest_state{{1.0}, {1.0}, 1.0, inf}));
  // The accumulated weight must remain finite.
  CHECK(not restored.restore(tdigest_state{{1.0, 2.0},
                                           {std::numeric_limits<double>::max(),
                                            std::numeric_limits<double>::max()},
                                           1.0,
                                           2.0}));
  // A failed restore resets the digest.
  CHECK(restored.is_empty());
}

} // namespace

} // namespace tenzir::detail
