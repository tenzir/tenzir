//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/tdigest.hpp"

#include "tenzir/test/test.hpp"

#include <cmath>

namespace tenzir::detail {

namespace {

TEST("tdigest save and restore round-trips quantiles") {
  auto digest = tdigest{};
  for (auto i = 0; i < 10'000; ++i) {
    digest.add(static_cast<double>(i));
  }
  const auto state = digest.save();
  auto restored = tdigest{};
  REQUIRE(restored.restore(state));
  CHECK(restored.validate().has_value());
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
