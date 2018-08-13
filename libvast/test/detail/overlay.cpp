/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE overlay

#include "test.hpp"
#include "fixtures/events.hpp"

#include <iomanip>

#include "vast/si_literals.hpp"

#include "vast/detail/mmapbuf.hpp"
#include "vast/detail/overlay.hpp"

using namespace vast;
using namespace vast::binary_byte_literals;

namespace {

struct fixture : fixtures::events {
  fixture() {
    std::transform(bro_conn_log.begin(),
                   bro_conn_log.end(),
                   std::back_inserter(xs),
                   [](auto& x) { return x.data(); });
  }

  std::vector<data> xs;
};

} // namespace <anonymous>

FIXTURE_SCOPE(overlay_tests, fixture)

TEST(writing and reading) {
  // Serialize a vector of data.
  std::stringbuf sb;
  detail::overlay::writer writer{sb};
  for (auto& x : xs)
    CHECK(writer.write(x));
  auto size = writer.finish();
  auto ascii_size = 1'026'256.0; // bro-cut < conn.log | wc -c
  auto ratio = size / ascii_size;
  MESSAGE("packed/ASCII bytes ratio: " << ratio);
  // Selectively deserialize values.
  detail::overlay::reader reader{sb};
  // Check first.
  auto x = reader.read<data>(0);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.front());
  // Check second.
  x = reader.read<data>(1);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[1]);
  // Check random.
  x = reader.read<data>(42);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[42]);
  // Check last.
  x = reader.read<data>(reader.size() - 1);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.back());
}

TEST(reading while writing) {
  std::stringbuf sb;
  MESSAGE("writing");
  detail::overlay::writer writer{sb};
  for (auto i = 0u; i < 10; ++i)
    CHECK(writer.write(xs[i]));
  CHECK_EQUAL(writer.size(), 10u);
  auto x = writer.read<data>(0);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.front());
  x = writer.read<data>(7);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[7]);
  for (auto i = 10u; i < 20; ++i)
    CHECK(writer.write(xs[i]));
  CHECK_EQUAL(writer.size(), 20u);
  x = writer.read<data>(15);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[15]);
  writer.finish();
  MESSAGE("reading");
  detail::overlay::reader reader{sb};
  REQUIRE_EQUAL(reader.size(), 20u);
  for (auto i = 0u; i < reader.size(); ++i) {
    x = reader.read<data>(i);
    REQUIRE(x);
    CHECK_EQUAL(*x, xs[i]);
  }
}

TEST(viewing) {
  MESSAGE("writing");
  detail::mmapbuf sb{2_MiB};
  detail::overlay::writer writer{sb};
  for (auto& x : xs)
    CHECK(writer.write(x));
  auto size = writer.finish();
  REQUIRE(size < sb.size());
  REQUIRE(sb.resize(size));
  MESSAGE("viewer access");
  auto viewer = detail::overlay::viewer{sb.release()};
  CHECK_EQUAL(viewer.size(), xs.size());
  MESSAGE("deserialize a specific element");
  auto x = viewer.read<data>(42);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[42]);
}

FIXTURE_SCOPE_END()
