#include "vast/segment.hpp"

#include "test.hpp"

#include "fixtures/events.hpp"

using namespace vast;

FIXTURE_SCOPE(view_tests, fixtures::events)

TEST(segment) {
  auto offset = 1000;
  MESSAGE("building segment");
  auto builder = segment_builder{10 << 20}; // 10 MB anonymous mapping
  auto id = event_id{0};
  for (auto& e : bro_conn_log) {
    e.id(offset + id++); // Assigning event IDs for this test.
    CHECK(builder.put(e));
  }
  MESSAGE("performing lookup before having finished");
  auto e = builder.get(offset + 42);
  REQUIRE(e);
  CHECK_EQUAL(*e, bro_conn_log[42]);
  MESSAGE("completing segment");
  auto chk = builder.finish();
  REQUIRE(chk);
  MESSAGE("viewing segment");
  auto viewer = segment_viewer{*chk};
  CHECK_EQUAL(viewer.size(), bro_conn_log.size());
  e = viewer.get(offset + 42);
  REQUIRE(e);
  CHECK_EQUAL(e, bro_conn_log[42]);
}

FIXTURE_SCOPE_END()
