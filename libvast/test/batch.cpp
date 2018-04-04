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

#include "vast/batch.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/concept/printable/vast/event.hpp"

#define SUITE batch
#include "test.hpp"

using namespace vast;

namespace {

struct fixture {
  fixture() : event_type{integer_type{}} {
    event_type.name("foo");
    for (auto i = 0; i < 1000; ++i) {
      events.push_back(event::make(i, event_type));
      events.back().id(666 + i);
    }
  }

  type event_type;
  std::vector<event> events;
};

} // namespace <anonymous>

FIXTURE_SCOPE(batch_tests, fixture)

TEST(events with IDs) {
  MESSAGE("write a batch");
  batch::writer writer{compression::lz4};
  for (auto& e : events)
    if (!writer.write(e))
      REQUIRE(!"failed to write event");
  auto b = writer.seal();
  b.ids(666, 666 + 1000);
  MESSAGE("read a batch");
  batch::reader reader{b};
  auto xs = reader.read(make_ids({{666, 1666}}));
  REQUIRE(xs);
  CHECK(*xs == events);
  MESSAGE("read partial batch");
  batch::reader partial{b};
  xs = partial.read(make_ids({{666, 667}, {1567, 1657}}));
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->size(), 91u);
  CHECK_EQUAL(xs->front().id(), 666u);
  CHECK_EQUAL(xs->back().id(), 666u + 990);
}

TEST(events without IDs) {
  batch::writer writer{compression::lz4};
  for (auto i = 0; i < 42; ++i)
    if (!writer.write(event::make(i, event_type)))
      REQUIRE(!"failed to write event");
  auto b = writer.seal();
  batch::reader reader{b};
  auto xs = reader.read();
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->size(), 42u);
  CHECK_EQUAL(xs->front().id(), invalid_id);
  CHECK_EQUAL(xs->front().type().name(), "foo");
  CHECK_EQUAL(xs->front().type(), event_type);
  CHECK_EQUAL(xs->back().type(), event_type);
  CHECK_EQUAL(xs->back(), event::make(41, event_type));
}

FIXTURE_SCOPE_END()
