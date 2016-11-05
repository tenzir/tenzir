#include "vast/batch.hpp"
#include "vast/event.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/concept/printable/vast/event.hpp"

#define SUITE batch
#include "test.hpp"

using namespace vast;

namespace {

struct fixture {
  fixture()
    : event_type{integer_type{}} {
    event_type.name() = "foo";
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
  batch::writer writer{compression::null};
  for (auto& e : events)
    if (!writer.write(e))
      REQUIRE(!"failed to write event");
  auto b = writer.seal();
  b.ids(666, 666 + 1000);
  MESSAGE("read a batch");
  batch::reader reader{b};
  bitmap ids;
  ids.append_bits(false, 666);
  ids.append_bits(true, 1000);
  auto xs = reader.read(ids);
  REQUIRE(xs);
  CHECK(*xs == events);
  MESSAGE("read partial batch");
  batch::reader partial{b};
  ids = bitmap{};
  ids.append_bits(false, 666);
  ids.append_bits(true, 1);
  ids.append_bits(false, 900);
  ids.append_bits(true, 90);
  ids.append_bits(false, 9);
  xs = partial.read(ids);
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->size(), 91u);
  CHECK_EQUAL(xs->front().id(), 666u);
  CHECK_EQUAL(xs->back().id(), 666u + 990);
}

TEST(events without IDs) {
  batch::writer writer{compression::null};
  std::cout << event_type.name() << std::endl;
  for (auto i = 0; i < 42; ++i)
    if (!writer.write(event::make(i, event_type)))
      REQUIRE(!"failed to write event");
  auto b = writer.seal();
  batch::reader reader{b};
  auto xs = reader.read();
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->size(), 42u);
  CHECK_EQUAL(xs->front().id(), invalid_event_id);
  CHECK_EQUAL(xs->front().type().name(), "foo");
  CHECK_EQUAL(xs->front().type(), event_type);
  CHECK_EQUAL(xs->back().type(), event_type);
  CHECK_EQUAL(xs->back(), event::make(41, event_type));
}

FIXTURE_SCOPE_END()
