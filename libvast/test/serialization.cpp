#include "vast/load.hpp"
#include "vast/maybe.hpp"
#include "vast/save.hpp"
#include "vast/concept/serializable/state.hpp"
#include "vast/concept/serializable/vast/maybe.hpp"
#include "vast/concept/serializable/vast/vector_event.hpp"

#define SUITE serialization
#include "test.hpp"
#include "fixtures/events.hpp"

using namespace vast;

TEST(maybe<T>) {
  maybe<int> m1, m2;
  m1 = 42;
  std::vector<char> buf;
  save(buf, m1);
  load(buf, m2);
  REQUIRE(m1);
  REQUIRE(m2);
  CHECK(*m2 == 42);
  CHECK(*m1 == *m2);
}

FIXTURE_SCOPE(events_scope, fixtures::simple_events)

// The serialization of events goes through custom (de)serialization routines
// to avoid redudant type serialization.
TEST(vector<event> serialization) {
  std::string buf;
  save(buf, events);
  std::vector<event> deserialized;
  load(buf, deserialized);
  CHECK(events == deserialized);
}

FIXTURE_SCOPE_END()
