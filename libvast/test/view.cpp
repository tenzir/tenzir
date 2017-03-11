#include "vast/data.hpp"
#include "vast/data_view.hpp"
#include "vast/event.hpp"
#include "vast/event_view.hpp"
#include "vast/pack.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#define SUITE views
#include "test.hpp"

#include "fixtures/events.hpp"

using namespace vast;

namespace {

} // namespace <anonymous>

FIXTURE_SCOPE(view_tests, fixtures::events)

TEST(data) {
  auto& x = bro_conn_log[0].data(); // first line, 115 bytes in ASCII.
  auto chk = pack(x);
  MESSAGE("packed data into chunk of " << chk->size() << " bytes");
  auto v = get_if<vector_view>(data_view{chk});
  REQUIRE(v);
  CHECK_EQUAL(unpack(*v), x);
}

TEST(event) {
  // TODO
}

FIXTURE_SCOPE_END()
