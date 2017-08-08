#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/event.hpp"

#define SUITE system
#include "test.hpp"
#include "fixtures/node.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(query_tests, fixtures::node)

TEST(node queries) {
  ingest("bro");
  auto xs = query("proto == \"tcp\"");
  CHECK_EQUAL(xs.size(), 3135u);
}

FIXTURE_SCOPE_END()
