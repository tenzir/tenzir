#include <iomanip>

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
  vector xs;
  std::transform(bro_conn_log.begin(), bro_conn_log.end(),
                 std::back_inserter(xs), [](auto& x) { return x.data(); });
  auto chk = pack(data{xs});
  auto ratio = chk->size() / 1'026'256.0; // bro-cut < conn.log | wc -c
  MESSAGE("ASCII/packed bytes ratio: " << std::setprecision(3) << ratio);
  auto v = get_if<vector_view>(data_view{chk});
  REQUIRE(v);
  CHECK_EQUAL(unpack(*v), xs);
}

TEST(event) {
  // TODO
}

FIXTURE_SCOPE_END()
