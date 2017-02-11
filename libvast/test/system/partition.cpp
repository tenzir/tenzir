#include "vast/bitmap.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/system/partition.hpp"
#include "vast/system/task.hpp"

#define SUITE index
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace std::chrono;

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(steady_clock::time_point)

FIXTURE_SCOPE(partition_tests, fixtures::actor_system_and_events)

TEST(partition) {
  auto issue_query = [&](auto& part) {
    MESSAGE("sending query");
    auto expr = to<expression>("string == \"SF\" && id.resp_p == 443/?");
    REQUIRE(expr);
    self->request(part, infinite, *expr).receive(
      [&](bitmap const& hits) {
        CHECK_EQUAL(rank(hits), 38u);
      },
      error_handler()
    );
  };
  directory /= "partition";
  MESSAGE("ingesting conn.log");
  auto part = self->spawn(system::partition, directory);
  self->send(part, bro_conn_log);
  MESSAGE("ingesting http.log");
  self->send(part, bro_http_log);
  issue_query(part);
  MESSAGE("shutting down partition");
  self->send(part, system::shutdown_atom::value);
  self->wait_for(part);
  REQUIRE(exists(directory));
  REQUIRE(exists(directory / "547119946" / "data" / "id" / "orig_h"));
  REQUIRE(exists(directory / "547119946" / "meta" / "time"));
  MESSAGE("loading persistent state from file system");
  part = self->spawn(system::partition, directory);
  issue_query(part);
  self->send(part, system::shutdown_atom::value);
  self->wait_for(part);
}

FIXTURE_SCOPE_END()
