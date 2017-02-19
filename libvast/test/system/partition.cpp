#include "vast/bitmap.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/system/partition.hpp"
#include "vast/system/task.hpp"

#define SUITE system
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace std::chrono;

namespace {

struct partition_fixture : fixtures::actor_system_and_events {
  partition_fixture() {
    directory /= "partition";
    MESSAGE("ingesting conn.log");
    partition = self->spawn(system::partition, directory);
    self->send(partition, bro_conn_log);
    MESSAGE("ingesting http.log");
    self->send(partition, bro_http_log);
    MESSAGE("completed ingestion");
  }

  ~partition_fixture() {
    self->send(partition, system::shutdown_atom::value);
    self->wait_for(partition);
  }

  bitmap query(const std::string& str) {
    MESSAGE("sending query");
    auto expr = to<expression>(str);
    REQUIRE(expr);
    bitmap result;
    self->request(partition, infinite, *expr).receive(
      [&](bitmap& hits) {
        result = std::move(hits);
      },
      error_handler()
    );
    MESSAGE("shutting down partition");
    self->send(partition, system::shutdown_atom::value);
    self->wait_for(partition);
    REQUIRE(exists(directory));
    REQUIRE(exists(directory / "547119946" / "data" / "id" / "orig_h"));
    REQUIRE(exists(directory / "547119946" / "meta" / "time"));
    MESSAGE("respawning partition and sending query again");
    partition = self->spawn(system::partition, directory);
    self->request(partition, infinite, *expr).receive(
      [&](const bitmap& hits) {
        REQUIRE_EQUAL(hits, result);
      },
      error_handler()
    );
    return result;
  }

  actor partition;
};

} // namespace <anonymous>

FIXTURE_SCOPE(partition_tests, partition_fixture)

TEST(partition queries 1) {
  auto hits = query(":string == \"SF\" && id.resp_p == 443/?");
  CHECK_EQUAL(rank(hits), 38u);
}

TEST(partition queries 2) {
  auto hits = query("service == \"http\" && :addr == 212.227.96.110");
  CHECK_EQUAL(rank(hits), 28u);
}

FIXTURE_SCOPE_END()
