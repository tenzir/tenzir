#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/bitmap.hpp"

#include "vast/system/indexer.hpp"

#define SUITE system
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(indexer_tests, fixtures::actor_system_and_events)

TEST(indexer) {
  directory /= "indexer";
  const auto conn_log_type = bro_conn_log[0].type();
  auto i = self->spawn(system::event_indexer, directory, conn_log_type);
  MESSAGE("ingesting events");
  self->send(i, bro_conn_log);
  // Event indexers operate with predicates, whereas partitions take entire
  // expressions.
  MESSAGE("querying");
  auto pred = to<predicate>("id.resp_p == 995/?");
  REQUIRE(pred);
  bitmap result;
  self->request(i, infinite, *pred).receive(
    [&](bitmap& bm) {
      result = std::move(bm);
    },
    error_handler()
  );
  CHECK_EQUAL(rank(result), 53u);
  auto check_uid = [](event const& e, std::string const& uid) {
    auto& v = get<vector>(e.data());
    return v[1] == uid;
  };
  for (auto i : select(result))
    if (i == 819)
      CHECK(check_uid(bro_conn_log[819], "KKSlmtmkkxf")); // first
    else if (i == 3594)
      CHECK(check_uid(bro_conn_log[3594], "GDzpFiROJQi")); // intermediate
    else if (i == 6338)
      CHECK(check_uid(bro_conn_log[6338], "zwCckCCgXDb")); // last
  MESSAGE("shutting down indexer");
  self->send(i, system::shutdown_atom::value);
  self->wait_for(i);
  CHECK(exists(directory));
  CHECK(exists(directory / "data" / "id" / "orig_h"));
  CHECK(exists(directory / "meta" / "time"));
  MESSAGE("respawning indexer from file system");
  i = self->spawn(system::event_indexer, directory, conn_log_type);
  // Same as above: submit the query and verify the result.
  self->request(i, infinite, *pred).receive(
    [&](bitmap& bm) {
      CHECK_EQUAL(rank(bm), 53u);
    },
    error_handler()
  );
  // Test another query that involves a map-reduce computation of value
  // indexers.
  pred = to<predicate>(":addr == 65.55.184.16");
  REQUIRE(pred);
  self->request(i, infinite, *pred).receive(
    [&](const bitmap& bm) {
      CHECK_EQUAL(rank(bm), 2u);
    },
    error_handler()
  );
}

FIXTURE_SCOPE_END()
