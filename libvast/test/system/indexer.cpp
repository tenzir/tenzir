#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/bitmap.hpp"

#include "vast/system/indexer.hpp"
#include "vast/system/task.hpp"

#define SUITE index
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(indexer_tests, fixtures::actor_system_and_events)

TEST(indexer) {
  const auto conn_log_type = bro_conn_log[0].type();
  auto i = self->spawn(system::event_indexer, directory, conn_log_type);
  auto t = self->spawn<monitored>(system::task<>);
  MESSAGE("ingesting events");
  self->send(t, i);
  self->send(i, bro_conn_log, t);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == t); },
    error_handler()
  );
  // Event indexers operate with predicates, whereas partitions take entire
  // expressions.
  MESSAGE("querying");
  auto pred = to<predicate>("id.resp_p == 995/?");
  REQUIRE(pred);
  t = self->spawn<monitored>(system::task<>);
  self->send(t, i);
  self->send(i, *pred, self, t);
  bitmap result;
  self->receive(
    [&](predicate const& p, bitmap const& bm) {
      CHECK(p == *pred);
      result = bm;
    },
    error_handler()
  );
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == t); },
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
  MESSAGE("flushing to filesystem");
  t = self->spawn<monitored>(system::task<>);
  self->send(t, i);
  self->send(i, flush_atom::value, t);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == t); },
    error_handler()
  );
  CHECK(exists(directory));
  CHECK(exists(directory / "data" / "id" / "orig_h"));
  CHECK(exists(directory / "meta" / "time"));
  MESSAGE("shutting down indexer");
  self->monitor(i);
  self->send(i, system::shutdown_atom::value);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == i); },
    error_handler()
  );
  MESSAGE("respawning indexer from file system");
  i = self->spawn(system::event_indexer, directory, conn_log_type);
  // Same as above: create a task, submit the query, verify the result.
  t = self->spawn<monitored>(system::task<>);
  self->send(t, i);
  self->send(i, *pred, self, t);
  self->receive(
    [&](predicate const& p, bitmap const& bm) {
      CHECK(p == *pred);
      result = bm;
    },
    error_handler()
  );
  CHECK_EQUAL(rank(result), 53u);
}

FIXTURE_SCOPE_END()
