#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/query_options.hpp"

#include "vast/system/index.hpp"

#define SUITE index
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace std::chrono;

FIXTURE_SCOPE(index_tests, fixtures::actor_system_and_events)

namespace {

auto issue_query = [](auto& self, auto& idx, auto error_handler) {
  auto expr = to<expression>("string == \"SF\" && id.resp_p == 443/?");
  REQUIRE(expr);
  self->send(idx, *expr, historical, self);
  actor task;
  self->receive(
    [&](actor const& t) {
      REQUIRE(t);
      self->monitor(t);
      task = t;
    },
    error_handler
  );
  bool done = false;
  bitmap hits;
  self->do_receive(
    [&](bitmap const& bm) {
      CHECK(!bm.empty());
      hits |= bm;
    },
    [&](system::done_atom, timespan, expression const& e) {
      CHECK(*expr == e);
      done = true;
    }
  ).until([&] { return done; });
  CHECK_EQUAL(rank(hits), 38u);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == task); },
    error_handler
  );
};

} // namespace <anonymous>

TEST(index) {
  directory /= "index";
  MESSAGE("ingesting conn.log");
  auto idx = self->spawn<monitored>(system::index, directory, 1000, 2);
  self->send(idx, bro_conn_log);
  self->send(idx, bro_http_log);
  MESSAGE("issueing query against active partition");
  issue_query(self, idx, error_handler());
  MESSAGE("shutting down index");
  self->send(idx, system::shutdown_atom::value);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == idx); },
    error_handler()
  );
  MESSAGE("reloading index");
  idx = self->spawn<monitored>(system::index, directory, 1000, 2);
  MESSAGE("issueing query against passive partition");
  issue_query(self, idx, error_handler());
  MESSAGE("shutting down index");
  self->send(idx, system::shutdown_atom::value);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == idx); },
    error_handler()
  );
  //MESSAGE("creating a continuous query");
  //// The expression must have already been normalized as it hits the index.
  //expr = to<expression>("s ni \"7\"");
  //REQUIRE(expr);
  //self->send(idx, *expr, continuous, self);
  //self->receive(
  //  [&](actor const& t) {
  //    REQUIRE(t != invalid_actor);
  //    self->monitor(t);
  //    task = t;
  //  });
  //MESSAGE("sending another event batch and getting continuous hits");
  //self->send(idx, events);
  //self->receive([&](bitstream_type const& bs) { CHECK(bs.count() == 95); });
  //MESSAGE("disabling continuous query and sending another event");
  //self->send(idx, *expr, continuous_atom::value, disable_atom::value);
  //self->receive([&](down_msg const& msg) { CHECK(msg.source == task); });
  //auto e = event::make(record{1337u, std::to_string(1337)}, type0);
  //e.id(4711);
  //self->send(idx, std::vector<event>{std::move(e)});
  //// Make sure that we didn't get any new hits.
  //CHECK(self->mailbox().count() == 0);
  //MESSAGE("cleaning up");
  //self->send_exit(idx, exit::done);
  //self->await_all_other_actors_done();
  //rm(dir);
}

FIXTURE_SCOPE_END()
