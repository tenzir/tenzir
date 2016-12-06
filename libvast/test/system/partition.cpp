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
  directory /= "partition";
  MESSAGE("ingesting conn.log");
  auto p = self->spawn<monitored>(system::partition, directory, self);
  auto t = self->spawn<monitored>(
    system::task<steady_clock::time_point, uint64_t>,
    steady_clock::now(), bro_conn_log.size());
  schema sch;
  REQUIRE(sch.add(bro_conn_log[0].type()));
  self->send(p, bro_conn_log, sch, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  MESSAGE("ingesting http.log");
  t = self->spawn<monitored>(system::task<steady_clock::time_point, uint64_t>,
                             steady_clock::now(), bro_http_log.size());
  sch = {};
  REQUIRE(sch.add(bro_http_log[0].type()));
  self->send(p, bro_http_log, sch, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  auto send_query = [&] {
    MESSAGE("sending query");
    auto expr = to<expression>("string == \"SF\" && id.resp_p == 443/?");
    REQUIRE(expr);
    self->send(p, *expr, system::historical_atom::value);
    bool done = false;
    bitmap hits;
    self->do_receive(
      [&](expression const& e, bitmap const& bm, system::historical_atom) {
        CHECK(*expr == e);
        hits |= bm;
      },
      [&](system::done_atom, steady_clock::time_point, expression const& e) {
        CHECK(*expr == e);
        done = true;
      }
    ).until([&] { return done; });
    CHECK(rank(hits) == 38);
  };
  send_query();
  MESSAGE("flushing to filesystem");
  t = self->spawn<monitored>(system::task<>);
  self->send(t, p);
  self->send(p, flush_atom::value, t);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == t); },
    error_handler()
  );
  REQUIRE(exists(directory));
  REQUIRE(exists(directory / "0-8462" / "bro::conn" / "data" / "id" / "orig_h"));
  REQUIRE(exists(directory / "0-8462" / "bro::conn" / "meta" / "time"));
  MESSAGE("shutting down partition");
  self->monitor(p);
  self->send(p, system::shutdown_atom::value);
  self->receive(
    [&](down_msg const& msg) { CHECK(msg.source == p); },
    error_handler()
  );
  MESSAGE("loading persistent state from file system");
  p = self->spawn<monitored>(system::partition, directory, self);
  send_query();
  //MESSAGE("creating a continuous query");
  //expr = to<expression>("s ni \"7\"");
  //REQUIRE(expr);
  //self->send(p, *expr, continuous_atom::value);
  //MESSAGE("sending another event");
  //t = self->spawn<monitored>(task::make<time::moment, uint64_t>,
  //                           time::snapshot(), events.size());
  //self->send(p, events, sch, t);
  //self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  //MESSAGE("getting continuous hits");
  //self->receive(
  //  [&](expression const& e, bitmap const& hits, continuous_atom) {
  //    CHECK(*expr == e);
  //    // (0..1024)
  //    //   .select{|x| x % 2 == 0}
  //    //   .map{|x| x.to_s}
  //    //   .select{|x| x =~ /7/}
  //    //   .length == 95
  //    CHECK(hits.count() == 95);
  //  });
  //MESSAGE("disabling continuous query and sending another event");
  //self->send(p, *expr, continuous_atom::value, disable_atom::value);
  //auto e = event::make(record{1337u, std::to_string(1337)}, type0);
  //e.id(4711);
  //t = self->spawn<monitored>(task::make<time::moment, uint64_t>,
  //                           time::snapshot(), 1);
  //self->send(p, std::vector<event>{std::move(e)}, sch, t);
  //self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  //// Make sure that we didn't get any new hits.
  //CHECK(self->mailbox().count() == 0);
  //MESSAGE("cleaning up");
  //self->send_exit(p, exit::done);
  //self->await_all_other_actors_done();
  //rm(dir);
}

FIXTURE_SCOPE_END()
