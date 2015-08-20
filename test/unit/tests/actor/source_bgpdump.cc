#include "vast/actor/source/bgpdump.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/subnet.h"
#include "vast/io/file_stream.h"

#define SUITE actors
#include "test.h"
#include "data.h"

using namespace vast;

TEST(bgpdump_source) {
  scoped_actor self;
  auto f = bgpdump::updates20140821;
  auto is = std::make_unique<vast::io::file_input_stream>(f);
  auto bgpdump = self->spawn<source::bgpdump>(std::move(is));
  self->monitor(bgpdump);
  anon_send(bgpdump, put_atom::value, sink_atom::value, self);
  self->receive([&](upstream_atom, actor const& a) { CHECK(a == bgpdump); });

  MESSAGE("running the source");
  anon_send(bgpdump, run_atom::value);
  self->receive(
    [&](std::vector<event> const& events) {
      REQUIRE(events.size() == 11782);
      CHECK(events[0].type().name() == "bgpdump::state_change");
      auto r = get<record>(events[0]);
      REQUIRE(r);
      CHECK((*r)[1] == *to<address>("2a02:20c8:1f:1::4"));
      CHECK((*r)[2] == 50304u);
      CHECK((*r)[3] == "3");
      CHECK((*r)[4] == "2");
      CHECK(events[2].type().name() == "bgpdump::announcement");
      r = get<record>(events[2]);
      REQUIRE(r);
      CHECK((*r)[1] == *to<address>("2001:8e0:0:ffff::9"));
      auto as_path = get<vector>((*r)[4]);
      REQUIRE(as_path);
      CHECK(as_path->size() == 4);
      CHECK((*as_path)[3] == 15194u);
      CHECK(events[13].type().name() == "bgpdump::withdrawn");
      r = get<record>(events[13]);
      REQUIRE(r);
      CHECK((*r)[1] == *to<address>("68.67.63.245"));
      CHECK((*r)[2] == 22652u);
      CHECK((*r)[3] == *to<subnet>("188.123.160.0/19"));
    }
  );
  // The source terminates after having read the entire log file.
  self->receive(
    [&](down_msg const& d) { CHECK(d.reason == exit::done); }
  );
  self->await_all_other_actors_done();
}
