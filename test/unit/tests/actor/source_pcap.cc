#include "vast/actor/source/pcap.h"

#define SUITE actors
#include "test.h"
#include "data.h"

using namespace vast;

TEST(pcap_source) {
  scoped_actor self;
  self->on_sync_failure([&] {
    FAIL("got unexpected message: " << to_string(self->current_message()));
  });
  MESSAGE("spawning pcap source with no cutoff and <= 5 concurrent flows");
  auto pcap = self->spawn<source::pcap, monitored>(traces::nmap_vsn, -1, 5);
  anon_send(pcap, put_atom::value, sink_atom::value, self);
  self->receive([&](upstream_atom, actor const& a) { CHECK(a == pcap); });
  MESSAGE("running the source");
  anon_send(pcap, run_atom::value);
  self->receive([&](std::vector<event> const& events) {
   REQUIRE(events.size() == 44);
   CHECK(events[0].type().name() == "pcap::packet");
  });
  // The PCAP source terminates after having read the entire trace.
  self->receive(
    [&](down_msg const& d) { CHECK(d.reason == exit::done); }
  );
  // Spawn a PCAP source with a 64-byte cutoff, at most 100 flow table entries,
  // with flows inactive for more than 5 seconds to be evicted every 2 seconds.
  MESSAGE("spawning pcap source with 64B cutoff and <= 100 concurrent flows");
  pcap = self->spawn<source::pcap, monitored>(
    traces::workshop_2011_browse, 64, 100, 5, 2);
  anon_send(pcap, put_atom::value, sink_atom::value, self);
  self->receive([&](upstream_atom, actor const& a) { CHECK(a == pcap); });
  anon_send(pcap, run_atom::value);
  self->receive(
    [&](std::vector<event> const& events) { CHECK(events.size() == 36); }
  );
  self->receive(
    [&](down_msg const& d) { CHECK(d.reason == exit::done); }
  );
  self->await_all_other_actors_done();
}
