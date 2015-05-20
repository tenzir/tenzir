#include "vast/actor/source/pcap.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("pcap source")
{
  scoped_actor self;
  auto fail = others() >> [&]
  {
    std::cerr << to_string(self->current_message()) << std::endl;
    REQUIRE(false);
  };
  // Spawn a PCAP source with a no cutoff and at most 5 concurrent flows.
  auto pcap = self->spawn<source::pcap, monitored>(traces::nmap_vsn, -1, 5);
  anon_send(pcap, put_atom::value, sink_atom::value, self);
  self->receive([&](upstream_atom, actor const& a) { CHECK(a == pcap); });
  // Run the source.
  anon_send(pcap, run_atom::value);
  self->receive(
    [&](chunk const& chk)
    {
      CHECK(chk.meta().schema.find_type("vast::packet") != nullptr);
      CHECK(chk.events() == 44);
    },
    fail
    );
  // The PCAP source terminates after having read the entire trace.
  self->receive(
      [&](down_msg const& d) { CHECK(d.reason == exit::done); },
      fail
      );
  // Spawn a PCAP source with a 64-byte cutoff, at most 100 flow table entries,
  // with flows inactive for more than 5 seconds to be evicted every 2 seconds.
  pcap = self->spawn<source::pcap, monitored>(
      traces::workshop_2011_browse, 64, 100, 5, 2);
  anon_send(pcap, put_atom::value, sink_atom::value, self);
  self->receive([&](upstream_atom, actor const& a) { CHECK(a == pcap); });
  anon_send(pcap, run_atom::value);
  self->receive(
      [&](chunk const& chk) { CHECK(chk.events() == 36); },
      fail
      );
  self->receive(
      [&](down_msg const& d) { CHECK(d.reason == exit::done); },
      fail
      );
  self->await_all_other_actors_done();
}
