#include "vast/format/pcap.hpp"
#include "vast/system/source.hpp"

#define SUITE system
#include "test.hpp"
#include "data.hpp"
#include "system/actor_system_fixture.hpp"

using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(source_tests, actor_system_fixture)

TEST(PCAP source) {
  format::pcap::reader reader{traces::nmap_vsn};
  auto src = self->spawn(source<format::pcap::reader>, std::move(reader));
  self->monitor(src);
  self->send(src, put_atom::value, sink_atom::value, self);
  self->send(src, run_atom::value);
  self->receive([&](std::vector<event> const& events) {
   REQUIRE(events.size() == 44);
   CHECK(events[0].type().name() == "pcap::packet");
  });
  // A source terminates normally after having consumed the entire input.
  self->receive([&](caf::down_msg const& msg) {
    CHECK(msg.reason == caf::exit_reason::normal);
  });
}

FIXTURE_SCOPE_END()
