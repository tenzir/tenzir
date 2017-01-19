#include "vast/format/pcap.hpp"
#include "vast/system/sink.hpp"

#define SUITE system
#include "test.hpp"
#include "data.hpp"
#include "fixtures/actor_system.hpp"

using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(sink_tests, fixtures::actor_system)

TEST(PCAP sink) {
  // First read some events.
  format::pcap::reader reader{traces::nmap_vsn};
  maybe<event> e;
  std::vector<event> events;
  while (!e.error()) {
    e = reader.read();
    if (e)
      events.push_back(std::move(*e));
  }
  REQUIRE_EQUAL(events.size(), 44u);
  // Now write those events.
  MESSAGE("constructing a sink");
  format::pcap::writer writer{"/dev/null"};
  auto snk = self->spawn(sink<format::pcap::writer>, std::move(writer));
  MESSAGE("sending events");
  self->send(snk, std::move(events));
  MESSAGE("shutting down");
  self->send_exit(snk, caf::exit_reason::user_shutdown);
  self->wait_for(snk);
}

FIXTURE_SCOPE_END()
