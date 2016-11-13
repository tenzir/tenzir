#include "vast/format/pcap.hpp"
#include "vast/system/sink.hpp"

#define SUITE system
#include "test.hpp"
#include "data.hpp"
#include "system/actor_system_fixture.hpp"

using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(sink_tests, actor_system_fixture)

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
  self->monitor(snk);
  MESSAGE("sending events");
  self->send(snk, std::move(events));
  MESSAGE("shutting down");
  self->send(snk, shutdown_atom::value);
  self->receive([&](caf::down_msg const& msg) {
    CHECK(msg.reason == caf::exit_reason::user_shutdown);
  });
}

FIXTURE_SCOPE_END()
