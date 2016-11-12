#include "vast/event.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/format/pcap.hpp"

#define SUITE format
#include "test.hpp"
#include "data.hpp"

using namespace vast;

namespace {

struct fixture {
  format::pcap pcap;
  std::vector<event> events;
};

} // namespace <anonymous>

FIXTURE_SCOPE(pcap_tests, fixture)

TEST(PCAP source 1) {
  // Initialize a PCAP source with no cutoff (-1), and at most 5 flow table
  // entries.
  pcap.init(traces::nmap_vsn, -1, 5);
  maybe<event> e;
  while (!e.error()) {
    e = pcap.extract();
    if (e)
      events.push_back(std::move(*e));
  }
  CHECK(e.error() == ec::end_of_input);
  REQUIRE(!events.empty());
  CHECK_EQUAL(events.size(), 44u);
  CHECK_EQUAL(events[0].type().name(), "pcap::packet");
  auto pkt = get_if<vector>(events.back().data());
  REQUIRE(pkt);
  auto conn_id = get_if<vector>(pkt->at(0));
  REQUIRE(conn_id); //[192.168.1.1, 192.168.1.71, 53/udp, 64480/udp]
  auto src = get_if<address>(conn_id->at(0));
  REQUIRE(src);
  CHECK_EQUAL(*src, *to<address>("192.168.1.1"));
}

TEST(PCAP source 2) {
  // Spawn a PCAP source with a 64-byte cutoff, at most 100 flow table entries,
  // with flows inactive for more than 5 seconds to be evicted every 2 seconds.
  pcap.init(traces::workshop_2011_browse, 64, 100, 5, 2);
  maybe<event> e;
  while (!e.error()) {
    e = pcap.extract();
    if (e)
      events.push_back(std::move(*e));
  }
  CHECK(e.error() == ec::end_of_input);
  REQUIRE(!events.empty());
  CHECK_EQUAL(events.size(), 36u);
  CHECK_EQUAL(events[0].type().name(), "pcap::packet");
}

FIXTURE_SCOPE_END()
