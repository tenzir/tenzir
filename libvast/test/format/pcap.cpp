/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE format
#include "vast/test/test.hpp"
#include "vast/test/data.hpp"

#include "vast/test/fixtures/actor_system.hpp"

#include "vast/format/pcap.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"
#include "vast/to_events.hpp"

using namespace vast;

// Technically, we don't need the actor system. However, we do need to
// initialize the table slice builder factories which happens automatically in
// the actor system setup. Further, including this fixture gives us access to
// log files to hunt down bugs faster.
FIXTURE_SCOPE(pcap_tests, fixtures::deterministic_actor_system)

TEST(PCAP read/write 1) {
  // Initialize a PCAP source with no cutoff (-1), and at most 5 flow table
  // entries.
  format::pcap::reader reader{defaults::system::table_slice_type,
                              artifacts::traces::nmap_vsn, uint64_t(-1), 5};
  std::vector<event> events;
  auto add_events = [&](const table_slice_ptr& slice) {
    to_events(events, *slice);
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     defaults::system::table_slice_size,
                                     add_events);
  CHECK_EQUAL(err, ec::end_of_input);
  REQUIRE_EQUAL(events.size(), produced);
  REQUIRE(!events.empty());
  CHECK_EQUAL(events.size(), 44u);
  CHECK_EQUAL(events[0].type().name(), "pcap::packet");
  auto pkt = unbox(caf::get_if<vector>(&events.back().data()));
  auto src = unbox(caf::get_if<address>(&pkt.at(1)));
  CHECK_EQUAL(src, unbox(to<address>("192.168.1.1")));
  MESSAGE("write out read packets");
  auto file = "vast-unit-test-nmap-vsn.pcap";
  format::pcap::writer writer{file};
  auto deleter = caf::detail::make_scope_guard([&] { rm(file); });
  for (auto& e : events)
    REQUIRE(writer.write(e));
}

TEST(PCAP read/write 2) {
  // Spawn a PCAP source with a 64-byte cutoff, at most 100 flow table entries,
  // with flows inactive for more than 5 seconds to be evicted every 2 seconds.
  format::pcap::reader reader{defaults::system::table_slice_type,
                              artifacts::traces::workshop_2011_browse,
                              64,
                              100,
                              5,
                              2};
  std::vector<event> events;
  auto add_events = [&](const table_slice_ptr& slice) {
    to_events(events, *slice);
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     defaults::system::table_slice_size,
                                     add_events);
  CHECK_EQUAL(err, ec::end_of_input);
  REQUIRE_EQUAL(events.size(), produced);
  REQUIRE(!events.empty());
  CHECK_EQUAL(events.size(), 36u);
  CHECK_EQUAL(events[0].type().name(), "pcap::packet");
  MESSAGE("write out read packets");
  auto file = "vast-unit-test-workshop-2011-browse.pcap";
  format::pcap::writer writer{file};
  auto deleter = caf::detail::make_scope_guard([&] { rm(file); });
  for (auto& e : events)
    if (!writer.write(e))
      FAIL("failed to write event");
}

FIXTURE_SCOPE_END()
