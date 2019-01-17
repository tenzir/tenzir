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

#include "vast/format/pcap.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"

using namespace vast;

TEST(PCAP read/write 1) {
  // Initialize a PCAP source with no cutoff (-1), and at most 5 flow table
  // entries.
  format::pcap::reader reader{traces::nmap_vsn, uint64_t(-1), 5};
  auto e = expected<event>{no_error};
  std::vector<event> events;
  while (e || !e.error()) {
    e = reader.read();
    if (e)
      events.push_back(std::move(*e));
  }
  REQUIRE(!e);
  CHECK(e.error() == ec::end_of_input);
  REQUIRE(!events.empty());
  CHECK_EQUAL(events.size(), 44u);
  CHECK_EQUAL(events[0].type().name(), "pcap::packet");
  auto pkt = caf::get_if<vector>(&events.back().data());
  REQUIRE(pkt);
  auto src = caf::get_if<address>(&pkt->at(1));
  REQUIRE(src);
  CHECK_EQUAL(*src, *to<address>("192.168.1.1"));
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
  format::pcap::reader reader{traces::workshop_2011_browse, 64, 100, 5, 2};
  auto e = expected<event>{no_error};
  std::vector<event> events;
  while (e || !e.error()) {
    e = reader.read();
    if (e)
      events.push_back(std::move(*e));
  }
  REQUIRE(!e);
  CHECK(e.error() == ec::end_of_input);
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
