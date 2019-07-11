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
  size_t events_produces = 0;
  table_slice_ptr slice;
  auto add_slice = [&](const table_slice_ptr& x) {
    REQUIRE(slice == nullptr);
    REQUIRE(x != nullptr);
    slice = x;
    events_produces = x->rows();
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     100, // we expect only 44 events
                                     add_slice);
  CHECK_EQUAL(err, ec::end_of_input);
  CHECK_EQUAL(events_produces, 44u);
  CHECK_EQUAL(slice->layout().name(), "pcap.packet");
  auto src_field = slice->at(43, 1);
  auto src = unbox(caf::get_if<view<address>>(&src_field));
  CHECK_EQUAL(src, unbox(to<address>("192.168.1.1")));
  MESSAGE("write out read packets");
  auto file = "vast-unit-test-nmap-vsn.pcap";
  format::pcap::writer writer{file};
  auto deleter = caf::detail::make_scope_guard([&] { rm(file); });
  REQUIRE_EQUAL(writer.write(*slice), caf::none);
}

/*
TEST(PCAP read/write 2) {
  // Spawn a PCAP source with a 64-byte cutoff, at most 100 flow table entries,
  // with flows inactive for more than 5 seconds to be evicted every 2 seconds.
  format::pcap::reader reader{defaults::system::table_slice_type,
                              artifacts::traces::workshop_2011_browse,
                              64,
                              100,
                              5,
                              2};
  std::vector<table_slice_ptr> slices;
  auto add_slice = [&](const table_slice_ptr& slice) {
    slices.emplace_back(slice);
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     defaults::system::table_slice_size,
                                     add_slice);
  CHECK_EQUAL(err, ec::end_of_input);
  REQUIRE_EQUAL(events.size(), produced);
  REQUIRE(!events.empty());
  CHECK_EQUAL(events.size(), 36u);
  CHECK_EQUAL(events[0].type().name(), "pcap.packet");
  MESSAGE("write out read packets");
  auto file = "vast-unit-test-workshop-2011-browse.pcap";
  format::pcap::writer writer{file};
  auto deleter = caf::detail::make_scope_guard([&] { rm(file); });
  for (auto& slice : events)
    REQUIRE_EQUAL(writer.write(slice), caf::none);
}
*/

FIXTURE_SCOPE_END()
