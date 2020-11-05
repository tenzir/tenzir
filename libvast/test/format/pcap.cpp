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
#include "vast/format/pcap.hpp"

#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_column.hpp"

using namespace vast;

namespace {

// Baseline computed via `./community-id.py nmap_vsn.pcap` from the
// repository https://github.com/corelight/community-id-spec.
std::string_view community_ids[] = {
  "1:S2JPnyxVrN68D+w4ZMxKNeyQoNI=", "1:S2JPnyxVrN68D+w4ZMxKNeyQoNI=",
  "1:holOOTgd0/2k/ojauB8VsMbd2pI=", "1:holOOTgd0/2k/ojauB8VsMbd2pI=",
  "1:Vzc86YWBMwkcA1dPNrPN6t5hvj4=", "1:QbjD7ZBgS/i6o4RS0ovLWNhArt0=",
  "1:gvhz8+T8uMPcj1nTxa7QZCz4RkI=", "1:8iil9/ZM2nGLcSw5H1hLk3AB4OY=",
  "1:8EW/SvA6t3JXhn5vefyUyYCtPQY=", "1:8EW/SvA6t3JXhn5vefyUyYCtPQY=",
  "1:8EW/SvA6t3JXhn5vefyUyYCtPQY=", "1:8EW/SvA6t3JXhn5vefyUyYCtPQY=",
  "1:Vzc86YWBMwkcA1dPNrPN6t5hvj4=", "1:Vzc86YWBMwkcA1dPNrPN6t5hvj4=",
  "1:Vzc86YWBMwkcA1dPNrPN6t5hvj4=", "1:gvhz8+T8uMPcj1nTxa7QZCz4RkI=",
  "1:6r39sKcWauHVhKZ+Z92/0UK9lNg=", "1:xIXIGoyl8i+RURiBec05S5X8XEk=",
  "1:Ry5Au48dLKiT1Sq7N1kqT7n0wn8=", "1:EP0qhzV2s6lNTSAErUFzHBDLXog=",
  "1:0FtkY5KIWLZIwfKcr7k3dLvAkpo=", "1:HzDIiZWEeOnjh8jBPlvUCnCxemo=",
  "1:bMRO6UR8tNUnjnO3GuJCXs/ufuo=", "1:4O0NCs9k1xB4iZqlTYsOMaeZPiE=",
  "1:I7m0KKPgV/VUUmVf2aJkP+iDKNw=", "1:xIXIGoyl8i+RURiBec05S5X8XEk=",
  "1:0FtkY5KIWLZIwfKcr7k3dLvAkpo=", "1:4O0NCs9k1xB4iZqlTYsOMaeZPiE=",
  "1:7xMlZ3kChAVsoDvCm6u5nsrqjMY=", "1:7xMlZ3kChAVsoDvCm6u5nsrqjMY=",
  "1:7xMlZ3kChAVsoDvCm6u5nsrqjMY=", "1:7xMlZ3kChAVsoDvCm6u5nsrqjMY=",
  "1:zjGM746aZkpYb2mVIlsgLrUG59k=", "1:zjGM746aZkpYb2mVIlsgLrUG59k=",
  "1:zjGM746aZkpYb2mVIlsgLrUG59k=", "1:zjGM746aZkpYb2mVIlsgLrUG59k=",
  "1:zjGM746aZkpYb2mVIlsgLrUG59k=", "1:zjGM746aZkpYb2mVIlsgLrUG59k=",
  "1:zjGM746aZkpYb2mVIlsgLrUG59k=", "1:zjGM746aZkpYb2mVIlsgLrUG59k=",
  "1:zjGM746aZkpYb2mVIlsgLrUG59k=", "1:zjGM746aZkpYb2mVIlsgLrUG59k=",
  "1:zjGM746aZkpYb2mVIlsgLrUG59k=", "1:zjGM746aZkpYb2mVIlsgLrUG59k=",
};

} // namespace

// Technically, we don't need the actor system. However, we do need to
// initialize the table slice builder factories which happens automatically in
// the actor system setup. Further, including this fixture gives us access to
// log files to hunt down bugs faster.
FIXTURE_SCOPE(pcap_tests, fixtures::deterministic_actor_system)

TEST(PCAP read/write 1) {
  // Initialize a PCAP source with no cutoff (-1), and at most 5 flow table
  // entries.
  caf::settings settings;
  caf::put(settings, "vast.import.pcap.read", artifacts::traces::nmap_vsn);
  caf::put(settings, "vast.import.pcap.cutoff", static_cast<uint64_t>(-1));
  caf::put(settings, "vast.import.pcap.max-flows", static_cast<size_t>(5));
  // A non-positive value disables the timeout. We need to do this because the
  // deterministic actor system is messing with the clocks.
  caf::put(settings, "vast.import.batch-timeout", "0s");
  format::pcap::reader reader{defaults::import::table_slice_type,
                              std::move(settings)};
  size_t events_produced = 0;
  table_slice_ptr slice;
  auto add_slice = [&](const table_slice_ptr& x) {
    REQUIRE(slice == nullptr);
    REQUIRE(x != nullptr);
    slice = x;
    events_produced = x->rows();
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     100, // we expect only 44 events
                                     add_slice);
  CHECK_EQUAL(err, ec::end_of_input);
  REQUIRE_EQUAL(events_produced, 44u);
  auto layout = slice->layout();
  CHECK_EQUAL(layout.name(), "pcap.packet");
  auto src_field = slice->at(43, 1);
  auto src = unbox(caf::get_if<view<address>>(&src_field));
  CHECK_EQUAL(src, unbox(to<address>("192.168.1.1")));
  auto community_id_column = table_slice_column::make(slice, "community_id");
  REQUIRE(community_id_column);
  for (size_t row = 0; row < 44; ++row)
    CHECK_VARIANT_EQUAL((*community_id_column)[row], community_ids[row]);
  MESSAGE("write out read packets");
  auto file = "vast-unit-test-nmap-vsn.pcap";
  format::pcap::writer writer{file};
  auto deleter = caf::detail::make_scope_guard([&] { rm(file); });
  REQUIRE_EQUAL(writer.write(slice), caf::none);
}

TEST(PCAP read/write 2) {
  // Spawn a PCAP source with a 64-byte cutoff, at most 100 flow table entries,
  // with flows inactive for more than 5 seconds to be evicted every 2 seconds.
  caf::settings settings;
  caf::put(settings, "vast.import.pcap.read", artifacts::traces::nmap_vsn);
  caf::put(settings, "vast.import.pcap.cutoff", static_cast<uint64_t>(64));
  caf::put(settings, "vast.import.pcap.max-flows", static_cast<size_t>(100));
  caf::put(settings, "vast.import.pcap.max-flow-age", static_cast<size_t>(5));
  caf::put(settings, "vast.import.pcap.flow-expiry", static_cast<size_t>(2));
  // A non-positive value disables the timeout. We need to do this because the
  // deterministic actor system is messing with the clocks.
  caf::put(settings, "vast.import.batch-timeout", "0s");
  format::pcap::reader reader{defaults::import::table_slice_type,
                              std::move(settings)};
  table_slice_ptr slice;
  auto add_slice = [&](const table_slice_ptr& x) {
    REQUIRE_EQUAL(slice, nullptr);
    slice = x;
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     100, // we expect only 36 events
                                     add_slice);
  REQUIRE_NOT_EQUAL(slice, nullptr);
  CHECK_EQUAL(err, ec::end_of_input);
  REQUIRE_EQUAL(produced, 36u);
  CHECK_EQUAL(slice->rows(), 36u);
  auto layout = slice->layout();
  CHECK_EQUAL(layout.name(), "pcap.packet");
  MESSAGE("write out read packets");
  auto file = "vast-unit-test-workshop-2011-browse.pcap";
  format::pcap::writer writer{file};
  auto deleter = caf::detail::make_scope_guard([&] { rm(file); });
  REQUIRE_EQUAL(writer.write(slice), caf::none);
}

FIXTURE_SCOPE_END()
