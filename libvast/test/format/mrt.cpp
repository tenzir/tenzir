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

#include "vast/format/mrt.hpp"

#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/filesystem.hpp"
#include "vast/to_events.hpp"

using namespace vast;

// Technically, we don't need the actor system. However, we do need to
// initialize the table slice builder factories which happens automatically in
// the actor system setup. Further, including this fixture gives us access to
// log files to hunt down bugs faster.
FIXTURE_SCOPE(mrt_tests, fixtures::deterministic_actor_system)

TEST(MRT) {
  auto in = detail::make_input_stream(mrt::updates20150505, false);
  format::mrt::reader reader{defaults::system::table_slice_type,
                             std::move(*in)};
  std::unordered_map<std::string, std::vector<event>> events;
  auto add_events = [&](const table_slice_ptr& slice) {
    auto& xs = events[slice->layout().name()];
    to_events(xs, *slice);
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     defaults::system::table_slice_size,
                                     add_events);
  CHECK_EQUAL(err, ec::end_of_input);
  // Verify number of individual events.
  REQUIRE_EQUAL(produced, 26479u);
  REQUIRE_EQUAL(events.size(), 3u);
  /// These numbers are calculated using:
  /// https://github.com/t2mune/mrtparse/blob/master/examples/print_all.py.
  /// The output was stored in plain.txt and then:
  /// - # state changes: grep STATE_CHANGE plain.txt | grep -v "Subtype" | wc
  /// - # withdrawals: grep "Withdrawn Routes: " plain.txt | wc
  /// - # announcements: grep "NLRI: " plain.txt | wc
  CHECK_EQUAL(events["mrt::bgp4mp::state_change"], 46u);
  CHECK_EQUAL(events["mrt::bgp4mp::update::withdrawn"], 2105);
  CHECK_EQUAL(events["mrt::bgp4mp::update::announcement"], 24328u);
  // Check announcement at index 2.
  auto& announcements = events["mrt::bgp4mp::update::announcement"];
  auto xs = unbox(caf::get_if<vector>(&announcements[2].data()));
  auto addr = unbox(caf::get_if<address>(&xs.at(0)));
  CHECK_EQUAL(addr, unbox(to<address>("12.0.1.63")));
  CHECK_EQUAL(xs.at(1), count{7018});
  auto sn = unbox(caf::get_if<subnet>(&xs.at(2)));
  CHECK_EQUAL(sn, unbox(to<subnet>("200.29.24.0/24")));
  // Check withdrawal at index 2.
  auto& withdrawals = events["mrt::bgp4mp::update::withdrawn"];
  xs = unbox(caf::get_if<vector>(&withdrawals[4].data()));
  addr = unbox(caf::get_if<address>(&xs.at(0)));
  CHECK_EQUAL(addr, unbox(to<address>("12.0.1.63")));
  CHECK_EQUAL(xs.at(1), count{7018});
  sn = unbox(caf::get_if<subnet>(&xs.at(2)));
  CHECK_EQUAL(sn, unbox(to<subnet>("200.29.24.0/24")));
  // Check state change at index 0.
  auto& state_changes = events["mrt::bgp4mp::state_change"];
  xs = unbox(caf::get_if<vector>(&state_changes[0].data()));
  addr = unbox(caf::get_if<address>(&xs.at(0)));
  CHECK_EQUAL(addr, unbox(to<address>("111.91.233.1")));
  CHECK_EQUAL(xs.at(1), count{45896});
  CHECK_EQUAL(xs.at(2), count{3});
  CHECK_EQUAL(xs.at(3), count{2});
}

FIXTURE_SCOPE_END()
