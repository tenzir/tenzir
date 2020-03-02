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
#include "vast/format/syslog.hpp"

#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

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
FIXTURE_SCOPE(syslog_tests, fixtures::deterministic_actor_system)
TEST(syslog reader) {
  auto in
    = detail::make_input_stream(artifacts::logs::syslog::syslog_msgs, false);
  format::syslog::reader reader{defaults::system::table_slice_type,
                                caf::settings{}, std::move(*in)};
  table_slice_ptr slice;
  auto add_slice = [&](const table_slice_ptr& x) {
    REQUIRE_EQUAL(slice, nullptr);
    slice = x;
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     100, // we expect only 5 events
                                     add_slice);
  REQUIRE_NOT_EQUAL(slice, nullptr);
  REQUIRE_EQUAL(produced, 5u);
  CHECK_EQUAL(slice->layout().name(), "syslog.rfc5424");
}

TEST(syslog header parser) {
  format::syslog::header attr;
  auto p = format::syslog::header_parser{};
  CHECK(
    p("<34>1 2003-10-11T22:14:15.003Z mymachineexamplecom su asd ID47", attr));
  CHECK_EQUAL(attr.facility, 4);
  CHECK_EQUAL(attr.severity, 2);
  CHECK_EQUAL(attr.version, 1);
  CHECK_EQUAL(attr.hostname, "mymachineexamplecom");
  CHECK_EQUAL(attr.app_name, "su");
  CHECK_EQUAL(attr.process_id, "asd");
  CHECK_EQUAL(attr.msg_id, "ID47");
  CHECK(p("<34>1 2003-10-11T22:14:15.003Z - su asd ID47", attr));
  CHECK_EQUAL(attr.hostname, "");
}

TEST(syslog structured data element parser) {
  format::syslog::structured_data_element attr;
  auto p = format::syslog::structured_data_element_parser{};
  CHECK(p("[exampleSDID@32473 iut=\"3\" eventSource=\"App\\]lication\" "
          "eventID=\"1011\"]",
          attr));
  auto sd_id = std::get<0>(attr);
  CHECK_EQUAL(sd_id, "exampleSDID@32473");
}

TEST(syslog parameters parser) {
  format::syslog::parameter attr;
  auto p = format::syslog::parameter_parser{};
  CHECK(p(" iut=\"3\"", attr));
  CHECK_EQUAL(std::get<0>(attr), "iut");
  CHECK_EQUAL(std::get<1>(attr), "3");
}
FIXTURE_SCOPE_END()
