//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/syslog.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

using namespace vast;

namespace {

// Technically, we don't need the actor system. However, we do need to
// initialize the table slice builder factories which happens automatically in
// the actor system setup. Further, including this fixture gives us access to
// log files to hunt down bugs faster.
struct fixture : public fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(syslog_tests, fixture)
TEST(syslog reader) {
  auto in = detail::make_input_stream(artifacts::logs::syslog::syslog_msgs);
  format::syslog::reader reader{caf::settings{}, std::move(*in)};
  table_slice slice;
  auto add_slice = [&](const table_slice& x) {
    REQUIRE_EQUAL(slice.encoding(), table_slice_encoding::none);
    slice = x;
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     100, // we expect only 5 events
                                     add_slice);
  REQUIRE_NOT_EQUAL(slice.encoding(), table_slice_encoding::none);
  REQUIRE_EQUAL(produced, 5u);
  auto&& schema = slice.schema();
  CHECK_EQUAL(schema.name(), "syslog.rfc5424");
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
