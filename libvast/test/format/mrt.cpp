#include "vast/filesystem.hpp"

#include "vast/detail/make_io_stream.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/format/mrt.hpp"

#define SUITE format
#include "vast/test/test.hpp"
#include "vast/test/data.hpp"

using namespace vast;

TEST_DISABLED(MRT) {
  auto in = detail::make_input_stream(mrt::updates20150505, false);
  format::mrt::reader reader{std::move(*in)};
  auto result = expected<event>{no_error};
  std::vector<event> events;
  while (result || !result.error()) {
    result = reader.read();
  if (result)
    events.push_back(std::move(*result));
  }
  REQUIRE(!result);
  CHECK_EQUAL(result.error(), ec::end_of_input);
  REQUIRE(!events.empty());
  CHECK_EQUAL(events.size(), 26479u);
  REQUIRE(events.size() == 26479u);
  // Event 2
  CHECK_EQUAL(events[2].type().name(), "mrt::bgp4mp::update::announcement");
  auto record = caf::get_if<vector>(&events[2].data());
  REQUIRE(record);
  auto addr = caf::get_if<address>(&record->at(0));
  CHECK_EQUAL(*addr, *to<address>("12.0.1.63"));
  CHECK_EQUAL(record->at(1), count{7018});
  auto subn = caf::get_if<subnet>(&record->at(2));
  CHECK_EQUAL(*subn, *to<subnet>("200.29.24.0/24"));
  // Event 17
  CHECK_EQUAL(events[17].type().name(), "mrt::bgp4mp::update::withdrawn");
  record = caf::get_if<vector>(&events[17].data());
  REQUIRE(record);
  addr = caf::get_if<address>(&record->at(0));
  CHECK_EQUAL(*addr, *to<address>("12.0.1.63"));
  CHECK_EQUAL(record->at(1), count{7018});
  subn = caf::get_if<subnet>(&record->at(2));
  CHECK_EQUAL(*subn, *to<subnet>("200.29.24.0/24"));
  // Event 73
  CHECK_EQUAL(events[73].type().name(), "mrt::bgp4mp::state_change");
  record = caf::get_if<vector>(&events[73].data());
  REQUIRE(record);
  addr = caf::get_if<address>(&record->at(0));
  CHECK_EQUAL(*addr, *to<address>("111.91.233.1"));
  CHECK_EQUAL(record->at(1), count{45896});
  CHECK_EQUAL(record->at(2), count{3});
  CHECK_EQUAL(record->at(3), count{2});
  // Event 26478
  CHECK_EQUAL(events[26478].type().name(), "mrt::bgp4mp::update::announcement");
  record = caf::get_if<vector>(&events[26478].data());
  REQUIRE(record);
  addr = caf::get_if<address>(&record->at(0));
  CHECK_EQUAL(*addr, *to<address>("2a01:2a8::3"));
  subn = caf::get_if<subnet>(&record->at(2));
  CHECK_EQUAL(*subn, *to<subnet>("2a00:bdc0:e003::/48"));
  auto as_path = caf::get_if<vector>(&record->at(3));
  CHECK_EQUAL(as_path->size(), 4u);
  CHECK_EQUAL(as_path->at(0), count{1836});
  CHECK_EQUAL(as_path->at(1), count{6939});
  CHECK_EQUAL(as_path->at(2), count{47541});
  CHECK_EQUAL(as_path->at(3), count{28709});
}
