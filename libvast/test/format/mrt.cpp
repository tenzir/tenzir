#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"

#include "vast/format/mrt.hpp"

#define SUITE format
#include "test.hpp"
#include "data.hpp"

using namespace vast;

TEST(MRT) {
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
  CHECK(result.error() == ec::end_of_input);
  REQUIRE(!events.empty());
  REQUIRE(events.size() == 26543);
  // Event 0
  CHECK(events[0].type().name() == "mrt::bgp4mp::announcement");
  auto record = get_if<vector>(events[0].data());
  REQUIRE(record);
  auto addr = get_if<address>(record->at(1));
  CHECK(*addr == *to<address>("12.0.1.63"));
  CHECK(record->at(2) == count{7018});
  auto subn = get_if<subnet>(record->at(3));
  CHECK(*subn == *to<subnet>("200.29.24.0/24"));
  auto as_path = get_if<vector>(record->at(4));
  CHECK(as_path->size() == 3);
  CHECK(as_path->at(0) == count{7018});
  CHECK(as_path->at(1) == count{6762});
  CHECK(as_path->at(2) == count{14318});
  // Event 13
  CHECK(events[13].type().name() == "mrt::bgp4mp::withdrawn");
  record = get_if<vector>(events[13].data());
  REQUIRE(record);
  addr = get_if<address>(record->at(1));
  CHECK(*addr == *to<address>("12.0.1.63"));
  CHECK(record->at(2) == count{7018});
  subn = get_if<subnet>(record->at(3));
  CHECK(*subn == *to<subnet>("200.29.24.0/24"));
  // Event 73
  CHECK(events[73].type().name() == "mrt::bgp4mp::state_change");
  record = get_if<vector>(events[73].data());
  REQUIRE(record);
  addr = get_if<address>(record->at(1));
  CHECK(*addr == *to<address>("111.91.233.1"));
  CHECK(record->at(2) == count{45896});
  CHECK(record->at(3) == count{3});
  CHECK(record->at(4) == count{2});
  // Event 26542
  CHECK(events[26542].type().name() == "mrt::bgp4mp::announcement");
  record = get_if<vector>(events[26542].data());
  REQUIRE(record);
  addr = get_if<address>(record->at(1));
  CHECK(*addr == *to<address>("2a01:2a8::3"));
  subn = get_if<subnet>(record->at(3));
  CHECK(*subn == *to<subnet>("2a00:bdc0:e003::/48"));
}
