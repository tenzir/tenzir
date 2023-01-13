//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE format

#include "vast/format/json.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/json.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture : public fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(zeek_reader_tests, fixture)

TEST(json to data) {
  auto schema = type{
    "schema",
    record_type{
      {"b", bool_type{}},
      {"c", uint64_type{}},
      {"r", double_type{}},
      {"i", int64_type{}},
      {"s", string_type{}},
      {"snum", string_type{}},
      {"a", ip_type{}},
      {"sn", subnet_type{}},
      {"t", time_type{}},
      {"d", duration_type{}},
      {"d2", duration_type{}},
      {"e", enumeration_type{{"FOO"}, {"BAR"}, {"BAZ"}}},
      {"lc", list_type{uint64_type{}}},
      {"lt", list_type{time_type{}}},
      {"rec", record_type{{"c", uint64_type{}}, {"s", string_type{}}}},
      {"msa", map_type{string_type{}, ip_type{}}},
      {"mcs", map_type{uint64_type{}, string_type{}}},
    },
  };
  auto builder = std::make_shared<table_slice_builder>(schema);
  std::string_view str = R"json({
    "b": true,
    "c": 424242,
    "r": 4.2,
    "i": -1337,
    "s": "0123456789®\r\n",
    "snum": 42.42,
    "a": "147.32.84.165",
    "sn": "192.168.0.1/24",
    "t": "2011-08-12+14:59:11.994970",
    "d": "42s",
    "d2": 3.006088,
    "e": "BAZ",
    "lc": [ "0x3e7", 19, 5555, 0 ],
    "lt": [ 1556624773, "2019-04-30T11:46:13Z" ],
    "rec": { "c": 421, "s":"test" },
    "msa": { "foo": "1.2.3.4", "bar": "2001:db8::" },
    "mcs": { "1": "FOO", "1024": "BAR!" }
  })json";
  ::simdjson::dom::parser p;
  auto el = p.parse(str);
  CHECK(el.error() == ::simdjson::error_code::SUCCESS);
  auto obj = el.value().get_object();
  CHECK(obj.error() == ::simdjson::error_code::SUCCESS);
  REQUIRE_EQUAL(vast::format::json::add(obj.value(), *builder), caf::none);
  auto slice = builder->finish();
  REQUIRE_NOT_EQUAL(slice.encoding(), table_slice_encoding::none);
  CHECK(slice.at(0, 0) == data{true});
  CHECK(slice.at(0, 1) == data{uint64_t{424242}});
  CHECK(slice.at(0, 2).is<double>());
  const auto r = slice.at(0, 2).get_data().get(
    std::integral_constant<
      int, caf::detail::tl_index_of<data::types, double>::value>());
  CHECK(std::abs(r - double{4.2}) < 0.000001);
  CHECK_EQUAL(materialize(slice.at(0, 3)), int64_t{-1337});
  CHECK_EQUAL(materialize(slice.at(0, 4)), "0123456789®\r\n");
  CHECK_EQUAL(materialize(slice.at(0, 5)), "42.42");
  std::array<std::uint8_t, 4> addr1{147, 32, 84, 165};
  CHECK(slice.at(0, 6) == data{ip::v4(std::span{addr1})});
  std::array<std::uint8_t, 4> addr2{192, 168, 0, 1};
  CHECK((slice.at(0, 7) == data{subnet{ip::v4(std::span{addr2}), 24}}));
  CHECK(slice.at(0, 11) == data{enumeration{2}});
  const list lc = {data{uint64_t{0x3e7}}, data{uint64_t{19}},
                   data{uint64_t{5555}}, data{uint64_t{0}}};
  CHECK(slice.at(0, 12) == data{lc});
  CHECK(slice.at(0, 14) == data{uint64_t{421}});
  CHECK(slice.at(0, 15) == data{std::string{"test"}});
  auto reference = map{};
  reference[uint64_t{1}] = data{"FOO"};
  reference[uint64_t{1024}] = data{"BAR!"};
  CHECK_EQUAL(materialize(slice.at(0, 17)), data{reference});
}

TEST(json hex number parser) {
  using namespace parsers;
  double x;
  CHECK(json_number("123.0", x));
  CHECK_EQUAL(x, 123.0);
  CHECK(json_number("-123.0", x));
  CHECK_EQUAL(x, -123.0);
  CHECK(json_number("123", x));
  CHECK_EQUAL(x, 123.0);
  // JSON does not allow `+` before a number.
  // https://datatracker.ietf.org/doc/html/rfc7159#section-6
  // CHECK(json_number("+123", x));
  // CHECK_EQUAL(x, 123.0);
  CHECK(json_number("0xFF", x));
  CHECK_EQUAL(x, 255.0);
}

FIXTURE_SCOPE_END()
