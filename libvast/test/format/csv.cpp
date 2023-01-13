//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/csv.hpp"

#define SUITE format

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <algorithm>

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture : fixtures::deterministic_actor_system {
  const type l0 = type{
    "l0",
    record_type{
      {"ts", time_type{}},
      {"addr", address_type{}},
      {"port", uint64_type{}},
    },
  };

  const type l1 = type{
    "l1",
    record_type{
      {"s", string_type{}},
      {"ptn", pattern_type{}},
      {"lis", list_type{uint64_type{}}},
    },
  };

  const type l2 = type{
    "l2",
    record_type{
      {"b", bool_type{}},
      {"c", uint64_type{}},
      {"r", double_type{}},
      {"i", int64_type{}},
      {"s", string_type{}},
      {"a", address_type{}},
      {"sn", subnet_type{}},
      {"t", time_type{}},
      {"d", duration_type{}},
      {"d2", duration_type{}},
      {"e", enumeration_type{{{"FOO"}, {"BAR"}, {"BAZ"}}}},
      {"lc", list_type{uint64_type{}}},
      {"lt", list_type{time_type{}}},
      {"r2", double_type{}},
      {"msa", map_type{string_type{}, address_type{}}},
      {"mcs", map_type{uint64_type{}, string_type{}}},
    },
  };

  const type l3 = type{
    "l3",
    record_type{
      {"s1", string_type{}},
      {"s2", string_type{}},
      {"s2,3", string_type{}},
    },
  };

  module m;

  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
    m.add(l0);
    m.add(l1);
    m.add(l2);
    m.add(l3);
  }

  const caf::settings options = {};

  std::vector<table_slice>
  run(std::string_view data, size_t max_events, size_t max_slice_size) {
    auto in = std::make_unique<std::istringstream>(std::string{data});
    format::csv::reader reader{options, std::move(in)};
    CHECK(!reader.module(m));
    std::vector<table_slice> slices;
    auto add_slice = [&](table_slice slice) {
      slices.emplace_back(std::move(slice));
    };
    auto [err, num] = reader.read(max_events, max_slice_size, add_slice);
    REQUIRE_EQUAL(err, caf::none);
    size_t lines = std::count(data.begin(), data.end(), '\n');
    REQUIRE_EQUAL(num, std::min(lines, max_events));
    return slices;
  }
};

} // namespace

FIXTURE_SCOPE(csv_reader_tests, fixture)

std::string_view l0_log0 = R"__(ts,addr,port
2011-08-12T13:00:36.349948Z,147.32.84.165,1027
2011-08-12T13:08:01.360925Z,147.32.84.165,3101
2011-08-12T13:08:01.360925Z,147.32.84.165,1029
2011-08-12T13:09:35.498887Z,147.32.84.165,1029
2011-08-12T13:14:36.012344Z,147.32.84.165,1041
2011-08-12T14:59:11.994970Z,147.32.84.165,1046
2011-08-12T14:59:12.448311Z,147.32.84.165,1047
2011-08-13T13:04:24.640406Z,147.32.84.165,1089)__";

TEST(csv reader - simple) {
  auto slices = run(l0_log0, 8, 5);
  REQUIRE_EQUAL(slices[0].schema(), l0);
  CHECK(slices[1].at(0, 0)
        == data{unbox(to<vast::time>("2011-08-12T14:59:11.994970Z"))});
  CHECK(slices[1].at(1, 2) == data{count{1047}});
}

std::string_view l0_log1 = R"__(ts,addr,port
2011-08-12T13:00:36.349948Z,"147.32.84.165",1027
"2011-08-12T13:08:01.360925Z",147.32.84.165,
2011-08-12T13:08:01.360925Z,,"1029"
2011-08-12T13:09:35.498887Z,147.32.84.165,1029
2011-08-12T13:14:36.012344Z,147.32.84.165,1041
,147.32.84.165,1046
,147.32.84.165,
,,)__";

TEST(csv reader - empty fields) {
  auto slices = run(l0_log1, 8, 5);
  REQUIRE_EQUAL(slices[0].schema(), l0);
  CHECK(slices[1].at(0, 1, address_type{})
        == data{unbox(to<address>("147.32.84.165"))});
  CHECK(slices[1].at(1, 2, uint64_type{}) == std::nullopt);
}

std::string_view l1_log_string = R"__(s
hello
)__";

TEST(csv reader - string) {
  auto slices = run(l1_log_string, 1, 1);
  auto l1_string = type{"l1", record_type{{"s", string_type{}}}};
  REQUIRE_EQUAL(slices[0].schema(), l1_string);
  CHECK(slices[0].at(0, 0) == data{"hello"});
}

std::string_view l1_log_pattern = R"__(ptn
hello
)__";

TEST(csv reader - pattern) {
  auto slices = run(l1_log_pattern, 1, 1);
  auto l1_pattern = type{"l1", record_type{{"ptn", pattern_type{}}}};
  REQUIRE_EQUAL(slices[0].schema(), l1_pattern);
  CHECK(slices[0].at(0, 0) == data{pattern{"hello"}});
}

std::string_view l1_log0 = R"__(s,ptn,lis
hello,world,[1,2]
Tom,appeared,[42,1337]
on,the,[42,1337]
sidewalk,with,[42,1337]
a,bucket,[42,1337]
of,whitewash,[42,1337]
and,a,[42,1337]
long-handled,brush,[42,1337]
He,surveyed the,[42,1337]
fence,and,[42,1337]
all,gladness,[42,1337]
left,him,[42,1337]
and ,a,[42,1337]
deep,melancholy,[42,1337]
settled,down,[42,1337]
upon,his,[42,1337]
spirit,Thirty,[42,1337]
yards,of,[42,1337]
board, fence,[42,1337]
nine,feet,[42,1337]
high,Life,[42,1337]
to,him,[42,1337]
seemed,hollow,[42,1337]
and,existence,[42,1337]
but,a,[42,1337]
burden,Sighing,[42,1337]
,,)__";

TEST(csv reader - schema with container) {
  auto slices = run(l1_log0, 20, 20);
  REQUIRE_EQUAL(slices[0].schema(), l1);
  CHECK(slices[0].at(10, 1) == data{pattern{"gladness"}});
  auto xs = vast::list{};
  xs.emplace_back(data{count{42}});
  xs.emplace_back(data{count{1337}});
  CHECK(slices[0].at(19, 2, list_type{uint64_type{}}) == make_view(xs));
}

std::string_view l1_log1 = R"__(s,ptn
hello,world
Tom,appeared
"on",the
sidewalk,"with"
a,bucket
of,whitewash
and,a
long-handled,brush
He,surveyed the
fence,and
all,gladness
left,him
and ,a
deep,melancholy
settled,down
upon,his
spirit,Thirty
yards,of
board, fence
nine,feet
high,Life
to,him
seemed,hollow
and,existence
but,a
burden,Sighing
,,)__";

TEST(csv reader - subschema construction) {
  auto l1_sub = type{
    "l1",
    record_type{
      {"s", string_type{}},
      {"ptn", pattern_type{}},
    },
  };
  auto slices = run(l1_log1, 20, 20);
  REQUIRE_EQUAL(slices[0].schema(), l1_sub);
  CHECK(slices[0].at(10, 1) == data{pattern{"gladness"}});
}

std::string_view l2_log_msa = R"__(msa
{ foo=1.2.3.4, bar=2001:db8:: })__";

TEST(csv reader - map string->address) {
  auto slices = run(l2_log_msa, 1, 1);
  auto t = map_type{string_type{}, address_type{}};
  auto l2_msa = type{"l2", record_type{{"msa", t}}};
  REQUIRE_EQUAL(slices[0].schema(), l2_msa);
  auto m = vast::map{};
  m.emplace(data{"foo"}, unbox(to<address>("1.2.3.4")));
  m.emplace(data{"bar"}, unbox(to<address>("2001:db8::")));
  CHECK_EQUAL(materialize(slices[0].at(0, 0)), data{m});
}

std::string_view l2_log_vp = R"__(lc
[1, 2, 3, 4, 5]
[])__";

TEST(csv reader - list of count) {
  auto slices = run(l2_log_vp, 2, 100);
  auto t = type{list_type{uint64_type{}}};
  auto l2_vp = type{"l2", record_type{{"lc", t}}};
  REQUIRE_EQUAL(slices[0].schema(), l2_vp);
  CHECK((slices[0].at(0, 0, t) == data{list{1u, 2u, 3u, 4u, 5u}}));
  CHECK(slices[0].at(1, 0, t) == data{list{}});
}

std::string_view l2_log_subnet = R"__(sn
"1.2.3.4/20"
2001:db8::/125)__";

TEST(csv reader - subnet) {
  auto slices = run(l2_log_subnet, 2, 2);
  auto l2_subnet = type{"l2", record_type{{"sn", subnet_type{}}}};
  REQUIRE_EQUAL(slices[0].schema(), l2_subnet);
  CHECK(slices[0].at(0, 0) == data{unbox(to<subnet>("1.2.3.4/20"))});
  CHECK(slices[0].at(1, 0) == data{unbox(to<subnet>("2001:db8::/125"))});
}

std::string_view l2_log_duration = R"__(d,d2
"42s",5days)__";

TEST(csv reader - duration) {
  auto slices = run(l2_log_duration, 1, 1);
  auto l2_duration = type{
    "l2",
    record_type{
      {"d", duration_type{}},
      {"d2", duration_type{}},
    },
  };
  REQUIRE_EQUAL(slices[0].schema(), l2_duration);
  CHECK(slices[0].at(0, 0, duration_type{})
        == data{unbox(to<duration>("42s"))});
}

std::string_view l2_log_reord
  = R"__(msa, c, r, i, b,  a,  sn, d,  e,  t, lc, lt, r2
{ foo=1.2.3.4, bar=2001:db8:: },424242,4.2,-1337,T,147.32.84.165,192.168.0.1/24,42s,BAZ,2011-08-12+14:59:11.994970,[ 5555,0],[ 2019-04-30T11:46:13Z ],3)__";
// FIXME: Parsing maps in csv is broken, see ch12358.
//   = R"__(msa, c, r, i, b,  a,  sn, d,  e,  t,  lc, lt, mcs
// { foo=1.2.3.4, bar=2001:db8::
// },424242,4.2,-1337,T,147.32.84.165,42/udp,192.168.0.1/24,42s,BAZ,2011-08-12+14:59:11.994970,
// [ 5555/tcp, 0/icmp ],[ 2019-04-30T11:46:13Z ],{ 1=FOO, 1024=BAR! })__";

TEST(csv reader - reordered schema) {
  auto slices = run(l2_log_reord, 1, 1);
  auto l2_sub = type{
    "l2",
    record_type{
      {"msa", map_type{string_type{}, address_type{}}},
      {"c", uint64_type{}},
      {"r", double_type{}},
      {"i", int64_type{}},
      {"b", bool_type{}},
      {"a", address_type{}},
      {"sn", subnet_type{}},
      {"d", duration_type{}},
      {"e", enumeration_type{{{"FOO"}, {"BAR"}, {"BAZ"}}}},
      {"t", time_type{}},
      {"lc", list_type{uint64_type{}}},
      {"lt", list_type{time_type{}}},
      {"r2", double_type{}},
      // FIXME: Parsing maps in csv is broken, see ch12358.
      // {"mcs", map_type{uint64_type{}, string_type{}}}
    },
  };
  REQUIRE_EQUAL(slices[0].schema(), l2_sub);
  CHECK((slices[0].at(0, 0)
         == data{map{{data{"foo"}, unbox(to<address>("1.2.3.4"))},
                     {data{"bar"}, unbox(to<address>("2001:db8::"))}}}));
  CHECK(slices[0].at(0, 1) == data{count{424242}});
  CHECK(slices[0].at(0, 2) == data{real{4.2}});
  CHECK(slices[0].at(0, 3) == data{integer{-1337}});
  CHECK(slices[0].at(0, 4) == data{true});
  CHECK(slices[0].at(0, 5) == data{unbox(to<address>("147.32.84.165"))});
  CHECK(slices[0].at(0, 6) == data{unbox(to<subnet>("192.168.0.1/24"))});
  CHECK(slices[0].at(0, 7) == data{unbox(to<duration>("42s"))});
  CHECK(slices[0].at(0, 8) == data{enumeration{2}});
  CHECK(slices[0].at(0, 9)
        == data{unbox(to<vast::time>("2011-08-12+14:59:11.994970"))});
  CHECK((slices[0].at(0, 10) == data{list{5555u, 0u}}));
  CHECK(slices[0].at(0, 11)
        == data{list{unbox(to<vast::time>("2019-04-30T11:46:13Z"))}});
  CHECK(slices[0].at(0, 12) == data{real{3.}});
  // FIXME: Parsing maps in csv is broken, see ch12358.
  // auto m = map{};
  // m[1u] = data{"FOO"};
  // m[1024u] = data{"BAR!"};
  // CHECK_EQUAL(materialize(slices[0].at(0, 13)), data{m});
}

std::string_view l2_line_endings = "d,d2\r\n42s,5days\n10s,1days\r\n";

TEST(csv reader - line endings) {
  auto slices = run(l2_line_endings, 2, 2);
  auto l2_duration = type{
    "l2",
    record_type{
      {"d", duration_type{}},
      {"d2", duration_type{}},
    },
  };
  REQUIRE_EQUAL(slices[0].schema(), l2_duration);
  CHECK(slices[0].at(0, 0) == data{unbox(to<duration>("42s"))});
  CHECK(slices[0].at(0, 1) == data{unbox(to<duration>("5days"))});
  CHECK(slices[0].at(1, 0) == data{unbox(to<duration>("10s"))});
  CHECK(slices[0].at(1, 1) == data{unbox(to<duration>("1days"))});
}

// Below are strings that extensively test quoting and escaping for string
// fields and column names. For other field types, other tests above have
// quoted field sprinkled all over them.

std::string_view l3_quoted_strings_header = R"__(s1,"s2,3"
a,b
c,d)__";

TEST(csv reader - quoted strings in header) {
  auto slices = run(l3_quoted_strings_header, 2, 2);
  auto l3_strings = type{
    "l3",
    record_type{
      {"s1", string_type{}},
      {"s2,3", string_type{}},
    },
  };
  REQUIRE_EQUAL(slices[0].schema(), l3_strings);
  CHECK(slices[0].at(0, 0) == data{"a"});
  CHECK(slices[0].at(0, 1) == data{"b"});
  CHECK(slices[0].at(1, 0) == data{"c"});
  CHECK(slices[0].at(1, 1) == data{"d"});
}

std::string_view l3_quoted_strings_1 = R"__(s1
"hello, world")__";

std::string_view l3_quoted_strings_2 = R"__(s1,s2
a,"b,c"
"d,e,\"f",\"g)__";

TEST(csv reader - quoted string) {
  {
    auto slices = run(l3_quoted_strings_1, 1, 1);
    auto l3_strings = type{
      "l3",
      record_type{
        {"s1", string_type{}},
      },
    };
    REQUIRE_EQUAL(slices[0].schema(), l3_strings);
    CHECK(slices[0].at(0, 0) == data{"hello, world"});
  }
  {
    auto slices = run(l3_quoted_strings_2, 2, 2);
    auto l3_strings = type{
      "l3",
      record_type{
        {"s1", string_type{}},
        {"s2", string_type{}},
      },
    };
    REQUIRE_EQUAL(slices[0].schema(), l3_strings);
    CHECK(slices[0].at(0, 0) == data{"a"});
    CHECK(slices[0].at(0, 1) == data{"b,c"});
    CHECK(slices[0].at(1, 0) == data{"d,e,\"f"});
    CHECK(slices[0].at(1, 1) == data{"\\\"g"});
  }
}

FIXTURE_SCOPE_END()
