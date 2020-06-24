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

#include "vast/format/csv.hpp"

#define SUITE format

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include "vast/caf_table_slice_builder.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast.hpp"

#include <algorithm>

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture : fixtures::deterministic_actor_system {
  const record_type l0 = record_type{{"ts", time_type{}},
                                     {"addr", address_type{}},
                                     {"port", count_type{}}}
                           .name("l0");

  const record_type l1 = record_type{{"s", string_type{}},
                                     {"ptn", pattern_type{}},
                                     {"set", set_type{count_type{}}}}
                           .name("l1");

  const record_type l2 = record_type{{"b", bool_type{}},
                                     {"c", count_type{}},
                                     {"r", real_type{}},
                                     {"i", integer_type{}},
                                     {"s", string_type{}},
                                     {"a", address_type{}},
                                     {"p", port_type{}},
                                     {"sn", subnet_type{}},
                                     {"t", time_type{}},
                                     {"d", duration_type{}},
                                     {"d2", duration_type{}},
                                     {"e",
                                      enumeration_type{{"FOO", "BAR", "BAZ"}}},
                                     {"sc", set_type{count_type{}}},
                                     {"vp", vector_type{port_type{}}},
                                     {"vt", vector_type{time_type{}}},
                                     {"msa",
                                      map_type{string_type{}, address_type{}}},
                                     {"mcs",
                                      map_type{count_type{}, string_type{}}}}
                           .name("l2");

  schema s;

  fixture() {
    s.add(l0);
    s.add(l1);
    s.add(l2);
  }

  const caf::settings options = {};

  std::vector<table_slice_ptr> run(std::string_view data, size_t max_events,
                                   size_t max_slice_size) {
    using reader_type = format::csv::reader;
    auto in = std::make_unique<std::istringstream>(std::string{data});
    reader_type reader{defaults::import::table_slice_type, options,
                       std::move(in)};
    reader.schema(s);
    std::vector<table_slice_ptr> slices;
    auto add_slice = [&](table_slice_ptr ptr) {
      slices.emplace_back(std::move(ptr));
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
  REQUIRE_EQUAL(slices[0]->layout(), l0);
  CHECK(slices[1]->at(0, 0)
        == data{unbox(to<vast::time>("2011-08-12T14:59:11.994970Z"))});
  CHECK(slices[1]->at(1, 2) == data{count{1047}});
}

std::string_view l0_log1 = R"__(ts,addr,port
2011-08-12T13:00:36.349948Z,147.32.84.165,1027
2011-08-12T13:08:01.360925Z,147.32.84.165,
2011-08-12T13:08:01.360925Z,,1029
2011-08-12T13:09:35.498887Z,147.32.84.165,1029
2011-08-12T13:14:36.012344Z,147.32.84.165,1041
,147.32.84.165,1046
,147.32.84.165,
,,)__";

TEST(csv reader - empty fields) {
  auto slices = run(l0_log1, 8, 5);
  REQUIRE_EQUAL(slices[0]->layout(), l0);
  CHECK(slices[1]->at(0, 1) == data{unbox(to<address>("147.32.84.165"))});
  CHECK(slices[1]->at(1, 2) == data{caf::none});
}

std::string_view l1_log_string = R"__(s
hello
)__";

TEST(csv reader - string) {
  auto slices = run(l1_log_string, 1, 1);
  auto l1_string = record_type{{"s", string_type{}}}.name("l1");
  REQUIRE_EQUAL(slices[0]->layout(), l1_string);
  CHECK(slices[0]->at(0, 0) == data{"hello"});
}

std::string_view l1_log_pattern = R"__(ptn
hello
)__";

TEST(csv reader - pattern) {
  auto slices = run(l1_log_pattern, 1, 1);
  auto l1_pattern = record_type{{"ptn", pattern_type{}}}.name("l1");
  REQUIRE_EQUAL(slices[0]->layout(), l1_pattern);
  CHECK(slices[0]->at(0, 0) == data{pattern{"hello"}});
}

std::string_view l1_log0 = R"__(s,ptn,set
hello,world,{1,2}
Tom,appeared,{42,1337}
on,the,{42,1337}
sidewalk,with,{42,1337}
a,bucket,{42,1337}
of,whitewash,{42,1337}
and,a,{42,1337}
long-handled,brush,{42,1337}
He,surveyed the,{42,1337}
fence,and,{42,1337}
all,gladness,{42,1337}
left,him,{42,1337}
and ,a,{42,1337}
deep,melancholy,{42,1337}
settled,down,{42,1337}
upon,his,{42,1337}
spirit,Thirty,{42,1337}
yards,of,{42,1337}
board, fence,{42,1337}
nine,feet,{42,1337}
high,Life,{42,1337}
to,him,{42,1337}
seemed,hollow,{42,1337}
and,existence,{42,1337}
but,a,{42,1337}
burden,Sighing,{42,1337}
,,)__";

TEST(csv reader - layout with container) {
  auto slices = run(l1_log0, 20, 20);
  REQUIRE_EQUAL(slices[0]->layout(), l1);
  CHECK(slices[0]->at(10, 1) == data{pattern{"gladness"}});
  auto s = vast::set{};
  s.emplace(data{count{42}});
  s.emplace(data{count{1337}});
  CHECK(slices[0]->at(19, 2) == data{s});
}

std::string_view l1_log1 = R"__(s,ptn
hello,world
Tom,appeared
on,the
sidewalk,with
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

TEST(csv reader - sublayout construction) {
  auto l1_sub = record_type{{"s", string_type{}}, {"ptn", pattern_type{}}}.name(
    "l1");
  auto slices = run(l1_log1, 20, 20);
  REQUIRE_EQUAL(slices[0]->layout(), l1_sub);
  CHECK(slices[0]->at(10, 1) == data{pattern{"gladness"}});
}

std::string_view l2_log_msa = R"__(msa
{ foo=1.2.3.4, bar=2001:db8:: })__";

TEST(csv reader - map string->address) {
  auto slices = run(l2_log_msa, 1, 1);
  auto l2_msa = record_type{{"msa", map_type{string_type{}, address_type{}}}}
                  .name("l2");
  REQUIRE_EQUAL(slices[0]->layout(), l2_msa);
  auto m = vast::map{};
  m.emplace(data{"foo"}, unbox(to<address>("1.2.3.4")));
  m.emplace(data{"bar"}, unbox(to<address>("2001:db8::")));
  CHECK_EQUAL(materialize(slices[0]->at(0, 0)), data{m});
}

std::string_view l2_log_vp = R"__(vp
[5555/tcp, 0/icmp]
[])__";

TEST(csv reader - vector of port) {
  auto slices = run(l2_log_vp, 2, 100);
  auto l2_vp = record_type{{"vp", vector_type{port_type{}}}}.name("l2");
  REQUIRE_EQUAL(slices[0]->layout(), l2_vp);
  CHECK(
    slices[0]->at(0, 0)
    == data{vector{unbox(to<port>("5555/tcp")), unbox(to<port>("0/icmp"))}});
  CHECK(slices[0]->at(1, 0) == data{vector{}});
}

std::string_view l2_log_subnet = R"__(sn
1.2.3.4/20
2001:db8::/125)__";

TEST(csv reader - subnet) {
  auto slices = run(l2_log_subnet, 2, 2);
  auto l2_subnet = record_type{{"sn", subnet_type{}}}.name("l2");
  REQUIRE_EQUAL(slices[0]->layout(), l2_subnet);
  CHECK(slices[0]->at(0, 0) == data{unbox(to<subnet>("1.2.3.4/20"))});
  CHECK(slices[0]->at(1, 0) == data{unbox(to<subnet>("2001:db8::/125"))});
}

std::string_view l2_log_duration = R"__(d,d2
42s,5days)__";

TEST(csv reader - duration) {
  auto slices = run(l2_log_duration, 1, 1);
  auto l2_duration = record_type{{"d", duration_type{}},
                                 {"d2", duration_type{}}}
                       .name("l2");
  REQUIRE_EQUAL(slices[0]->layout(), l2_duration);
  CHECK(slices[0]->at(0, 0) == data{unbox(to<duration>("42s"))});
}

std::string_view l2_log_reord
  = R"__(msa, c, r, i, b,  a,  p, sn, d,  e,  t,  sc, vp, vt
{ foo=1.2.3.4, bar=2001:db8:: },424242,4.2,-1337,T,147.32.84.165,42/udp,192.168.0.1/24,42s,BAZ,2011-08-12+14:59:11.994970,{ 44, 42, 43 },[ 5555/tcp, 0/icmp ],[ 2019-04-30T11:46:13Z ])__";
// FIXME: Parsing maps in csv is broken, see ch12358.
//   = R"__(msa, c, r, i, b,  a,  p, sn, d,  e,  t,  sc, vp, vt, mcs
// { foo=1.2.3.4, bar=2001:db8::
// },424242,4.2,-1337,T,147.32.84.165,42/udp,192.168.0.1/24,42s,BAZ,2011-08-12+14:59:11.994970,{
// 44, 42, 43 },[ 5555/tcp, 0/icmp ],[ 2019-04-30T11:46:13Z ],{ 1=FOO, 1024=BAR!
// })__";

TEST(csv reader - reordered layout) {
  auto slices = run(l2_log_reord, 1, 1);
  auto l2_sub = record_type{{"msa", map_type{string_type{}, address_type{}}},
                            {"c", count_type{}},
                            {"r", real_type{}},
                            {"i", integer_type{}},
                            {"b", bool_type{}},
                            {"a", address_type{}},
                            {"p", port_type{}},
                            {"sn", subnet_type{}},
                            {"d", duration_type{}},
                            {"e", enumeration_type{{"FOO", "BAR", "BAZ"}}},
                            {"t", time_type{}},
                            {"sc", set_type{count_type{}}},
                            {"vp", vector_type{port_type{}}},
                            {"vt", vector_type{time_type{}}},
                            // FIXME: Parsing maps in csv is broken, see ch12358.
                            // {"mcs", map_type{count_type{}, string_type{}}}
                            }
                  .name("l2");
  REQUIRE_EQUAL(slices[0]->layout(), l2_sub);
  CHECK(slices[0]->at(0, 0)
        == data{map{{data{"foo"}, unbox(to<address>("1.2.3.4"))},
                    {data{"bar"}, unbox(to<address>("2001:db8::"))}}});
  CHECK(slices[0]->at(0, 1) == data{count{424242}});
  CHECK(slices[0]->at(0, 2) == data{real{4.2}});
  CHECK(slices[0]->at(0, 3) == data{integer{-1337}});
  CHECK(slices[0]->at(0, 4) == data{true});
  CHECK(slices[0]->at(0, 5) == data{unbox(to<address>("147.32.84.165"))});
  CHECK(slices[0]->at(0, 6) == data{unbox(to<port>("42/udp"))});
  CHECK(slices[0]->at(0, 7) == data{unbox(to<subnet>("192.168.0.1/24"))});
  CHECK(slices[0]->at(0, 8) == data{unbox(to<duration>("42s"))});
  CHECK(slices[0]->at(0, 9) == data{enumeration{2}});
  CHECK(slices[0]->at(0, 10)
        == data{unbox(to<vast::time>("2011-08-12+14:59:11.994970"))});
  auto s = set{};
  s.emplace(count{44});
  s.emplace(count{42});
  s.emplace(count{43});
  CHECK_EQUAL(materialize(slices[0]->at(0, 11)), data{s});
  CHECK(
    slices[0]->at(0, 12)
    == data{vector{unbox(to<port>("5555/tcp")), unbox(to<port>("0/icmp"))}});
  CHECK(slices[0]->at(0, 13)
        == data{vector{unbox(to<vast::time>("2019-04-30T11:46:13Z"))}});
  auto m = map{};
  m[1u] = data{"FOO"};
  m[1024u] = data{"BAR!"};
  // FIXME: Parsing maps in csv is broken, see ch12358.
  // CHECK_EQUAL(materialize(slices[0]->at(0, 14)), data{m});
}

std::string_view l2_line_endings = "d,d2\r\n42s,5days\n10s,1days\r\n";

TEST(csv reader - line endings) {
  auto slices = run(l2_line_endings, 2, 2);
  auto l2_duration
    = record_type{{"d", duration_type{}}, {"d2", duration_type{}}}.name("l2");
  REQUIRE_EQUAL(slices[0]->layout(), l2_duration);
  CHECK(slices[0]->at(0, 0) == data{unbox(to<duration>("42s"))});
  CHECK(slices[0]->at(0, 1) == data{unbox(to<duration>("5days"))});
  CHECK(slices[0]->at(1, 0) == data{unbox(to<duration>("10s"))});
  CHECK(slices[0]->at(1, 1) == data{unbox(to<duration>("1days"))});
}

FIXTURE_SCOPE_END()
