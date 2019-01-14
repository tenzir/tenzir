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

#define SUITE value_index

#include "vast/test/test.hpp"
#include "vast/test/fixtures/events.hpp"

#include "vast/value_index.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(value_index_tests, fixtures::events)

TEST(boolean) {
  arithmetic_index<boolean> idx;
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(true)));
  REQUIRE(idx.append(make_data_view(true)));
  REQUIRE(idx.append(make_data_view(false)));
  REQUIRE(idx.append(make_data_view(true)));
  REQUIRE(idx.append(make_data_view(false)));
  REQUIRE(idx.append(make_data_view(false)));
  REQUIRE(idx.append(make_data_view(false)));
  REQUIRE(idx.append(make_data_view(true)));
  MESSAGE("lookup");
  auto f = idx.lookup(equal, make_data_view(false));
  REQUIRE(f);
  CHECK_EQUAL(to_string(*f), "00101110");
  auto t = idx.lookup(not_equal, make_data_view(false));
  REQUIRE(t);
  CHECK_EQUAL(to_string(*t), "11010001");
  auto xs = set{true, false};
  auto multi = idx.lookup(in, make_data_view(xs));
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "11111111");
  MESSAGE("serialization");
  std::string buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  arithmetic_index<boolean> idx2;
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  t = idx2.lookup(equal, make_data_view(true));
  REQUIRE(t);
  CHECK_EQUAL(to_string(*t), "11010001");
}

TEST(integer) {
  arithmetic_index<integer> idx{base::uniform(10, 20)};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(-7)));
  REQUIRE(idx.append(make_data_view(42)));
  REQUIRE(idx.append(make_data_view(10000)));
  REQUIRE(idx.append(make_data_view(4711)));
  REQUIRE(idx.append(make_data_view(31337)));
  REQUIRE(idx.append(make_data_view(42)));
  REQUIRE(idx.append(make_data_view(42)));
  MESSAGE("lookup");
  auto leet = idx.lookup(equal, make_data_view(31337));
  REQUIRE(leet);
  CHECK(to_string(*leet) == "0000100");
  auto less_than_leet = idx.lookup(less, make_data_view(31337));
  REQUIRE(less_than_leet);
  CHECK(to_string(*less_than_leet) == "1111011");
  auto greater_zero = idx.lookup(greater, make_data_view(0));
  REQUIRE(greater_zero);
  CHECK(to_string(*greater_zero) == "0111111");
  auto xs = set{42, 10, 4711};
  auto multi = idx.lookup(in, make_data_view(xs));
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "0101011");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  auto idx2 = arithmetic_index<integer>{};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  less_than_leet = idx2.lookup(less, make_data_view(31337));
  REQUIRE(less_than_leet);
  CHECK(to_string(*less_than_leet) == "1111011");
}

TEST(floating-point with custom binner) {
  using index_type = arithmetic_index<real, precision_binner<6, 2>>;
  auto idx = index_type{base::uniform<64>(10)};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(-7.8)));
  REQUIRE(idx.append(make_data_view(42.123)));
  REQUIRE(idx.append(make_data_view(10000.0)));
  REQUIRE(idx.append(make_data_view(4711.13510)));
  REQUIRE(idx.append(make_data_view(31337.3131313)));
  REQUIRE(idx.append(make_data_view(42.12258)));
  REQUIRE(idx.append(make_data_view(42.125799)));
  MESSAGE("lookup");
  auto result = idx.lookup(less, make_data_view(100.0));
  CHECK_EQUAL(to_string(*result), "1100011");
  result = idx.lookup(less, make_data_view(43.0));
  CHECK_EQUAL(to_string(*result), "1100011");
  result = idx.lookup(greater_equal, make_data_view(42.0));
  CHECK_EQUAL(to_string(*result), "0111111");
  result = idx.lookup(not_equal, make_data_view(4711.14));
  CHECK_EQUAL(to_string(*result), "1110111");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  auto idx2 = index_type{};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  result = idx2.lookup(not_equal, make_data_view(4711.14));
  CHECK_EQUAL(to_string(*result), "1110111");
}

TEST(timespan) {
  using namespace std::chrono;
  // Default binning gives granularity of seconds.
  auto idx = arithmetic_index<timespan>{base::uniform<64>(10)};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(milliseconds(1000))));
  REQUIRE(idx.append(make_data_view(milliseconds(2000))));
  REQUIRE(idx.append(make_data_view(milliseconds(3000))));
  REQUIRE(idx.append(make_data_view(milliseconds(1011))));
  REQUIRE(idx.append(make_data_view(milliseconds(2222))));
  REQUIRE(idx.append(make_data_view(milliseconds(2322))));
  MESSAGE("lookup");
  auto hun = idx.lookup(equal, make_data_view(milliseconds(1034)));
  REQUIRE(hun);
  CHECK(to_string(*hun) == "100100");
  auto twokay = idx.lookup(less_equal, make_data_view(milliseconds(2000)));
  REQUIRE(twokay);
  CHECK(to_string(*twokay) == "110111");
  auto twelve = idx.lookup(greater, make_data_view(milliseconds(1200)));
  REQUIRE(twelve);
  CHECK(to_string(*twelve) == "011011");
}

TEST(timestamp) {
  arithmetic_index<timestamp> idx{base::uniform<64>(10)};
  auto t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(*t)));
  t = to<timestamp>("2014-01-16+05:30:12");
  REQUIRE(t);
  REQUIRE(idx.append(make_data_view(*t)));
  t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  REQUIRE(idx.append(make_data_view(*t)));
  t = to<timestamp>("2014-01-16+05:30:18");
  REQUIRE(t);
  REQUIRE(idx.append(make_data_view(*t)));
  t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  REQUIRE(idx.append(make_data_view(*t)));
  t = to<timestamp>("2014-01-16+05:30:19");
  REQUIRE(t);
  REQUIRE(idx.append(make_data_view(*t)));
  MESSAGE("lookup");
  t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  auto fifteen = idx.lookup(equal, make_data_view(*t));
  CHECK(to_string(*fifteen) == "101010");
  t = to<timestamp>("2014-01-16+05:30:20");
  REQUIRE(t);
  auto twenty = idx.lookup(less, make_data_view(*t));
  CHECK(to_string(*twenty) == "111111");
  t = to<timestamp>("2014-01-16+05:30:18");
  REQUIRE(t);
  auto eighteen = idx.lookup(greater_equal, make_data_view(*t));
  CHECK(to_string(*eighteen) == "000101");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  auto idx2 = decltype(idx){};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  eighteen = idx2.lookup(greater_equal, make_data_view(*t));
  CHECK(to_string(*eighteen) == "000101");
}

TEST(string) {
  string_index idx{100};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view("bar")));
  REQUIRE(idx.append(make_data_view("baz")));
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view("bar")));
  REQUIRE(idx.append(make_data_view("")));
  REQUIRE(idx.append(make_data_view("qux")));
  REQUIRE(idx.append(make_data_view("corge")));
  REQUIRE(idx.append(make_data_view("bazz")));
  MESSAGE("lookup");
  auto result = idx.lookup(equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(*result), "1001100000");
  result = idx.lookup(equal, make_data_view("bar"));
  CHECK_EQUAL(to_string(*result), "0100010000");
  result = idx.lookup(equal, make_data_view("baz"));
  CHECK_EQUAL(to_string(*result), "0010000000");
  result = idx.lookup(equal, make_data_view(""));
  CHECK_EQUAL(to_string(*result), "0000001000");
  result = idx.lookup(equal, make_data_view("qux"));
  CHECK_EQUAL(to_string(*result),   "0000000100");
  result = idx.lookup(equal, make_data_view("corge"));
  CHECK_EQUAL(to_string(*result), "0000000010");
  result = idx.lookup(equal, make_data_view("bazz"));
  CHECK_EQUAL(to_string(*result),  "0000000001");
  result = idx.lookup(not_equal, make_data_view(""));
  CHECK_EQUAL(to_string(*result),    "1111110111");
  result = idx.lookup(not_equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(*result), "0110011111");
  result = idx.lookup(not_ni, make_data_view(""));
  CHECK_EQUAL(to_string(*result), "0000000000");
  result = idx.lookup(ni, make_data_view(""));
  CHECK_EQUAL(to_string(*result),     "1111111111");
  result = idx.lookup(ni, make_data_view("o"));
  CHECK_EQUAL(to_string(*result),    "1001100010");
  result = idx.lookup(ni, make_data_view("oo"));
  CHECK_EQUAL(to_string(*result),   "1001100000");
  result = idx.lookup(ni, make_data_view("z"));
  CHECK_EQUAL(to_string(*result),    "0010000001");
  result = idx.lookup(ni, make_data_view("zz"));
  CHECK_EQUAL(to_string(*result),   "0000000001");
  result = idx.lookup(ni, make_data_view("ar"));
  CHECK_EQUAL(to_string(*result),   "0100010000");
  result = idx.lookup(ni, make_data_view("rge"));
  CHECK_EQUAL(to_string(*result),  "0000000010");
  result = idx.lookup(match, make_data_view("foo"));
  CHECK(!result);
  auto xs = set{"foo", "bar", "baz"};
  result = idx.lookup(in, make_data_view(xs));
  REQUIRE(result);
  CHECK_EQUAL(to_string(*result), "1111110000");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  string_index idx2{};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  result = idx2.lookup(equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(*result), "1001100000");
  result = idx2.lookup(equal, make_data_view("bar"));
  CHECK_EQUAL(to_string(*result), "0100010000");
}

TEST(address) {
  address_index idx;
  MESSAGE("append");
  auto x = *to<address>("192.168.0.1");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.2");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.3");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.1");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.1");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.2");
  REQUIRE(idx.append(make_data_view(x)));
  MESSAGE("address equality");
  x = *to<address>("192.168.0.1");
  auto bm = idx.lookup(equal, make_data_view(x));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "100110");
  bm = idx.lookup(not_equal, make_data_view(x));
  CHECK(to_string(*bm) == "011001");
  x = *to<address>("192.168.0.5");
  CHECK(to_string(*idx.lookup(equal, make_data_view(x))) == "000000");
  MESSAGE("invalid operator");
  CHECK(!idx.lookup(match, make_data_view(x)));
  MESSAGE("prefix membership");
  x = *to<address>("192.168.0.128");
  CHECK(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.130");
  CHECK(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.240");
  CHECK(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.127");
  CHECK(idx.append(make_data_view(x)));
  x = *to<address>("192.168.0.33");
  CHECK(idx.append(make_data_view(x)));
  auto y = subnet{*to<address>("192.168.0.128"), 25};
  bm = idx.lookup(in, make_data_view(y));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "00000011100");
  bm = idx.lookup(not_in, make_data_view(y));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111100011");
  y = {*to<address>("192.168.0.0"), 24};
  bm = idx.lookup(in, make_data_view(y));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111111111");
  y = {*to<address>("192.168.0.0"), 20};
  bm = idx.lookup(in, make_data_view(y));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111111111");
  y = {*to<address>("192.168.0.64"), 26};
  bm = idx.lookup(not_in, make_data_view(y));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111111101");
  auto xs = vector{*to<address>("192.168.0.1"), *to<address>("192.168.0.2")};
  auto multi = idx.lookup(in, make_data_view(xs));
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "11011100000");
  MESSAGE("gaps");
  x = *to<address>("192.168.0.2");
  CHECK(idx.append(make_data_view(x), 42));
  x = *to<address>("192.168.0.2");
  auto str = "01000100000"s + std::string('0', 42) + '1';
  CHECK_EQUAL(idx.lookup(equal, make_data_view(x)), str);
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  address_index idx2{};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  CHECK_EQUAL(idx2.lookup(equal, make_data_view(x)), str);
}

TEST(subnet) {
  subnet_index idx;
  auto s0 = *to<subnet>("192.168.0.0/24");
  auto s1 = *to<subnet>("192.168.1.0/24");
  auto s2 = *to<subnet>("::/40");
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s1)));
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s2)));
  REQUIRE(idx.append(make_data_view(s2)));
  MESSAGE("equality lookup");
  auto bm = idx.lookup(equal, make_data_view(s0));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101100");
  bm = idx.lookup(not_equal, make_data_view(s1));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101111");
  MESSAGE("subset lookup (in)");
  auto x = *to<subnet>("192.168.0.0/23");
  bm = idx.lookup(in, make_data_view(x));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "111100");
  x = *to<subnet>("192.168.0.0/25");
  bm = idx.lookup(in, make_data_view(x));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "000000");
  MESSAGE("subset lookup (ni)");
  bm = idx.lookup(ni, make_data_view(s0));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101100");
  x = *to<subnet>("192.168.1.128/25");
  bm = idx.lookup(ni, make_data_view(x));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "010000");
  x = *to<subnet>("192.168.0.254/32");
  bm = idx.lookup(ni, make_data_view(x));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101100");
  x = *to<subnet>("192.0.0.0/8");
  bm = idx.lookup(ni, make_data_view(x));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "000000");
  auto xs = vector{s0, s1};
  auto multi = idx.lookup(in, make_data_view(xs));
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "111100");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  subnet_index idx2;
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  bm = idx2.lookup(not_equal, make_data_view(s1));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101111");
}

TEST(port) {
  port_index idx;
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(port(80, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(443, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(53, port::udp))));
  REQUIRE(idx.append(make_data_view(port(8, port::icmp))));
  REQUIRE(idx.append(make_data_view(port(31337, port::unknown))));
  REQUIRE(idx.append(make_data_view(port(80, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(8080, port::tcp))));
  MESSAGE("lookup");
  port http{80, port::tcp};
  auto bm = idx.lookup(equal, make_data_view(http));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "1000010");
  port priv{1024, port::unknown};
  bm = idx.lookup(less_equal, make_data_view(priv));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "1111010");
  bm = idx.lookup(greater, make_data_view(port{2, port::unknown}));
  REQUIRE(bm);
  CHECK(to_string(*bm) == "1111111");
  auto xs = vector{http, port(53, port::udp)};
  auto multi = idx.lookup(in, make_data_view(xs));
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "1010010");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  port_index idx2;
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  bm = idx2.lookup(less_equal, make_data_view(priv));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "1111010");
}

TEST(container) {
  sequence_index idx{string_type{}};
  MESSAGE("append");
  vector xs{"foo", "bar"};
  REQUIRE(idx.append(make_data_view(xs)));
  xs = {"qux", "foo", "baz", "corge"};
  REQUIRE(idx.append(make_data_view(xs)));
  xs = {"bar"};
  REQUIRE(idx.append(make_data_view(xs)));
  REQUIRE(idx.append(make_data_view(xs)));
  REQUIRE(idx.append(make_data_view(xs), 7));
  MESSAGE("lookup");
  auto x = "foo"s;
  CHECK_EQUAL(to_string(*idx.lookup(ni, make_data_view(x))), "11000000");
  CHECK_EQUAL(to_string(*idx.lookup(not_ni, make_data_view(x))), "00110001");
  x = "bar";
  CHECK_EQUAL(to_string(*idx.lookup(ni, make_data_view(x))), "10110001");
  x = "not";
  CHECK_EQUAL(to_string(*idx.lookup(ni, make_data_view(x))), "00000000");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  sequence_index idx2;
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  x = "foo";
  CHECK_EQUAL(to_string(*idx2.lookup(ni, make_data_view(x))), "11000000");
}

TEST(polymorphic) {
  type t = set_type{integer_type{}}.attributes({{"max_size", "2"}});
  auto idx = value_index::make(t);
  REQUIRE(idx);
  auto xs = set{42, 43, 44};
  REQUIRE(idx->append(make_data_view(xs)));
  xs = set{1, 2, 3};
  REQUIRE(idx->append(make_data_view(xs)));
  xs = set{};
  REQUIRE(idx->append(make_data_view(xs)));
  xs = set{42};
  REQUIRE(idx->append(make_data_view(xs)));
  CHECK_EQUAL(to_string(*idx->lookup(ni, make_data_view(42))), "1001");
  MESSAGE("chopped off");
  CHECK_EQUAL(to_string(*idx->lookup(ni, make_data_view(44))), "0000");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, detail::value_index_inspect_helper{t, idx}),
              caf::none);
  std::unique_ptr<value_index> idx2;
  detail::value_index_inspect_helper helper{t, idx2};
  CHECK_EQUAL(load(nullptr, buf, helper), caf::none);
  REQUIRE(idx2);
  CHECK_EQUAL(to_string(*idx2->lookup(ni, make_data_view(42))), "1001");
  MESSAGE("attributes");
  t = integer_type{}.attributes({{"base", "uniform(2, 4)"}});
  idx = value_index::make(t);
  REQUIRE(idx);
  t = integer_type{}.attributes({{"base", "[2, 3,4]"}});
  idx = value_index::make(t);
  REQUIRE(idx);
  MESSAGE("nil");
  REQUIRE(idx->append(make_data_view(caf::none)));
}

// Attention
// =========
// !(x == 42) is no the same as x != 42 because nil values never participate in
// a lookup. This may seem counter-intuitive first, but the rationale
// comes from consistency. For x != 42 it may seem natural to include nil
// values because they are not 42, but for <, <=, >=, > it becomes less clear:
// should nil be less or great than any other value in the domain?
TEST(polymorphic none values) {
  auto idx = value_index::make(string_type{});
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view("bar")));
  REQUIRE(idx->append(make_data_view("bar")));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view("bar")));
  REQUIRE(idx->append(make_data_view("bar")));
  REQUIRE(idx->append(make_data_view("bar")));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view("foo")));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(caf::none)));
  auto bm = idx->lookup(equal, make_data_view("foo"));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "01100010000001110001100");
  // NB: not same as !(x == 42)
  bm = idx->lookup(not_equal, make_data_view("foo"));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "00000001100000001110000");
  bm = idx->lookup(equal, make_data_view(caf::none));
  CHECK_EQUAL(to_string(*bm), "10011100011110000000011");
  bm = idx->lookup(not_equal, make_data_view(caf::none));
  CHECK_EQUAL(to_string(*bm), "01100011100001111111100");
}

auto orig_h(const event& x) {
  auto& log_entry = caf::get<vector>(x.data());
  auto& conn_id = caf::get<vector>(log_entry[2]);
  return make_view(caf::get<address>(conn_id[0]));
}

// This test uncovered a regression that ocurred when computing the rank of a
// bitmap representing conn.log events. The culprit was the EWAH bitmap
// encoding, because swapping out ewah_bitmap for null_bitmap in address_index
// made the bug disappear.
TEST_DISABLED(regression - build an address index from bro events) {
  // Populate the index with data up to the critical point.
  address_index idx;
  for (auto i = 0; i < 6464; ++i) {
    auto& x = bro_conn_log[i];
    CHECK(idx.append(orig_h(x), x.id()));
  }
  // This is where we are in trouble: the last ID should be 720, but the bogus
  // test reports 6452.
  auto addr = *to<data>("169.254.225.22");
  auto before = idx.lookup(equal, make_data_view(addr));
  CHECK_EQUAL(rank(*before), 4u);
  CHECK_EQUAL(select(*before, -1), id{720});
  auto& x = bro_conn_log[6464];
  // After adding another event, the correct state is restored again and the
  // bug doesn't show up anymore.
  CHECK(idx.append(orig_h(x), x.id()));
  auto after = idx.lookup(equal, make_data_view(addr));
  CHECK_EQUAL(rank(*after), 4u);
  CHECK_EQUAL(select(*after, -1), id{720});
  CHECK_NOT_EQUAL(select(*after, -1), id{6452});
}

// This was the first attempt in figuring out where the bug sat. I didn't fire.
TEST_DISABLED(regression - checking the result single bitmap) {
  ewah_bitmap bm;
  bm.append<0>(680);
  bm.append<1>();     //  681
  bm.append<0>();     //  682
  bm.append<1>();     //  683
  bm.append<0>(36);   //  719
  bm.append<1>();     //  720
  bm.append<1>();     //  721
  for (auto i = bm.size(); i < 6464; ++i)
    bm.append<0>();
  CHECK_EQUAL(rank(bm), 4u); // regression had rank 5
  bm.append<0>();
  CHECK_EQUAL(rank(bm), 4u);
  CHECK_EQUAL(bm.size(), 6465u);
}

TEST_DISABLED(regression - manual address bitmap index from bitmaps) {
  MESSAGE("populating index");
  std::array<ewah_bitmap, 32> idx;
  for (auto n = 0; n < 6464; ++n) {
    auto x = orig_h(bro_conn_log[n]);
    for (auto i = 0u; i < 4; ++i) {
      auto byte = x.data()[i + 12];
      for (auto j = 0u; j < 8; ++j)
        idx[(i * 8) + j].append_bits((byte >> j) & 1, 1);
    }
  }
  MESSAGE("querying 169.254.225.22");
  auto x = *to<address>("169.254.225.22");
  auto result = ewah_bitmap{idx[0].size(), true};
  REQUIRE_EQUAL(result.size(), 6464u);
  for (auto i = 0u; i < 4; ++i) {
    auto byte = x.data()[i + 12];
    for (auto j = 0u; j < 8; ++j) {
      auto& bm = idx[(i * 8) + j];
      result &= ((byte >> j) & 1) ? bm : ~bm;
    }
  }
  CHECK_EQUAL(rank(result), 4u);
  CHECK_EQUAL(select(result, -1), id{720});
}

TEST_DISABLED(regression - manual address bitmap index from 4 byte indexes) {
  using byte_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  std::array<byte_index, 4> idx;
  idx.fill(byte_index{8});
  MESSAGE("populating index");
  for (auto n = 0; n < 6464; ++n) {
    auto x = orig_h(bro_conn_log[n]);
    for (auto i = 0u; i < 4; ++i) {
      auto byte = x.data()[i + 12];
      idx[i].append(byte);
    }
  }
  MESSAGE("querying 169.254.225.22");
  auto x = *to<address>("169.254.225.22");
  auto result = ewah_bitmap{idx[0].size(), true};
  REQUIRE_EQUAL(result.size(), 6464u);
  for (auto i = 0u; i < 4; ++i) {
    auto byte = x.data()[i + 12];
    result &= idx[i].lookup(equal, byte);
  }
  CHECK_EQUAL(rank(result), 4u);
  CHECK_EQUAL(select(result, -1), id{720});
}

namespace {

auto service(const event& log) {
  auto& xs = caf::get<vector>(log.data());
  return make_view(xs[4]);
}

bool is_http(view<data> x) {
  return caf::get<view<std::string>>(x) == "http";
}

} // namespace <anonymous>

TEST_DISABLED(regression - bro conn log service http) {
  // The number of occurrences of the 'service == "http"' in the conn.log,
  // sliced in batches of 100. Pre-computed via:
  //  bro-cut service < test/logs/bro/conn.log \
  //    | awk '{ if ($1 == "http") ++n; if (NR % 100 == 0) { print n; n = 0 } }\
  //           END { print n }' \
  //    | paste -s -d , -
  std::vector<size_t> http_per_100_events{
    13, 16, 20, 22, 31, 11, 14, 28, 13, 42, 45, 52, 59, 54, 59, 59, 51,
    29, 21, 31, 20, 28, 9,  56, 48, 57, 32, 53, 25, 31, 25, 44, 38, 55,
    40, 23, 31, 27, 23, 59, 23, 2,  62, 29, 1,  5,  7,  0,  10, 5,  52,
    39, 2,  0,  9,  8,  0,  13, 4,  2,  13, 2,  36, 33, 17, 48, 50, 27,
    44, 9,  94, 63, 74, 66, 5,  54, 21, 7,  2,  3,  21, 7,  2,  14, 7
  };
  std::vector<std::pair<std::unique_ptr<value_index>, ids>> slices;
  slices.reserve(http_per_100_events.size());
  for (size_t i = 0; i < bro_conn_log.size(); ++i) {
    if (i % 100 == 0) {
      slices.emplace_back(value_index::make(string_type{}), ids(i, false));
    }
    auto& [idx, expected] = slices.back();
    auto x = service(bro_conn_log[i]);
    idx->append(x, i);
    expected.append_bit(is_http(x));
  }
  for (size_t i = 0; i < slices.size(); ++i) {
    MESSAGE("verifying batch [" << (i * 100) << ',' << (i * 100) + 100 << ')');
    auto& [idx, expected] = slices[i];
    auto result = idx->lookup(equal, make_data_view("http"));
    REQUIRE(result);
    CHECK_EQUAL(rank(*result), http_per_100_events[i]);
  }
}

TEST_DISABLED(regression - manual value index for bro conn log service http) {
  // Setup string size bitmap index.
  using length_bitmap_index =
    bitmap_index<uint32_t, multi_level_coder<range_coder<ids>>>;
  auto length = length_bitmap_index{base::uniform(10, 3)};
  // Setup one bitmap index per character.
  using char_bitmap_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  std::vector<char_bitmap_index> chars;
  chars.resize(42, char_bitmap_index{8});
  // Manually build a failing slice: [8000,8100).
  ewah_bitmap none;
  ewah_bitmap mask;
  for (auto i = 8000; i < 8100; ++i)
    caf::visit(detail::overload(
      [&](caf::none_t) {
        none.append_bits(false, i - none.size());
        none.append_bit(true);
        mask.append_bits(false, i - mask.size());
        mask.append_bit(true);
      },
      [&](view<std::string> x) {
        if (x.size() >= chars.size())
          FAIL("insufficient character indexes");
        for (size_t j = 0; j < x.size(); ++j) {
          chars[j].skip(i - chars[j].size());
          chars[j].append(static_cast<uint8_t>(x[j]));
        }
        length.skip(i - length.size());
        length.append(x.size());
        mask.append_bits(false, i - mask.size());
        mask.append_bit(true);
      },
      [&](auto) {
        FAIL("unexpected service type");
      }
    ), service(bro_conn_log[i]));
  REQUIRE_EQUAL(rank(mask), 100u);
  // Perform a manual index lookup for "http".
  auto http = "http"s;
  auto data = length.lookup(less_equal, http.size());
  for (auto i = 0u; i < http.size(); ++i)
    data &= chars[i].lookup(equal, static_cast<uint8_t>(http[i]));
  // Generated via:
  // bro-cut service < test/logs/bro/conn.log \
  //  | awk 'NR > 8000 && NR <= 8100 && $1 == "http" { print NR-1  }' \
  //  | paste -s -d , -
  auto expected = make_ids(
    {
      8002, 8003, 8004, 8005, 8006, 8007, 8008, 8011, 8012, 8013, 8014,
      8015, 8016, 8019, 8039, 8041, 8042, 8044, 8047, 8051, 8061,
    },
    8100);
  // Manually subtract none values and mask the result to [8000, 8100).
  auto result = (data - none) & mask;
  CHECK_EQUAL(result, expected);
}

FIXTURE_SCOPE_END()
