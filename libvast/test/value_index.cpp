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

#define SUITE value_index
#include "test.hpp"
#include "fixtures/events.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(boolean) {
  arithmetic_index<boolean> idx;
  MESSAGE("push_back");
  REQUIRE(idx.push_back(true));
  REQUIRE(idx.push_back(true));
  REQUIRE(idx.push_back(false));
  REQUIRE(idx.push_back(true));
  REQUIRE(idx.push_back(false));
  REQUIRE(idx.push_back(false));
  REQUIRE(idx.push_back(false));
  REQUIRE(idx.push_back(true));
  MESSAGE("lookup");
  auto f = idx.lookup(equal, false);
  REQUIRE(f);
  CHECK_EQUAL(to_string(*f), "00101110");
  auto t = idx.lookup(not_equal, false);
  REQUIRE(t);
  CHECK_EQUAL(to_string(*t), "11010001");
  auto multi = idx.lookup(in, set{true, false});
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "11111111");
  MESSAGE("serialization");
  std::string buf;
  save(buf, idx);
  arithmetic_index<boolean> idx2;
  load(buf, idx2);
  t = idx2.lookup(equal, true);
  REQUIRE(t);
  CHECK_EQUAL(to_string(*t), "11010001");
}

TEST(integer) {
  arithmetic_index<integer> idx{base::uniform(10, 20)};
  MESSAGE("push_back");
  REQUIRE(idx.push_back(-7));
  REQUIRE(idx.push_back(42));
  REQUIRE(idx.push_back(10000));
  REQUIRE(idx.push_back(4711));
  REQUIRE(idx.push_back(31337));
  REQUIRE(idx.push_back(42));
  REQUIRE(idx.push_back(42));
  MESSAGE("lookup");
  auto leet = idx.lookup(equal, 31337);
  REQUIRE(leet);
  CHECK(to_string(*leet) == "0000100");
  auto less_than_leet = idx.lookup(less, 31337);
  REQUIRE(less_than_leet);
  CHECK(to_string(*less_than_leet) == "1111011");
  auto greater_zero = idx.lookup(greater, 0);
  REQUIRE(greater_zero);
  CHECK(to_string(*greater_zero) == "0111111");
  auto multi = idx.lookup(in, set{42, 10, 4711});
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "0101011");
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  auto idx2 = arithmetic_index<integer>{};
  load(buf, idx2);
  less_than_leet = idx2.lookup(less, 31337);
  REQUIRE(less_than_leet);
  CHECK(to_string(*less_than_leet) == "1111011");
}

TEST(floating-point with custom binner) {
  using index_type = arithmetic_index<real, precision_binner<6, 2>>;
  auto idx = index_type{base::uniform<64>(10)};
  MESSAGE("push_back");
  REQUIRE(idx.push_back(-7.8));
  REQUIRE(idx.push_back(42.123));
  REQUIRE(idx.push_back(10000.0));
  REQUIRE(idx.push_back(4711.13510));
  REQUIRE(idx.push_back(31337.3131313));
  REQUIRE(idx.push_back(42.12258));
  REQUIRE(idx.push_back(42.125799));
  MESSAGE("lookup");
  CHECK(to_string(*idx.lookup(less, 100.0)) == "1100011");
  CHECK(to_string(*idx.lookup(less, 43.0)) == "1100011");
  CHECK(to_string(*idx.lookup(greater_equal, 42.0)) == "0111111");
  CHECK(to_string(*idx.lookup(not_equal, 4711.14)) == "1110111");
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  auto idx2 = index_type{};
  load(buf, idx2);
  CHECK(to_string(*idx2.lookup(not_equal, 4711.14)) == "1110111");
}

TEST(timespan) {
  using namespace std::chrono;
  // Default binning gives granularity of seconds.
  auto idx = arithmetic_index<timespan>{base::uniform<64>(10)};
  MESSAGE("push_back");
  REQUIRE(idx.push_back(milliseconds(1000)));
  REQUIRE(idx.push_back(milliseconds(2000)));
  REQUIRE(idx.push_back(milliseconds(3000)));
  REQUIRE(idx.push_back(milliseconds(1011)));
  REQUIRE(idx.push_back(milliseconds(2222)));
  REQUIRE(idx.push_back(milliseconds(2322)));
  MESSAGE("lookup");
  auto hun = idx.lookup(equal, milliseconds(1034));
  REQUIRE(hun);
  CHECK(to_string(*hun) == "100100");
  auto twokay = idx.lookup(less_equal, milliseconds(2000));
  REQUIRE(twokay);
  CHECK(to_string(*twokay) == "110111");
  auto twelve = idx.lookup(greater, milliseconds(1200));
  REQUIRE(twelve);
  CHECK(to_string(*twelve) == "011011");
}

TEST(timestamp) {
  arithmetic_index<timestamp> idx{base::uniform<64>(10)};
  auto t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  MESSAGE("push_back");
  REQUIRE(idx.push_back(*t));
  t = to<timestamp>("2014-01-16+05:30:12");
  REQUIRE(t);
  REQUIRE(idx.push_back(*t));
  t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  REQUIRE(idx.push_back(*t));
  t = to<timestamp>("2014-01-16+05:30:18");
  REQUIRE(t);
  REQUIRE(idx.push_back(*t));
  t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  REQUIRE(idx.push_back(*t));
  t = to<timestamp>("2014-01-16+05:30:19");
  REQUIRE(t);
  REQUIRE(idx.push_back(*t));
  MESSAGE("lookup");
  t = to<timestamp>("2014-01-16+05:30:15");
  REQUIRE(t);
  auto fifteen = idx.lookup(equal, *t);
  CHECK(to_string(*fifteen) == "101010");
  t = to<timestamp>("2014-01-16+05:30:20");
  REQUIRE(t);
  auto twenty = idx.lookup(less, *t);
  CHECK(to_string(*twenty) == "111111");
  t = to<timestamp>("2014-01-16+05:30:18");
  REQUIRE(t);
  auto eighteen = idx.lookup(greater_equal, *t);
  CHECK(to_string(*eighteen) == "000101");
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  auto idx2 = decltype(idx){};
  load(buf, idx2);
  eighteen = idx2.lookup(greater_equal, *t);
  CHECK(to_string(*eighteen) == "000101");
}

TEST(string) {
  string_index idx{100};
  MESSAGE("push_back");
  REQUIRE(idx.push_back("foo"));
  REQUIRE(idx.push_back("bar"));
  REQUIRE(idx.push_back("baz"));
  REQUIRE(idx.push_back("foo"));
  REQUIRE(idx.push_back("foo"));
  REQUIRE(idx.push_back("bar"));
  REQUIRE(idx.push_back(""));
  REQUIRE(idx.push_back("qux"));
  REQUIRE(idx.push_back("corge"));
  REQUIRE(idx.push_back("bazz"));
  MESSAGE("lookup");
  CHECK_EQUAL(to_string(*idx.lookup(equal, "foo")),   "1001100000");
  CHECK_EQUAL(to_string(*idx.lookup(equal, "bar")),   "0100010000");
  CHECK_EQUAL(to_string(*idx.lookup(equal, "baz")),   "0010000000");
  CHECK_EQUAL(to_string(*idx.lookup(equal, "")),      "0000001000");
  CHECK_EQUAL(to_string(*idx.lookup(equal, "qux")),   "0000000100");
  CHECK_EQUAL(to_string(*idx.lookup(equal, "corge")), "0000000010");
  CHECK_EQUAL(to_string(*idx.lookup(equal, "bazz")),  "0000000001");
  CHECK_EQUAL(to_string(*idx.lookup(not_equal, "")),    "1111110111");
  CHECK_EQUAL(to_string(*idx.lookup(not_equal, "foo")), "0110011111");
  CHECK_EQUAL(to_string(*idx.lookup(not_ni, "")), "0000000000");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "")),     "1111111111");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "o")),    "1001100010");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "oo")),   "1001100000");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "z")),    "0010000001");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "zz")),   "0000000001");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "ar")),   "0100010000");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "rge")),  "0000000010");
  auto e = idx.lookup(match, "foo");
  CHECK(!e);
  auto multi = idx.lookup(in, set{"foo", "bar", "baz"});
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "1111110000");
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  string_index idx2{};
  load(buf, idx2);
  CHECK_EQUAL(to_string(*idx2.lookup(equal, "foo")), "1001100000");
  CHECK_EQUAL(to_string(*idx2.lookup(equal, "bar")), "0100010000");
}

TEST(address) {
  address_index idx;
  MESSAGE("push_back");
  REQUIRE(idx.push_back(*to<address>("192.168.0.1")));
  REQUIRE(idx.push_back(*to<address>("192.168.0.2")));
  REQUIRE(idx.push_back(*to<address>("192.168.0.3")));
  REQUIRE(idx.push_back(*to<address>("192.168.0.1")));
  REQUIRE(idx.push_back(*to<address>("192.168.0.1")));
  REQUIRE(idx.push_back(*to<address>("192.168.0.2")));
  MESSAGE("address equality");
  auto addr = *to<address>("192.168.0.1");
  auto bm = idx.lookup(equal, addr);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "100110");
  bm = idx.lookup(not_equal, addr);
  CHECK(to_string(*bm) == "011001");
  addr = *to<address>("192.168.0.5");
  CHECK(to_string(*idx.lookup(equal, addr)) == "000000");
  CHECK(!idx.lookup(match, *to<address>("::"))); // Invalid operator
  MESSAGE("prefix membership");
  CHECK(idx.push_back(*to<address>("192.168.0.128")));
  CHECK(idx.push_back(*to<address>("192.168.0.130")));
  CHECK(idx.push_back(*to<address>("192.168.0.240")));
  CHECK(idx.push_back(*to<address>("192.168.0.127")));
  CHECK(idx.push_back(*to<address>("192.168.0.33")));
  auto sub = subnet{*to<address>("192.168.0.128"), 25};
  bm = idx.lookup(in, sub);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "00000011100");
  bm = idx.lookup(not_in, sub);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111100011");
  sub = {*to<address>("192.168.0.0"), 24};
  bm = idx.lookup(in, sub);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111111111");
  sub = {*to<address>("192.168.0.0"), 20};
  bm = idx.lookup(in, sub);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111111111");
  sub = {*to<address>("192.168.0.64"), 26};
  bm = idx.lookup(not_in, sub);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "11111111101");
  auto xs = vector{*to<address>("192.168.0.1"), *to<address>("192.168.0.2")};
  auto multi = idx.lookup(in, xs);
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "11011100000");
  MESSAGE("gaps");
  CHECK(idx.push_back(*to<address>("192.168.0.2"), 42));
  addr = *to<address>("192.168.0.2");
  auto str = "01000100000"s + std::string('0', 42) + '1';
  CHECK_EQUAL(idx.lookup(equal, addr), str);
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  address_index idx2{};
  load(buf, idx2);
  CHECK_EQUAL(idx2.lookup(equal, addr), str);
}

TEST(subnet) {
  subnet_index idx;
  auto s0 = to<subnet>("192.168.0.0/24");
  auto s1 = to<subnet>("192.168.1.0/24");
  auto s2 = to<subnet>("::/40");
  REQUIRE(s0);
  REQUIRE(s1);
  REQUIRE(s2);
  MESSAGE("push_back");
  REQUIRE(idx.push_back(*s0));
  REQUIRE(idx.push_back(*s1));
  REQUIRE(idx.push_back(*s0));
  REQUIRE(idx.push_back(*s0));
  REQUIRE(idx.push_back(*s2));
  REQUIRE(idx.push_back(*s2));
  MESSAGE("equality lookup");
  auto bm = idx.lookup(equal, *s0);
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101100");
  bm = idx.lookup(not_equal, *s1);
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101111");
  MESSAGE("subset lookup (in)");
  bm = idx.lookup(in, *to<subnet>("192.168.0.0/23"));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "111100");
  bm = idx.lookup(in, *to<subnet>("192.168.0.0/25"));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "000000");
  MESSAGE("subset lookup (ni)");
  bm = idx.lookup(ni, *s0);
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101100");
  bm = idx.lookup(ni, *to<subnet>("192.168.1.128/25"));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "010000");
  bm = idx.lookup(ni, *to<subnet>("192.168.0.254/32"));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101100");
  bm = idx.lookup(ni, *to<subnet>("192.0.0.0/8"));
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "000000");
  auto multi = idx.lookup(in, vector{*s0, *s1});
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "111100");
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  subnet_index idx2;
  load(buf, idx2);
  bm = idx2.lookup(not_equal, *s1);
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "101111");
}

TEST(port) {
  port_index idx;
  MESSAGE("push_back");
  REQUIRE(idx.push_back(port(80, port::tcp)));
  REQUIRE(idx.push_back(port(443, port::tcp)));
  REQUIRE(idx.push_back(port(53, port::udp)));
  REQUIRE(idx.push_back(port(8, port::icmp)));
  REQUIRE(idx.push_back(port(31337, port::unknown)));
  REQUIRE(idx.push_back(port(80, port::tcp)));
  REQUIRE(idx.push_back(port(8080, port::tcp)));
  MESSAGE("lookup");
  port http{80, port::tcp};
  auto bm = idx.lookup(equal, http);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "1000010");
  port priv{1024, port::unknown};
  bm = idx.lookup(less_equal, priv);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "1111010");
  bm = idx.lookup(greater, port{2, port::unknown});
  REQUIRE(bm);
  CHECK(to_string(*bm) == "1111111");
  auto multi = idx.lookup(in, vector{http, port(53, port::udp)});
  REQUIRE(multi);
  CHECK_EQUAL(to_string(*multi), "1010010");
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  port_index idx2;
  load(buf, idx2);
  bm = idx2.lookup(less_equal, priv);
  REQUIRE(bm);
  CHECK(to_string(*bm) == "1111010");
}

TEST(container) {
  sequence_index idx{string_type{}};
  MESSAGE("push_back");
  vector v{"foo", "bar"};
  REQUIRE(idx.push_back(v));
  v = {"qux", "foo", "baz", "corge"};
  REQUIRE(idx.push_back(v));
  v = {"bar"};
  REQUIRE(idx.push_back(v));
  REQUIRE(idx.push_back(v));
  REQUIRE(idx.push_back(v, 7));
  MESSAGE("lookup");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "foo")), "11000000");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "bar")), "10110001");
  CHECK_EQUAL(to_string(*idx.lookup(not_ni, "foo")), "00110001");
  CHECK_EQUAL(to_string(*idx.lookup(ni, "not")), "00000000");
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, idx);
  sequence_index idx2;
  load(buf, idx2);
  CHECK_EQUAL(to_string(*idx2.lookup(ni, "foo")), "11000000");
  CHECK_EQUAL(to_string(*idx2.lookup(ni, "bar")), "10110001");
}

TEST(polymorphic) {
  type t = set_type{integer_type{}}.attributes({{"max_size", "2"}});
  auto idx = value_index::make(t);
  REQUIRE(idx);
  REQUIRE(idx->push_back(set{42, 43, 44}));
  REQUIRE(idx->push_back(set{1, 2, 3}));
  REQUIRE(idx->push_back(set{}));
  REQUIRE(idx->push_back(set{42}));
  CHECK_EQUAL(to_string(*idx->lookup(ni, 42)), "1001");
  CHECK_EQUAL(to_string(*idx->lookup(ni, 44)), "0000"); // chopped off
  MESSAGE("serialization");
  std::vector<char> buf;
  save(buf, detail::value_index_inspect_helper{t, idx});
  std::unique_ptr<value_index> idx2;
  detail::value_index_inspect_helper helper{t, idx2};
  load(buf, helper);
  REQUIRE(idx2);
  CHECK_EQUAL(to_string(*idx2->lookup(ni, 42)), "1001");
  MESSAGE("attributes");
  t = integer_type{}.attributes({{"base", "uniform(2, 4)"}});
  idx = value_index::make(t);
  REQUIRE(idx);
  t = integer_type{}.attributes({{"base", "[2, 3,4]"}});
  idx = value_index::make(t);
  REQUIRE(idx);
  MESSAGE("nil");
  REQUIRE(idx->push_back(nil));
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
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back("bar"));
  REQUIRE(idx->push_back("bar"));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back("bar"));
  REQUIRE(idx->push_back("bar"));
  REQUIRE(idx->push_back("bar"));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back("foo"));
  REQUIRE(idx->push_back(nil));
  REQUIRE(idx->push_back(nil));
  auto bm = idx->lookup(equal, "foo");
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "01100010000001110001100");
  bm = idx->lookup(not_equal, "foo"); // NB: not same as !(x == 42)
  REQUIRE(bm);
  CHECK_EQUAL(to_string(*bm), "00000001100000001110000");
}

FIXTURE_SCOPE(bro_conn_log_value_index_tests, fixtures::events)

const address& orig_h(const event& x) {
  auto& log_entry = caf::get<vector>(x.data());
  auto& conn_id = caf::get<vector>(log_entry[2]);
  return caf::get<address>(conn_id[0]);
};

// This test uncovered a regression that ocurred when computing the rank of a
// bitmap representing conn.log events. The culprit was the EWAH bitmap
// encoding, because swapping out ewah_bitmap for null_bitmap in address_index
// made the bug disappear.
TEST(regression - build an address index from bro events) {
  // Populate the index with data up to the critical point.
  address_index idx;
  for (auto i = 0; i < 6464; ++i) {
    auto& x = bro_conn_log[i];
    CHECK(idx.push_back(orig_h(x), x.id()));
  }
  // This is where we are in trouble: the last ID should be 720, but the bogus
  // test reports 6452.
  auto addr = *to<data>("169.254.225.22");
  auto before = idx.lookup(equal, addr);
  CHECK_EQUAL(rank(*before), 4u);
  CHECK_EQUAL(select(*before, -1), id{720});
  auto& x = bro_conn_log[6464];
  // After adding another event, the correct state is restored again and the
  // bug doesn't show up anymore.
  CHECK(idx.push_back(orig_h(x), x.id()));
  auto after = idx.lookup(equal, addr);
  CHECK_EQUAL(rank(*after), 4u);
  CHECK_EQUAL(select(*after, -1), id{720});
  CHECK_NOT_EQUAL(select(*after, -1), id{6452});
}

// This was the first attempt in figuring out where the bug sat. I didn't fire.
TEST(regression - checking the result single bitmap) {
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

TEST(regression - manual address bitmap index from bitmaps) {
  MESSAGE("populating index");
  std::array<ewah_bitmap, 32> idx;
  for (auto n = 0; n < 6464; ++n) {
    auto& x = orig_h(bro_conn_log[n]);
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

TEST(regression - manual address bitmap index from 4 byte indexes) {
  using byte_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  std::array<byte_index, 4> idx;
  idx.fill(byte_index{8});
  MESSAGE("populating index");
  for (auto n = 0; n < 6464; ++n) {
    auto& x = orig_h(bro_conn_log[n]);
    for (auto i = 0u; i < 4; ++i) {
      auto byte = x.data()[i + 12];
      idx[i].push_back(byte);
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

FIXTURE_SCOPE_END()
