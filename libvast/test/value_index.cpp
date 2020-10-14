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

#include "vast/value_index.hpp"

#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/table_slice.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture : fixtures::events {
  fixture() {
    factory<value_index>::initialize();
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(value_index_tests, fixture)

TEST(bool) {
  auto idx = factory<value_index>::make(bool_type{}, caf::settings{});
  REQUIRE_NOT_EQUAL(idx, nullptr);
  MESSAGE("append");
  REQUIRE(idx->append(make_data_view(true)));
  REQUIRE(idx->append(make_data_view(true)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(true)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(true)));
  MESSAGE("lookup");
  auto f = idx->lookup(equal, make_data_view(false));
  CHECK_EQUAL(to_string(unbox(f)), "00101110");
  auto t = idx->lookup(not_equal, make_data_view(false));
  CHECK_EQUAL(to_string(unbox(t)), "11010001");
  auto xs = list{true, false};
  auto multi = unbox(idx->lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "11111111");
  MESSAGE("serialization");
  std::string buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  value_index_ptr idx2;
  REQUIRE_EQUAL(load(nullptr, buf, idx2), caf::none);
  t = idx2->lookup(equal, make_data_view(true));
  CHECK_EQUAL(to_string(unbox(t)), "11010001");
}

TEST(integer) {
  caf::settings opts;
  opts["base"] = "uniform(10, 20)";
  auto idx = factory<value_index>::make(integer_type{}, std::move(opts));
  REQUIRE_NOT_EQUAL(idx, nullptr);
  MESSAGE("append");
  REQUIRE(idx->append(make_data_view(-7)));
  REQUIRE(idx->append(make_data_view(42)));
  REQUIRE(idx->append(make_data_view(10000)));
  REQUIRE(idx->append(make_data_view(4711)));
  REQUIRE(idx->append(make_data_view(31337)));
  REQUIRE(idx->append(make_data_view(42)));
  REQUIRE(idx->append(make_data_view(42)));
  MESSAGE("lookup");
  auto leet = idx->lookup(equal, make_data_view(31337));
  CHECK(to_string(unbox(leet)) == "0000100");
  auto less_than_leet = idx->lookup(less, make_data_view(31337));
  CHECK(to_string(unbox(less_than_leet)) == "1111011");
  auto greater_zero = idx->lookup(greater, make_data_view(0));
  CHECK(to_string(unbox(greater_zero)) == "0111111");
  auto xs = list{42, 10, 4711};
  auto multi = unbox(idx->lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "0101011");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  value_index_ptr idx2;
  REQUIRE_EQUAL(load(nullptr, buf, idx2), caf::none);
  less_than_leet = idx2->lookup(less, make_data_view(31337));
  CHECK(to_string(unbox(less_than_leet)) == "1111011");
}

TEST(address) {
  address_index idx{address_type{}};
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
  CHECK(to_string(unbox(bm)) == "100110");
  bm = idx.lookup(not_equal, make_data_view(x));
  CHECK(to_string(unbox(bm)) == "011001");
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
  CHECK(to_string(unbox(bm)) == "00000011100");
  bm = idx.lookup(not_in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "11111100011");
  y = {*to<address>("192.168.0.0"), 24};
  bm = idx.lookup(in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "11111111111");
  y = {*to<address>("192.168.0.0"), 20};
  bm = idx.lookup(in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "11111111111");
  y = {*to<address>("192.168.0.64"), 26};
  bm = idx.lookup(not_in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "11111111101");
  auto xs = list{*to<address>("192.168.0.1"), *to<address>("192.168.0.2")};
  auto multi = unbox(idx.lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "11011100000");
  MESSAGE("gaps");
  x = *to<address>("192.168.0.2");
  CHECK(idx.append(make_data_view(x), 42));
  x = *to<address>("192.168.0.2");
  auto str = "01000100000"s + std::string(42 - 11, '0') + '1';
  CHECK_EQUAL(to_string(unbox(idx.lookup(equal, make_data_view(x)))), str);
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  address_index idx2{address_type{}};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  CHECK_EQUAL(to_string(unbox(idx2.lookup(equal, make_data_view(x)))), str);
}

TEST(subnet) {
  subnet_index idx{subnet_type{}};
  auto s0 = *to<subnet>("192.168.0.0/24");
  auto s1 = *to<subnet>("192.168.1.0/24");
  auto s2 = *to<subnet>("fe80::/10");
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s1)));
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s2)));
  REQUIRE(idx.append(make_data_view(s2)));
  MESSAGE("address lookup (ni)");
  auto a = unbox(to<address>("192.168.0.0")); // network address
  auto bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  a = unbox(to<address>("192.168.0.1"));
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  a = unbox(to<address>("192.168.1.42"));
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "010000");
  // IPv6
  a = unbox(to<address>("feff::")); // too far out
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  a = unbox(to<address>("fe80::aaaa"));
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "000011");
  MESSAGE("equality lookup");
  bm = idx.lookup(equal, make_data_view(s0));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  bm = idx.lookup(not_equal, make_data_view(s1));
  CHECK_EQUAL(to_string(unbox(bm)), "101111");
  MESSAGE("subset lookup (in)");
  auto x = unbox(to<subnet>("192.168.0.0/23"));
  bm = idx.lookup(in, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "111100");
  x = unbox(to<subnet>("192.168.0.0/25"));
  bm = idx.lookup(in, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  MESSAGE("subset lookup (ni)");
  bm = idx.lookup(ni, make_data_view(s0));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  x = unbox(to<subnet>("192.168.1.128/25"));
  bm = idx.lookup(ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "010000");
  x = unbox(to<subnet>("192.168.0.254/32"));
  bm = idx.lookup(ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  x = unbox(to<subnet>("192.0.0.0/8"));
  bm = idx.lookup(ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  auto xs = list{s0, s1};
  auto multi = unbox(idx.lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "111100");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  subnet_index idx2{subnet_type{}};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  bm = idx2.lookup(not_equal, make_data_view(s1));
  CHECK_EQUAL(to_string(unbox(bm)), "101111");
}

TEST(port) {
  port_index idx{port_type{}};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(port(80, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(443, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(53, port::udp))));
  REQUIRE(idx.append(make_data_view(port(8, port::icmp))));
  REQUIRE(idx.append(make_data_view(port(31337, port::unknown))));
  REQUIRE(idx.append(make_data_view(port(80, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(80, port::udp))));
  REQUIRE(idx.append(make_data_view(port(80, port::unknown))));
  REQUIRE(idx.append(make_data_view(port(8080, port::tcp))));
  MESSAGE("lookup");
  port http{80, port::tcp};
  auto bm = idx.lookup(equal, make_data_view(http));
  CHECK(to_string(unbox(bm)) == "100001000");
  bm = idx.lookup(not_equal, make_data_view(http));
  CHECK_EQUAL(to_string(unbox(bm)), "011110111");
  port port80{80, port::unknown};
  bm = idx.lookup(not_equal, make_data_view(port80));
  CHECK_EQUAL(to_string(unbox(bm)), "011110001");
  port priv{1024, port::unknown};
  bm = idx.lookup(less_equal, make_data_view(priv));
  CHECK(to_string(unbox(bm)) == "111101110");
  bm = idx.lookup(greater, make_data_view(port{2, port::unknown}));
  CHECK(to_string(unbox(bm)) == "111111111");
  auto xs = list{http, port(53, port::udp)};
  auto multi = unbox(idx.lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "101001000");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  port_index idx2{port_type{}};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  bm = idx2.lookup(less_equal, make_data_view(priv));
  CHECK_EQUAL(to_string(unbox(bm)), "111101110");
}

TEST(list) {
  auto container_type = list_type{string_type{}};
  list_index idx{container_type};
  MESSAGE("append");
  list xs{"foo", "bar"};
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
  list_index idx2{container_type};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  x = "foo";
  CHECK_EQUAL(to_string(*idx2.lookup(ni, make_data_view(x))), "11000000");
}

// This test uncovered a regression that ocurred when computing the rank of a
// bitmap representing conn.log events. The culprit was the EWAH bitmap
// encoding, because swapping out ewah_bitmap for null_bitmap in address_index
// made the bug disappear.
TEST(regression - build an address index from zeek events) {
  // Populate the index with data up to the critical point.
  address_index idx{address_type{}};
  size_t row_id = 0;
  for (auto& slice : zeek_conn_log_full) {
    for (size_t row = 0; row < slice->rows(); ++row) {
      // Column 2 is orig_h.
      if (!idx.append(slice->at(row, 2), row_id))
        FAIL("appending to the value_index failed!");
      if (++row_id == 6464) {
        // The last ID should be 720 at this point.
        auto addr = unbox(to<data>("169.254.225.22"));
        auto before = unbox(idx.lookup(equal, make_data_view(addr)));
        CHECK_EQUAL(rank(before), 4u);
        CHECK_EQUAL(select(before, -1), id{720});
      }
    }
  }
  // Checking again after ingesting all events must not change the outcome.
  auto addr = unbox(to<data>("169.254.225.22"));
  auto before = unbox(idx.lookup(equal, make_data_view(addr)));
  CHECK_EQUAL(rank(before), 4u);
  CHECK_EQUAL(select(before, -1), id{720});
}

// This was the first attempt in figuring out where the bug sat. It didn't fire.
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
  size_t row_id = 0;
  for (auto& slice : zeek_conn_log_full) {
    for (size_t row = 0; row < slice->rows(); ++row) {
      // Column 2 is orig_h.
      auto x = caf::get<view<address>>(slice->at(row, 2));
      for (auto i = 0u; i < 4; ++i) {
        auto byte = x.data()[i + 12];
        for (auto j = 0u; j < 8; ++j)
          idx[(i * 8) + j].append_bits((byte >> j) & 1, 1);
      }
      if (++row_id == 6464) {
        auto addr = unbox(to<address>("169.254.225.22"));
        auto result = ewah_bitmap{idx[0].size(), true};
        REQUIRE_EQUAL(result.size(), 6464u);
        for (auto i = 0u; i < 4; ++i) {
          auto byte = addr.data()[i + 12];
          for (auto j = 0u; j < 8; ++j) {
            auto& bm = idx[(i * 8) + j];
            result &= ((byte >> j) & 1) ? bm : ~bm;
          }
        }
        CHECK_EQUAL(rank(result), 4u);
        CHECK_EQUAL(select(result, -1), id{720});
        // Done testing, we're only interested in the first 6464 rows.
        return;
      }
    }
  }
}

TEST(regression - manual address bitmap index from 4 byte indexes) {
  using byte_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  std::array<byte_index, 4> idx;
  idx.fill(byte_index{8});
  size_t row_id = 0;
  MESSAGE("populating index");
  for (auto& slice : zeek_conn_log_full) {
    for (size_t row = 0; row < slice->rows(); ++row) {
      // Column 2 is orig_h.
      auto x = caf::get<view<address>>(slice->at(row, 2));
      for (auto i = 0u; i < 4; ++i) {
        auto byte = x.data()[i + 12];
        idx[i].append(byte);
      }
      if (++row_id == 6464) {
        MESSAGE("querying 169.254.225.22");
        auto x = unbox(to<address>("169.254.225.22"));
        auto result = ewah_bitmap{idx[0].size(), true};
        REQUIRE_EQUAL(result.size(), 6464u);
        for (auto i = 0u; i < 4; ++i) {
          auto byte = x.data()[i + 12];
          result &= idx[i].lookup(equal, byte);
        }
        CHECK_EQUAL(rank(result), 4u);
        CHECK_EQUAL(select(result, -1), id{720});
        // Done testing, we're only interested in the first 6464 rows.
        return;
      }
    }
  }
}

FIXTURE_SCOPE_END()
