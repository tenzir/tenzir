//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/index/ip_index.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/ip.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/subnet.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(ip) {
  ip_index idx{type{ip_type{}}};
  MESSAGE("append");
  auto x = *to<ip>("192.168.0.1");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.2");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.3");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.1");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.1");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.2");
  REQUIRE(idx.append(make_data_view(x)));
  x = *to<ip>("::fc00");
  REQUIRE(idx.append(make_data_view(x)));
  MESSAGE("ip equality");
  x = *to<ip>("192.168.0.1");
  auto bm = idx.lookup(relational_operator::equal, make_data_view(x));
  CHECK(to_string(unbox(bm)) == "1001100");
  bm = idx.lookup(relational_operator::not_equal, make_data_view(x));
  CHECK(to_string(unbox(bm)) == "0110011");
  x = *to<ip>("192.168.0.5");
  CHECK(to_string(*idx.lookup(relational_operator::equal, make_data_view(x)))
        == "0000000");
  MESSAGE("invalid operator");
  CHECK(!idx.lookup(relational_operator::in, make_data_view(x)));
  MESSAGE("prefix membership");
  x = *to<ip>("192.168.0.128");
  CHECK(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.130");
  CHECK(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.240");
  CHECK(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.127");
  CHECK(idx.append(make_data_view(x)));
  x = *to<ip>("192.168.0.33");
  CHECK(idx.append(make_data_view(x)));
  auto y = unbox(to<subnet>("192.168.0.128/25"));
  bm = idx.lookup(relational_operator::in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "000000011100");
  bm = idx.lookup(relational_operator::not_in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "111111100011");
  y = unbox(to<subnet>("192.168.0.0/24"));
  bm = idx.lookup(relational_operator::in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "111111011111");
  y = unbox(to<subnet>("192.168.0.0/20"));
  bm = idx.lookup(relational_operator::in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "111111011111");
  y = unbox(to<subnet>("192.168.0.64/26"));
  bm = idx.lookup(relational_operator::not_in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "111111111101");
  y = unbox(to<subnet>("::ffff:0:0/96"));
  bm = idx.lookup(relational_operator::not_in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "000000100000");
  y = unbox(to<subnet>("::ffff:0:0/96"));
  bm = idx.lookup(relational_operator::in, make_data_view(y));
  CHECK(to_string(unbox(bm)) == "111111011111");
  y = unbox(to<subnet>("::ffff:0:0/92"));
  bm = idx.lookup(relational_operator::in, make_data_view(y));
  CHECK_EQUAL(to_string(unbox(bm)), "111111011111");
  auto xs = list{*to<ip>("192.168.0.1"), *to<ip>("192.168.0.2")};
  auto multi = unbox(idx.lookup(relational_operator::in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "110111000000");
  MESSAGE("gaps");
  x = *to<ip>("192.168.0.2");
  CHECK(idx.append(make_data_view(x), 42));
  x = *to<ip>("192.168.0.2");
  auto str = "01000100000"s + std::string(42 - 11, '0') + '1';
  CHECK_EQUAL(
    to_string(unbox(idx.lookup(relational_operator::equal, make_data_view(x)))),
    str);
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  ip_index idx2{type{ip_type{}}};
  CHECK_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  CHECK_EQUAL(to_string(unbox(
                idx2.lookup(relational_operator::equal, make_data_view(x)))),
              str);
}

FIXTURE_SCOPE(value_index_tests, fixtures::events)

// This test uncovered a regression that ocurred when computing the rank of a
// bitmap representing conn.log events. The culprit was the EWAH bitmap
// encoding, because swapping out ewah_bitmap for null_bitmap in ip_index
// made the bug disappear.
TEST(regression - build an ip index from zeek events) {
  // Populate the index with data up to the critical point.
  ip_index idx{type{ip_type{}}};
  size_t row_id = 0;
  for (auto& slice : zeek_conn_log_full) {
    for (size_t row = 0; row < slice.rows(); ++row) {
      // Column 2 is orig_h.
      if (!idx.append(slice.at(row, 2), row_id))
        FAIL("appending to the value_index failed!");
      if (++row_id == 6464) {
        // The last ID should be 720 at this point.
        auto addr = unbox(to<data>("169.254.225.22"));
        auto before
          = unbox(idx.lookup(relational_operator::equal, make_data_view(addr)));
        CHECK_EQUAL(rank(before), 4u);
        CHECK_EQUAL(select(before, -1), id{720});
      }
    }
  }
  // Checking again after ingesting all events must not change the outcome.
  auto addr = unbox(to<data>("169.254.225.22"));
  auto before
    = unbox(idx.lookup(relational_operator::equal, make_data_view(addr)));
  CHECK_EQUAL(rank(before), 4u);
  CHECK_EQUAL(select(before, -1), id{720});
}

TEST(regression - manual ip bitmap index from bitmaps) {
  MESSAGE("populating index");
  std::array<ewah_bitmap, 32> idx;
  size_t row_id = 0;
  for (auto& slice : zeek_conn_log_full) {
    for (size_t row = 0; row < slice.rows(); ++row) {
      // Column 2 is orig_h.
      auto x = caf::get<view<ip>>(slice.at(row, 2));
      for (auto i = 0u; i < 4; ++i) {
        auto bytes = static_cast<ip::byte_array>(x);
        auto byte = bytes[i + 12];
        for (auto j = 0u; j < 8; ++j)
          idx[(i * 8) + j].append_bits((byte >> j) & 1, 1);
      }
      if (++row_id == 6464) {
        auto addr = unbox(to<ip>("169.254.225.22"));
        auto result = ewah_bitmap{idx[0].size(), true};
        REQUIRE_EQUAL(result.size(), 6464u);
        for (auto i = 0u; i < 4; ++i) {
          auto bytes = static_cast<ip::byte_array>(addr);
          auto byte = bytes[i + 12];
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

TEST(regression - manual ip bitmap index from 4 byte indexes) {
  using byte_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  std::array<byte_index, 4> idx;
  for (auto& elem : idx)
    elem = byte_index{8};
  size_t row_id = 0;
  MESSAGE("populating index");
  for (auto& slice : zeek_conn_log_full) {
    for (size_t row = 0; row < slice.rows(); ++row) {
      // Column 2 is orig_h.
      auto x = caf::get<view<ip>>(slice.at(row, 2));
      for (auto i = 0u; i < 4; ++i) {
        auto bytes = static_cast<ip::byte_array>(x);
        auto byte = bytes[i + 12];
        idx[i].append(byte);
      }
      if (++row_id == 6464) {
        MESSAGE("querying 169.254.225.22");
        auto x = unbox(to<ip>("169.254.225.22"));
        auto result = ewah_bitmap{idx[0].size(), true};
        REQUIRE_EQUAL(result.size(), 6464u);
        for (auto i = 0u; i < 4; ++i) {
          auto bytes = static_cast<ip::byte_array>(x);
          auto byte = bytes[i + 12];
          result &= idx[i].lookup(relational_operator::equal, byte);
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
