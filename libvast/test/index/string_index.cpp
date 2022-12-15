//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE value_index

#include "vast/index/string_index.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/fbs/value_index.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"
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

} // namespace

FIXTURE_SCOPE(value_index_tests, fixture)

TEST(string) {
  caf::settings opts;
  opts["max-size"] = 100;
  string_index idx{type{string_type{}}, opts};
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
  auto result = idx.lookup(relational_operator::equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(result)), "1001100000");
  result = idx.lookup(relational_operator::equal, make_data_view("bar"));
  CHECK_EQUAL(to_string(unbox(result)), "0100010000");
  result = idx.lookup(relational_operator::equal, make_data_view("baz"));
  CHECK_EQUAL(to_string(unbox(result)), "0010000000");
  result = idx.lookup(relational_operator::equal, make_data_view(""));
  CHECK_EQUAL(to_string(unbox(result)), "0000001000");
  result = idx.lookup(relational_operator::equal, make_data_view("qux"));
  CHECK_EQUAL(to_string(unbox(result)), "0000000100");
  result = idx.lookup(relational_operator::equal, make_data_view("corge"));
  CHECK_EQUAL(to_string(unbox(result)), "0000000010");
  result = idx.lookup(relational_operator::equal, make_data_view("bazz"));
  CHECK_EQUAL(to_string(unbox(result)), "0000000001");
  result = idx.lookup(relational_operator::not_equal, make_data_view(""));
  CHECK_EQUAL(to_string(unbox(result)), "1111110111");
  result = idx.lookup(relational_operator::not_equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(result)), "0110011111");
  result = idx.lookup(relational_operator::not_ni, make_data_view(""));
  CHECK_EQUAL(to_string(unbox(result)), "0000000000");
  result = idx.lookup(relational_operator::ni, make_data_view(""));
  CHECK_EQUAL(to_string(unbox(result)), "1111111111");
  result = idx.lookup(relational_operator::ni, make_data_view("o"));
  CHECK_EQUAL(to_string(unbox(result)), "1001100010");
  result = idx.lookup(relational_operator::ni, make_data_view("oo"));
  CHECK_EQUAL(to_string(unbox(result)), "1001100000");
  result = idx.lookup(relational_operator::ni, make_data_view("z"));
  CHECK_EQUAL(to_string(unbox(result)), "0010000001");
  result = idx.lookup(relational_operator::ni, make_data_view("zz"));
  CHECK_EQUAL(to_string(unbox(result)), "0000000001");
  result = idx.lookup(relational_operator::ni, make_data_view("ar"));
  CHECK_EQUAL(to_string(unbox(result)), "0100010000");
  result = idx.lookup(relational_operator::ni, make_data_view("rge"));
  CHECK_EQUAL(to_string(unbox(result)), "0000000010");
  auto xs = list{"foo", "bar", "baz"};
  result = idx.lookup(relational_operator::in, make_data_view(xs));
  CHECK_EQUAL(to_string(unbox(result)), "1111110000");
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  auto idx2 = string_index{type{string_type{}}};
  CHECK_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  result = idx2.lookup(relational_operator::equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(result)), "1001100000");
  result = idx2.lookup(relational_operator::equal, make_data_view("bar"));
  CHECK_EQUAL(to_string(unbox(result)), "0100010000");
}

TEST(none values - string) {
  auto idx = factory<value_index>::make(type{string_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(idx, nullptr);
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
  auto bm = idx->lookup(relational_operator::equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(bm)), "01100010000001110001100");
  bm = idx->lookup(relational_operator::not_equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(bm)), "10011101111110001110011");
  bm = idx->lookup(relational_operator::equal, make_data_view(caf::none));
  CHECK_EQUAL(to_string(unbox(bm)), "10011100011110000000011");
  bm = idx->lookup(relational_operator::not_equal, make_data_view(caf::none));
  CHECK_EQUAL(to_string(unbox(bm)), "01100011100001111111100");
  auto builder = flatbuffers::FlatBufferBuilder{};
  const auto idx_offset = pack(builder, idx);
  builder.Finish(idx_offset);
  auto maybe_fb = flatbuffer<fbs::ValueIndex>::make(builder.Release());
  REQUIRE_NOERROR(maybe_fb);
  auto fb = *maybe_fb;
  REQUIRE(fb);
  auto idx2 = value_index_ptr{};
  REQUIRE_EQUAL(unpack(*fb, idx2), caf::none);
  CHECK_EQUAL(idx->type(), idx2->type());
  CHECK_EQUAL(idx->options(), idx2->options());
  bm = idx2->lookup(relational_operator::equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(bm)), "01100010000001110001100");
  bm = idx2->lookup(relational_operator::not_equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(bm)), "10011101111110001110011");
  bm = idx2->lookup(relational_operator::equal, make_data_view(caf::none));
  CHECK_EQUAL(to_string(unbox(bm)), "10011100011110000000011");
  bm = idx2->lookup(relational_operator::not_equal, make_data_view(caf::none));
  CHECK_EQUAL(to_string(unbox(bm)), "01100011100001111111100");
}

TEST(regression - zeek conn log service http) {
  // The number of occurrences of the 'service == "http"' in the conn.log,
  // sliced in batches of 100. Pre-computed via:
  //  zeek-cut service < test/logs/zeek/conn.log
  //    | awk '{ if ($1 == "http") ++n; if (NR % 100 == 0) { print n; n = 0 } }
  //           END { print n }'
  //    | paste -s -d , -
  auto is_http = [](auto x) {
    return caf::get<view<std::string>>(x) == "http";
  };
  std::vector<size_t> http_per_100_events{
    13, 16, 20, 22, 31, 11, 14, 28, 13, 42, 45, 52, 59, 54, 59, 59, 51,
    29, 21, 31, 20, 28, 9,  56, 48, 57, 32, 53, 25, 31, 25, 44, 38, 55,
    40, 23, 31, 27, 23, 59, 23, 2,  62, 29, 1,  5,  7,  0,  10, 5,  52,
    39, 2,  0,  9,  8,  0,  13, 4,  2,  13, 2,  36, 33, 17, 48, 50, 27,
    44, 9,  94, 63, 74, 66, 5,  54, 21, 7,  2,  3,  21, 7,  2,  14, 7,
  };
  auto& slices = zeek_conn_log_full;
  REQUIRE_EQUAL(slices.size(), http_per_100_events.size());
  REQUIRE(std::all_of(slices.begin(), prev(slices.end()), [](auto& slice) {
    return slice.rows() == 100;
  }));
  std::vector<std::pair<value_index_ptr, ids>> slice_stats;
  slice_stats.reserve(slices.size());
  size_t row_id = 0;
  for (auto& slice : slices) {
    slice_stats.emplace_back(factory<value_index>::make(type{string_type{}},
                                                        caf::settings{}),
                             ids(row_id, false));
    auto& [idx, expected] = slice_stats.back();
    for (size_t row = 0; row < slice.rows(); ++row) {
      // Column 7 is service.
      auto x = slice.at(row, 7);
      idx->append(x, row_id);
      expected.append_bit(is_http(x));
      ++row_id;
    }
  }
  for (size_t i = 0; i < slice_stats.size(); ++i) {
    MESSAGE("verifying batch [" << (i * 100) << ',' << (i * 100) + 100 << ')');
    auto& [idx, expected] = slice_stats[i];
    auto result
      = unbox(idx->lookup(relational_operator::equal, make_data_view("http")));
    CHECK_EQUAL(rank(result), http_per_100_events[i]);
  }
}

TEST(regression - manual value index for zeek conn log service http) {
  // Setup string size bitmap index.
  using length_bitmap_index
    = bitmap_index<uint32_t, multi_level_coder<range_coder<ids>>>;
  auto length = length_bitmap_index{base::uniform(10, 3)};
  // Setup one bitmap index per character.
  using char_bitmap_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  std::vector<char_bitmap_index> chars;
  chars.reserve(42);
  for (size_t i = 0; i < 42; ++i)
    chars.emplace_back(8);
  // Manually build a failing slice: [8000,8100).
  ewah_bitmap none;
  ewah_bitmap mask;
  // Get the slice that contains the events for [8000,8100).
  auto slice = zeek_conn_log_full[80];
  for (size_t row = 0; row < slice.rows(); ++row) {
    auto i = 8000 + row;
    auto f = detail::overload{
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
      },
    };
    // Column 7 is service.
    caf::visit(f, slice.at(row, 7));
  }
  REQUIRE_EQUAL(rank(mask), 100u);
  // Perform a manual index lookup for "http".
  auto http = "http"s;
  auto data = length.lookup(relational_operator::less_equal, http.size());
  for (auto i = 0u; i < http.size(); ++i)
    data &= chars[i].lookup(relational_operator::equal,
                            static_cast<uint8_t>(http[i]));
  // Generated via:
  // zeek-cut service < test/logs/zeek/conn.log
  //  | awk 'NR > 8000 && NR <= 8100 && $1 == "http" { print NR-1  }'
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
