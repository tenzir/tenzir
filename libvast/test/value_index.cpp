//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE value_index

#include "vast/value_index.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/fbs/value_index.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/operator.hpp"
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

TEST(bool) {
  auto idx = factory<value_index>::make(type{bool_type{}}, caf::settings{});
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
  auto f = idx->lookup(relational_operator::equal, make_data_view(false));
  CHECK_EQUAL(to_string(unbox(f)), "00101110");
  auto t = idx->lookup(relational_operator::not_equal, make_data_view(false));
  CHECK_EQUAL(to_string(unbox(t)), "11010001");
  auto xs = list{true, false};
  auto multi = unbox(idx->lookup(relational_operator::in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "11111111");
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  value_index_ptr idx2;
  REQUIRE_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  t = idx2->lookup(relational_operator::equal, make_data_view(true));
  CHECK_EQUAL(to_string(unbox(t)), "11010001");
  auto builder = flatbuffers::FlatBufferBuilder{};
  const auto idx_offset = pack(builder, idx);
  builder.Finish(idx_offset);
  auto maybe_fb = flatbuffer<fbs::ValueIndex>::make(builder.Release());
  REQUIRE_NOERROR(maybe_fb);
  auto fb = *maybe_fb;
  REQUIRE(fb);
  auto idx3 = value_index_ptr{};
  REQUIRE_EQUAL(unpack(*fb, idx3), caf::none);
  CHECK_EQUAL(idx->type(), idx3->type());
  CHECK_EQUAL(idx->options(), idx3->options());
  t = idx3->lookup(relational_operator::equal, make_data_view(true));
  CHECK_EQUAL(to_string(unbox(t)), "11010001");
}

TEST(integer) {
  caf::settings opts;
  opts["base"] = "uniform(10, 20)";
  auto idx = factory<value_index>::make(type{integer_type{}}, std::move(opts));
  REQUIRE_NOT_EQUAL(idx, nullptr);
  MESSAGE("append");
  REQUIRE(idx->append(make_data_view(integer{-7})));
  REQUIRE(idx->append(make_data_view(integer{42})));
  REQUIRE(idx->append(make_data_view(integer{10000})));
  REQUIRE(idx->append(make_data_view(integer{4711})));
  REQUIRE(idx->append(make_data_view(integer{31337})));
  REQUIRE(idx->append(make_data_view(integer{42})));
  REQUIRE(idx->append(make_data_view(integer{42})));
  MESSAGE("lookup");
  auto leet
    = idx->lookup(relational_operator::equal, make_data_view(integer{31337}));
  CHECK(to_string(unbox(leet)) == "0000100");
  auto less_than_leet
    = idx->lookup(relational_operator::less, make_data_view(integer{31337}));
  CHECK(to_string(unbox(less_than_leet)) == "1111011");
  auto greater_zero
    = idx->lookup(relational_operator::greater, make_data_view(integer{0}));
  CHECK(to_string(unbox(greater_zero)) == "0111111");
  auto xs = list{integer{42}, integer{10}, integer{4711}};
  auto multi = unbox(idx->lookup(relational_operator::in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "0101011");
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  value_index_ptr idx2;
  REQUIRE_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  less_than_leet
    = idx2->lookup(relational_operator::less, make_data_view(integer{31337}));
  CHECK(to_string(unbox(less_than_leet)) == "1111011");
  auto builder = flatbuffers::FlatBufferBuilder{};
  const auto idx_offset = pack(builder, idx);
  builder.Finish(idx_offset);
  auto maybe_fb = flatbuffer<fbs::ValueIndex>::make(builder.Release());
  REQUIRE_NOERROR(maybe_fb);
  auto fb = *maybe_fb;
  REQUIRE(fb);
  auto idx3 = value_index_ptr{};
  REQUIRE_EQUAL(unpack(*fb, idx3), caf::none);
  CHECK_EQUAL(idx->type(), idx3->type());
  CHECK_EQUAL(idx->options(), idx3->options());
  less_than_leet
    = idx3->lookup(relational_operator::less, make_data_view(integer{31337}));
  CHECK(to_string(unbox(less_than_leet)) == "1111011");
}

// This was the first attempt in figuring out where the bug sat. It didn't fire.
TEST(regression - checking the result single bitmap) {
  ewah_bitmap bm;
  bm.append<0>(680);
  bm.append<1>();   //  681
  bm.append<0>();   //  682
  bm.append<1>();   //  683
  bm.append<0>(36); //  719
  bm.append<1>();   //  720
  bm.append<1>();   //  721
  for (auto i = bm.size(); i < 6464; ++i)
    bm.append<0>();
  CHECK_EQUAL(rank(bm), 4u); // regression had rank 5
  bm.append<0>();
  CHECK_EQUAL(rank(bm), 4u);
  CHECK_EQUAL(bm.size(), 6465u);
}

FIXTURE_SCOPE_END()
