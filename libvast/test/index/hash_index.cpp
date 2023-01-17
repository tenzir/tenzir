//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE hash_index

#include "vast/index/hash_index.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/fbs/value_index.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/operator.hpp"
#include "vast/si_literals.hpp"
#include "vast/test/test.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;
using namespace vast::si_literals;

TEST(string) {
  // This one-byte parameterization creates a collision for "foo" and "bar".
  hash_index<1> idx{type{string_type{}}};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view("bar")));
  REQUIRE(idx.append(make_data_view("baz")));
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view(caf::none)));
  REQUIRE(idx.append(make_data_view("bar"), 8));
  REQUIRE(idx.append(make_data_view("foo"), 9));
  REQUIRE(idx.append(make_data_view(caf::none)));
  MESSAGE("lookup");
  auto result = idx.lookup(relational_operator::equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(result)), "10010000010");
  result = idx.lookup(relational_operator::not_equal, make_data_view("foo"));
  REQUIRE(result);
  CHECK_EQUAL(to_string(unbox(result)), "01101000101");
}

TEST(serialization) {
  hash_index<1> x{type{string_type{}}};
  REQUIRE(x.append(make_data_view("foo")));
  REQUIRE(x.append(make_data_view("bar")));
  REQUIRE(x.append(make_data_view("baz")));
  caf::byte_buffer buf;
  REQUIRE(detail::serialize(buf, x));
  hash_index<1> y{type{string_type{}}};
  REQUIRE(detail::legacy_deserialize(buf, y));
  auto result = y.lookup(relational_operator::not_equal, make_data_view("bar"));
  CHECK_EQUAL(to_string(unbox(result)), "101");
  // Cannot append after deserialization.
  CHECK(!y.append(make_data_view("foo")));
}

// The attribute #index=hash selects the hash_index implementation.
TEST(factory construction and parameterization) {
  factory<value_index>::initialize();
  auto t = type{string_type{}, {{"index", "hash"}}};
  caf::settings opts;
  MESSAGE("test cardinality that is a power of 2");
  opts["cardinality"] = 1_Ki;
  auto idx = factory<value_index>::make(t, opts);
  auto ptr3 = dynamic_cast<hash_index<3>*>(idx.get()); // 20 bits in 3 bytes
  CHECK(ptr3 != nullptr);
  CHECK_EQUAL(idx->options().size(), 1u);
  MESSAGE("test cardinality that is not a power of 2");
  opts["cardinality"] = 1_Mi + 7;
  idx = factory<value_index>::make(t, opts);
  auto ptr6 = dynamic_cast<hash_index<6>*>(idx.get()); // 41 bits in 6 bytes
  CHECK(ptr6 != nullptr);
  MESSAGE("no options");
  idx = factory<value_index>::make(t, caf::settings{});
  auto ptr5 = dynamic_cast<hash_index<5>*>(idx.get());
  CHECK(ptr5 != nullptr);
}

TEST(hash index for integer) {
  factory<value_index>::initialize();
  auto t = type{int64_type{}, {{"index", "hash"}}};
  caf::settings opts;
  opts["cardinality"] = 1_Ki;
  auto idx = factory<value_index>::make(t, opts);
  REQUIRE(idx != nullptr);
  auto ptr = dynamic_cast<hash_index<3>*>(idx.get());
  REQUIRE(ptr != nullptr);
  CHECK(idx->append(make_data_view(int64_t{42})));
  CHECK(idx->append(make_data_view(int64_t{43})));
  CHECK(idx->append(make_data_view(int64_t{44})));
  auto result
    = idx->lookup(relational_operator::not_equal, make_data_view(int64_t{42}));
  CHECK_EQUAL(to_string(unbox(result)), "011");
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
  result
    = idx2->lookup(relational_operator::not_equal, make_data_view(int64_t{42}));
  CHECK_EQUAL(to_string(unbox(result)), "011");
}

TEST(hash index for list) {
  factory<value_index>::initialize();
  auto t = type{list_type{ip_type{}}, {{"index", "hash"}}};
  auto idx = factory<value_index>::make(t, caf::settings{});
  REQUIRE(idx != nullptr);
  auto xs = list{int64_t{1}, int64_t{2}, int64_t{3}};
  auto ys = list{int64_t{7}, int64_t{5}, int64_t{4}};
  auto zs = list{int64_t{0}, int64_t{0}, int64_t{0}};
  CHECK(idx->append(make_data_view(xs)));
  CHECK(idx->append(make_data_view(xs)));
  CHECK(idx->append(make_data_view(zs)));
  auto result = idx->lookup(relational_operator::equal, make_data_view(zs));
  CHECK_EQUAL(to_string(unbox(result)), "001");
  result = idx->lookup(relational_operator::ni, make_data_view(int64_t{1}));
  REQUIRE(!result);
  CHECK(result.error() == ec::unsupported_operator);
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
  result = idx2->lookup(relational_operator::equal, make_data_view(zs));
  CHECK_EQUAL(to_string(unbox(result)), "001");
  result = idx2->lookup(relational_operator::ni, make_data_view(int64_t{1}));
  REQUIRE(!result);
  CHECK(result.error() == ec::unsupported_operator);
}
