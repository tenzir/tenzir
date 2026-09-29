//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/bitz.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/record_array_builder.hpp"
#include "tenzir/nova/secret.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/storage_fwd.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/variant_traits.hpp"

#include <caf/test/test.hpp>

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

auto mask(std::initializer_list<bool> values) -> storage::BitMap {
  auto result = storage::BitMap::Builder{};
  for (auto value : values) {
    result.emplace_back(value);
  }
  return result.finish();
}

auto meta(storage::Index length) -> Events::Meta {
  auto names = ArrayBuilder<String>{};
  auto times = ArrayBuilder<Time>{};
  auto internal = ArrayBuilder<Bool>{};
  for (auto i = storage::Index{0}; i < length; ++i) {
    names.data(i == 0 ? "first" : "second");
    times.data(Time{Duration{i + 10}});
    internal.data(i % 2 != 0);
  }
  return {names.finish(), times.finish(), internal.finish()};
}

auto bytes(std::uint8_t last) -> std::array<std::byte, 16> {
  auto result = std::array<std::byte, 16>{};
  result.back() = std::byte{last};
  return result;
}

template <std::unsigned_integral T>
auto append_integer(std::vector<std::byte>& bytes, T value) -> void {
  for (auto i = std::size_t{0}; i < sizeof(T); ++i) {
    bytes.push_back(
      std::byte{static_cast<std::uint8_t>((value >> (i * 8)) & 0xff)});
  }
}

class WireReader {
public:
  explicit WireReader(std::span<std::byte const> bytes)
    : bytes_{bytes}, original_size_{bytes.size()} {
  }

  template <std::unsigned_integral T>
  auto integer() -> T {
    REQUIRE_GREATER_EQUAL(bytes_.size(), sizeof(T));
    auto result = T{0};
    for (auto i = std::size_t{0}; i < sizeof(T); ++i) {
      result
        |= static_cast<T>(std::to_integer<std::uint8_t>(bytes_[i])) << (i * 8);
    }
    bytes_ = bytes_.subspan(sizeof(T));
    return result;
  }

  auto scalar(bitz::ScalarByteOrder byte_order, std::size_t width = 8)
    -> std::uint64_t {
    REQUIRE_GREATER_EQUAL(bytes_.size(), width);
    auto result = std::uint64_t{0};
    for (auto i = std::size_t{0}; i < width; ++i) {
      auto const offset
        = byte_order == bitz::ScalarByteOrder::little ? i : width - i - 1;
      result |= static_cast<std::uint64_t>(
                  std::to_integer<std::uint8_t>(bytes_[offset]))
                << (i * 8);
    }
    bytes_ = bytes_.subspan(width);
    return result;
  }

  auto position() const -> std::size_t {
    return original_size_ - bytes_.size();
  }

  auto bitmap(std::size_t length) -> std::uint8_t {
    auto encoding = integer<std::uint8_t>();
    switch (encoding) {
      case 0:
        skip((length + 7) / 8);
        break;
      case 1:
      case 2:
        break;
      case 3:
      case 4: {
        auto count = integer<std::uint32_t>();
        skip(static_cast<std::size_t>(count) * 4);
        break;
      }
      default:
        FAIL("unexpected bitmap encoding");
    }
    return encoding;
  }

  auto string() -> std::string {
    auto size = integer<std::uint32_t>();
    REQUIRE_GREATER_EQUAL(bytes_.size(), size);
    auto result
      = std::string{reinterpret_cast<char const*>(bytes_.data()), size};
    bytes_ = bytes_.subspan(size);
    return result;
  }

  auto skip(std::size_t size) -> void {
    REQUIRE_GREATER_EQUAL(bytes_.size(), size);
    bytes_ = bytes_.subspan(size);
  }

private:
  std::span<std::byte const> bytes_;
  std::size_t original_size_;
};

template <data_type Tag>
auto scalar_bits(typename Type<Tag>::ViewType value) -> std::uint64_t {
  if constexpr (std::same_as<Tag, UInt>) {
    return value;
  } else if constexpr (std::same_as<Tag, Time>) {
    return std::bit_cast<std::uint64_t>(value.time_since_epoch().count());
  } else if constexpr (std::same_as<Tag, Duration>) {
    return std::bit_cast<std::uint64_t>(value.count());
  } else {
    return std::bit_cast<std::uint64_t>(value);
  }
}

template <data_type Tag>
auto check_dense_numeric_roundtrip(
  std::array<typename Type<Tag>::ViewType, 2> values,
  std::array<std::uint64_t, 2> expected, bitz::TypeId type,
  bitz::ScalarByteOrder byte_order) -> void {
  using Value = Type<Tag>::ViewType;
  auto storage = typename storage::SparseStorage<Value>::Mutable{2};
  storage.set(0, values[0]);
  storage.set(1, values[1]);
  auto input = Array<Data>{Array<Tag>{std::move(storage).finish()}};
  auto encoded = bitz::encode(bitz::Batch{std::move(input), mask({true, true}),
                                          Events::Meta::make_empty(2)},
                              {.scalar_byte_order = byte_order});
  REQUIRE(encoded);
  auto wire = WireReader{encoded.unwrap()};
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(byte_order));
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 2u);
  CHECK_EQUAL(wire.bitmap(2), 2u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(), static_cast<std::uint8_t>(type));
  auto const narrow = std::same_as<Tag, Int>;
  CHECK_EQUAL(wire.integer<std::uint8_t>(), narrow ? 1u : 0u);
  CHECK_EQUAL(wire.scalar(byte_order, narrow ? 1 : 8),
              narrow ? expected[0] & 0xff : expected[0]);
  CHECK_EQUAL(wire.scalar(byte_order, narrow ? 1 : 8), expected[1]);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  auto concrete = decoded.unwrap().data.template try_as<Tag>();
  REQUIRE(concrete);
  CHECK_EQUAL(scalar_bits<Tag>(*concrete->get(0)), expected[0]);
  CHECK_EQUAL(scalar_bits<Tag>(*concrete->get(1)), expected[1]);
}

} // namespace

TEST("bitz v2 round-trips all Nova logical types and nested values") {
  auto nested = Record{};
  nested.try_emplace("z", Data{std::int64_t{-7}});
  nested.try_emplace("a", Data{String{"keeps order"}});
  auto values = List{
    Data{Null{}},
    Data{true},
    Data{std::int64_t{-42}},
    Data{std::uint64_t{42}},
    Data{1.25},
    Data{String{"text"}},
    Data{Blob{std::byte{0x12}, std::byte{0x34}}},
    Data{Ip{bytes(1)}},
    Data{Subnet{Ip{bytes(0)}, 64}},
    Data{Time{Duration{123}}},
    Data{Duration{-456}},
    Data{List{Data{String{"nested"}}, Data{std::uint64_t{9}}}},
    Data{std::move(nested)},
  };
  auto builder = ArrayBuilder<Data>{};
  append_data(builder, Data{std::move(values)});
  auto original = builder.finish();
  auto encoded = bitz::encode(
    bitz::Batch{original, mask({true}), Events::Meta::make_empty(1, "root")});
  REQUIRE(encoded);
  // The payload starts with the scalar byte order, batch length, and root
  // array. Array lengths come from their enclosing scope; only the list child
  // has its own length because it differs from the list's length.
  auto wire = WireReader{encoded.unwrap()};
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::native_scalar_byte_order));
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 1u);
  CHECK_EQUAL(wire.bitmap(1), 2u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::list));
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 0u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 0u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 13u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 13u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::union_));
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 1u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 13u);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  CHECK_EQUAL(decoded.unwrap().length(), 1);
  CHECK(decoded.unwrap().mask.get(0));
  CHECK(equal(original.get(0), decoded.unwrap().data.get(0)));
  CHECK_EQUAL(*decoded.unwrap().meta.name.get(0), "root");
}

TEST("bitz v2 derives union masks from tags and enclosing visibility") {
  auto builder = ArrayBuilder<Data>{};
  builder.data(std::int64_t{7});
  builder.data(String{"x"});
  builder.data(std::int64_t{9});
  auto encoded = bitz::encode(bitz::Batch{
    builder.finish(), mask({true, true, false}), Events::Meta::make_empty(3)});
  REQUIRE(encoded);
  auto wire = WireReader{encoded.unwrap()};
  wire.skip(1 + 4);
  CHECK_EQUAL(wire.bitmap(3), 0u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::union_));
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 1u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 2u);
  auto const tags_offset = wire.position();
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 0u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 1u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 0u);
  // The first alternative follows the IDs directly, without a bitmap.
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::integer));
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 1u);
  CHECK_EQUAL(wire.scalar(bitz::native_scalar_byte_order, 1), 7u);
  CHECK_EQUAL(wire.scalar(bitz::native_scalar_byte_order, 1), 0u);
  CHECK_EQUAL(wire.scalar(bitz::native_scalar_byte_order, 1), 0u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::string));
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  auto const& result = decoded.unwrap().data;
  auto ints = result.get_alternative<Int>();
  auto strings = result.get_alternative<String>();
  REQUIRE(ints);
  REQUIRE(strings);
  CHECK(ints->present.get(0));
  CHECK(not ints->present.get(1));
  CHECK(not ints->present.get(2));
  CHECK(not strings->present.get(0));
  CHECK(strings->present.get(1));
  CHECK(not strings->present.get(2));
  CHECK(equal(result.get(0), RowView<Data>{Data{std::int64_t{7}}}));
  CHECK(equal(result.get(1), RowView<Data>{Data{String{"x"}}}));
  auto invalid_visible_tag = encoded.unwrap();
  invalid_visible_tag[tags_offset] = std::byte{2};
  CHECK(not bitz::decode(invalid_visible_tag));
  auto invalid_hidden_tag = encoded.unwrap();
  invalid_hidden_tag[tags_offset + 2] = std::byte{1};
  CHECK(not bitz::decode(invalid_hidden_tag));
  auto truncated_tags = encoded.unwrap();
  truncated_tags.resize(tags_offset + 2);
  CHECK(not bitz::decode(truncated_tags));
  // The decoder also accepts the original fixed-width tag representation.
  auto wide_tags = encoded.unwrap();
  wide_tags[tags_offset - 5] = std::byte{0};
  for (auto row = 3; row-- > 0;) {
    wide_tags.insert(wide_tags.begin() + tags_offset + row + 1, 3,
                     std::byte{0});
  }
  auto wide_decoded = bitz::decode(wide_tags);
  REQUIRE(wide_decoded);
  CHECK(
    equal(wide_decoded.unwrap().data.get(1), RowView<Data>{Data{String{"x"}}}));
}

TEST("bitz v2 selects packed, constant, and sparse bitmap encodings") {
  for (auto byte_order :
       {bitz::ScalarByteOrder::little, bitz::ScalarByteOrder::big}) {
    for (auto [length, true_positions, expected_encoding] : {
           std::tuple{16, std::vector<int>{0, 2, 4, 6, 8, 10, 12, 14}, 0u},
           std::tuple{16, std::vector<int>{}, 1u},
           std::tuple{16,
                      std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                       13, 14, 15},
                      2u},
           std::tuple{128, std::vector<int>{3, 121}, 3u},
           std::tuple{128, std::vector<int>{}, 4u},
         }) {
      auto active = storage::BitMap::Builder{};
      for (auto row = 0; row < length; ++row) {
        auto selected
          = std::ranges::find(true_positions, row) != true_positions.end();
        active.emplace_back(expected_encoding == 4 ? row != 3 and row != 121
                                                   : selected);
      }
      auto encoded = bitz::encode(
        bitz::Batch{Array<Data>{Array<Null>{storage::NullStorage{length}}},
                    active.finish(), Events::Meta::make_empty(length)},
        {.scalar_byte_order = byte_order});
      REQUIRE(encoded);
      auto wire = WireReader{encoded.unwrap()};
      CHECK_EQUAL(wire.integer<std::uint8_t>(),
                  static_cast<std::uint8_t>(byte_order));
      CHECK_EQUAL(wire.integer<std::uint32_t>(),
                  static_cast<std::uint32_t>(length));
      auto const bitmap_offset = wire.position();
      CHECK_EQUAL(wire.integer<std::uint8_t>(), expected_encoding);
      if (expected_encoding == 0) {
        CHECK_EQUAL(wire.integer<std::uint8_t>(), 0x55u);
        CHECK_EQUAL(wire.integer<std::uint8_t>(), 0x55u);
      } else if (expected_encoding >= 3) {
        CHECK_EQUAL(wire.integer<std::uint32_t>(), 2u);
        CHECK_EQUAL(wire.integer<std::uint32_t>(), 3u);
        CHECK_EQUAL(wire.integer<std::uint32_t>(), 121u);
        // Sparse positions and counts are structural, not scalar-endian.
        CHECK_EQUAL(encoded.unwrap()[bitmap_offset + 1], std::byte{2});
      }
      auto decoded = bitz::decode(encoded.unwrap());
      REQUIRE(decoded);
      for (auto row = 0; row < length; ++row) {
        CHECK_EQUAL(decoded.unwrap().mask.get(row),
                    expected_encoding == 4
                      ? row != 3 and row != 121
                      : std::ranges::find(true_positions, row)
                          != true_positions.end());
      }
      if (expected_encoding >= 3) {
        auto malformed = encoded.unwrap();
        // Count and ordered u32 positions follow the encoding byte.
        malformed[bitmap_offset + 5] = std::byte{0xff};
        CHECK(not bitz::decode(malformed));
        malformed = encoded.unwrap();
        malformed[bitmap_offset + 9] = std::byte{3};
        CHECK(not bitz::decode(malformed));
        malformed = encoded.unwrap();
        malformed.erase(malformed.begin() + bitmap_offset + 9, malformed.end());
        CHECK(not bitz::decode(malformed));
      }
    }
  }
}

TEST("bitz v2 encodes boolean columns with bitmap encodings") {
  auto builder = ArrayBuilder<Bool>{};
  for (auto row = 0; row < 128; ++row) {
    builder.data(row == 73);
  }
  auto encoded = bitz::encode(bitz::Batch{Array<Data>{builder.finish()},
                                          storage::BitMap{128, true},
                                          Events::Meta::make_empty(128)});
  REQUIRE(encoded);
  auto wire = WireReader{encoded.unwrap()};
  wire.skip(5);
  CHECK_EQUAL(wire.bitmap(128), 2u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::boolean));
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 3u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 1u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 73u);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  auto booleans = decoded.unwrap().data.try_as<Bool>();
  REQUIRE(booleans);
  for (auto row = 0; row < 128; ++row) {
    CHECK_EQUAL(*booleans->get(row), row == 73);
  }
}

TEST("bitz v2 derives nested union masks from list visibility") {
  auto builder = ArrayBuilder<Data>{};
  append_data(builder, Data{List{Data{std::int64_t{7}}, Data{String{"x"}}}});
  append_data(builder, Data{List{Data{String{"hidden"}}}});
  auto encoded = bitz::encode(bitz::Batch{builder.finish(), mask({true, false}),
                                          Events::Meta::make_empty(2)});
  REQUIRE(encoded);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  auto lists = decoded.unwrap().data.try_as<List>();
  REQUIRE(lists);
  auto visible_row = lists->get(0);
  CHECK_EQUAL(visible_row.length(), 2);
  CHECK(equal(visible_row.get(0), RowView<Data>{Data{std::int64_t{7}}}));
  CHECK(equal(visible_row.get(1), RowView<Data>{Data{String{"x"}}}));
}

TEST("bitz v2 refuses to encode secret data") {
  auto builder = ArrayBuilder<Secret>{};
  builder.data(SecretView{});
  auto encoded = bitz::encode(bitz::Batch{
    Array<Data>{builder.finish()}, mask({true}), Events::Meta::make_empty(1)});
  CHECK(not encoded);
}

TEST("bitz v2 scalar columns preserve both wire byte orders") {
  auto const negative = std::uint64_t{0xffff'ffff'ffff'ff85};
  auto const nan = std::uint64_t{0x7ff8'0000'0000'0042};
  auto const negative_zero = std::uint64_t{0x8000'0000'0000'0000};
  for (auto byte_order :
       {bitz::ScalarByteOrder::little, bitz::ScalarByteOrder::big}) {
    check_dense_numeric_roundtrip<Int>(
      {std::bit_cast<std::int64_t>(negative), std::int64_t{42}}, {negative, 42},
      bitz::TypeId::integer, byte_order);
    check_dense_numeric_roundtrip<UInt>(
      {std::uint64_t{0x0123'4567'89ab'cdef},
       std::uint64_t{0xfedc'ba98'7654'3210}},
      {0x0123'4567'89ab'cdef, 0xfedc'ba98'7654'3210},
      bitz::TypeId::unsigned_integer, byte_order);
    check_dense_numeric_roundtrip<Float>(
      {std::bit_cast<double>(nan), std::bit_cast<double>(negative_zero)},
      {nan, negative_zero}, bitz::TypeId::floating_point, byte_order);
    check_dense_numeric_roundtrip<Time>(
      {Time{Duration{std::bit_cast<std::int64_t>(negative)}},
       Time{Duration{42}}},
      {negative, 42}, bitz::TypeId::time, byte_order);
    check_dense_numeric_roundtrip<Duration>(
      {Duration{std::bit_cast<std::int64_t>(negative)}, Duration{42}},
      {negative, 42}, bitz::TypeId::duration, byte_order);
  }
}

TEST("bitz v2 selects narrow integer encodings in both byte orders") {
  for (auto byte_order :
       {bitz::ScalarByteOrder::little, bitz::ScalarByteOrder::big}) {
    for (auto [values, encoding, width, bits] : {
           std::tuple{std::array<std::int64_t, 2>{-128, 127}, 1u, 1u,
                      std::array<std::uint64_t, 2>{0x80, 0x7f}},
           std::tuple{std::array<std::int64_t, 2>{-129, 32767}, 2u, 2u,
                      std::array<std::uint64_t, 2>{0xff7f, 0x7fff}},
           std::tuple{std::array<std::int64_t, 2>{-32769, 2147483647}, 3u, 4u,
                      std::array<std::uint64_t, 2>{0xffff7fff, 0x7fffffff}},
           std::tuple{std::array<std::int64_t, 2>{-2147483649, 0}, 0u, 8u,
                      std::array<std::uint64_t, 2>{0xffffffff7fffffff, 0}},
         }) {
      auto builder = ArrayBuilder<Int>{};
      for (auto value : values) {
        builder.data(value);
      }
      auto encoded = bitz::encode(bitz::Batch{Array<Data>{builder.finish()},
                                              mask({true, true}),
                                              Events::Meta::make_empty(2)},
                                  {.scalar_byte_order = byte_order});
      REQUIRE(encoded);
      auto wire = WireReader{encoded.unwrap()};
      wire.skip(5);
      CHECK_EQUAL(wire.bitmap(2), 2u);
      CHECK_EQUAL(wire.integer<std::uint8_t>(),
                  static_cast<std::uint8_t>(bitz::TypeId::integer));
      CHECK_EQUAL(wire.integer<std::uint8_t>(), encoding);
      for (auto value : bits) {
        CHECK_EQUAL(wire.scalar(byte_order, width), value);
      }
      auto decoded = bitz::decode(encoded.unwrap());
      REQUIRE(decoded);
      auto ints = decoded.unwrap().data.try_as<Int>();
      REQUIRE(ints);
      for (auto i = std::size_t{0}; i < values.size(); ++i) {
        CHECK_EQUAL(*ints->get(static_cast<storage::Index>(i)), values[i]);
      }
    }
    for (auto [values, encoding, width] : {
           std::tuple{std::array<std::uint64_t, 2>{0, 255}, 1u, 1u},
           std::tuple{std::array<std::uint64_t, 2>{256, 65535}, 2u, 2u},
           std::tuple{std::array<std::uint64_t, 2>{65536, 0xffffffff}, 3u, 4u},
           std::tuple{std::array<std::uint64_t, 2>{0x100000000, 1}, 0u, 8u},
         }) {
      auto builder = ArrayBuilder<UInt>{};
      for (auto value : values) {
        builder.data(value);
      }
      auto encoded = bitz::encode(bitz::Batch{Array<Data>{builder.finish()},
                                              mask({true, true}),
                                              Events::Meta::make_empty(2)},
                                  {.scalar_byte_order = byte_order});
      REQUIRE(encoded);
      auto wire = WireReader{encoded.unwrap()};
      wire.skip(5);
      CHECK_EQUAL(wire.bitmap(2), 2u);
      CHECK_EQUAL(wire.integer<std::uint8_t>(),
                  static_cast<std::uint8_t>(bitz::TypeId::unsigned_integer));
      CHECK_EQUAL(wire.integer<std::uint8_t>(), encoding);
      for (auto value : values) {
        CHECK_EQUAL(wire.scalar(byte_order, width), value);
      }
      auto decoded = bitz::decode(encoded.unwrap());
      REQUIRE(decoded);
      auto ints = decoded.unwrap().data.try_as<UInt>();
      REQUIRE(ints);
      for (auto i = std::size_t{0}; i < values.size(); ++i) {
        CHECK_EQUAL(*ints->get(static_cast<storage::Index>(i)), values[i]);
      }
    }
  }
}

TEST("bitz v2 uses binary32 only for lossless floating-point values") {
  for (auto byte_order :
       {bitz::ScalarByteOrder::little, bitz::ScalarByteOrder::big}) {
    for (auto [values, encoding, width, bits] : {
           std::tuple{std::array{1.5, -2.25}, 1u, 4u,
                      std::array<std::uint64_t, 2>{0x3fc00000, 0xc0100000}},
           std::tuple{std::array{1.1, -0.0}, 0u, 8u,
                      std::array<std::uint64_t, 2>{
                        std::bit_cast<std::uint64_t>(1.1), 0x8000000000000000}},
           std::tuple{std::array{std::bit_cast<double>(
                                   std::uint64_t{0x7ff8000000000042}),
                                 1.5},
                      0u, 8u,
                      std::array<std::uint64_t, 2>{0x7ff8000000000042,
                                                   0x3ff8000000000000}},
         }) {
      auto builder = ArrayBuilder<Float>{};
      for (auto value : values) {
        builder.data(value);
      }
      auto encoded = bitz::encode(bitz::Batch{Array<Data>{builder.finish()},
                                              mask({true, true}),
                                              Events::Meta::make_empty(2)},
                                  {.scalar_byte_order = byte_order});
      REQUIRE(encoded);
      auto wire = WireReader{encoded.unwrap()};
      wire.skip(5);
      CHECK_EQUAL(wire.bitmap(2), 2u);
      CHECK_EQUAL(wire.integer<std::uint8_t>(),
                  static_cast<std::uint8_t>(bitz::TypeId::floating_point));
      CHECK_EQUAL(wire.integer<std::uint8_t>(), encoding);
      for (auto value : bits) {
        CHECK_EQUAL(wire.scalar(byte_order, width), value);
      }
      auto decoded = bitz::decode(encoded.unwrap());
      REQUIRE(decoded);
      auto floats = decoded.unwrap().data.try_as<Float>();
      REQUIRE(floats);
      for (auto i = std::size_t{0}; i < values.size(); ++i) {
        CHECK_EQUAL(std::bit_cast<std::uint64_t>(
                      *floats->get(static_cast<storage::Index>(i))),
                    std::bit_cast<std::uint64_t>(values[i]));
      }
    }
  }
}

TEST("bitz v2 keeps structural values little-endian") {
  auto const value = std::uint64_t{0x0123'4567'89ab'cdef};
  auto const little = std::array{
    std::byte{0xef}, std::byte{0xcd}, std::byte{0xab}, std::byte{0x89},
    std::byte{0x67}, std::byte{0x45}, std::byte{0x23}, std::byte{0x01},
  };
  auto const big = std::array{
    std::byte{0x01}, std::byte{0x23}, std::byte{0x45}, std::byte{0x67},
    std::byte{0x89}, std::byte{0xab}, std::byte{0xcd}, std::byte{0xef},
  };
  for (auto [byte_order, expected] : {
         std::pair{bitz::ScalarByteOrder::little, little},
         std::pair{bitz::ScalarByteOrder::big, big},
       }) {
    auto builder = ArrayBuilder<UInt>{};
    builder.data(value);
    auto encoded
      = bitz::encode(bitz::Batch{Array<Data>{builder.finish()}, mask({true}),
                                 Events::Meta::make_empty(1)},
                     {.scalar_byte_order = byte_order});
    REQUIRE(encoded);
    auto const& payload = encoded.unwrap();
    REQUIRE_GREATER_EQUAL(payload.size(), 17u);
    CHECK_EQUAL(payload[0], std::byte{static_cast<std::uint8_t>(byte_order)});
    CHECK(std::ranges::equal(
      std::span{payload}.subspan(1, 4),
      (std::array{std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0}})));
    auto wire = WireReader{payload};
    wire.skip(5);
    CHECK_EQUAL(wire.bitmap(1), 2u);
    CHECK_EQUAL(wire.integer<std::uint8_t>(),
                static_cast<std::uint8_t>(bitz::TypeId::unsigned_integer));
    CHECK_EQUAL(wire.integer<std::uint8_t>(), 0u);
    CHECK(std::ranges::equal(std::span{payload}.subspan(wire.position(), 8),
                             expected));
  }
}

TEST("bitz v2 sanitizes hidden dense numeric values") {
  for (auto byte_order :
       {bitz::ScalarByteOrder::little, bitz::ScalarByteOrder::big}) {
    auto storage = storage::SparseStorage<std::int64_t>::Mutable{2};
    storage.set(0, 11);
    storage.set(1, 0x1234);
    auto input = Array<Data>{Array<Int>{std::move(storage).finish()}};
    auto encoded
      = bitz::encode(bitz::Batch{std::move(input), mask({true, false}),
                                 Events::Meta::make_empty(2)},
                     {.scalar_byte_order = byte_order});
    REQUIRE(encoded);
    auto wire = WireReader{encoded.unwrap()};
    wire.skip(1 + 4);
    CHECK_EQUAL(wire.bitmap(2), 0u);
    CHECK_EQUAL(wire.integer<std::uint8_t>(),
                static_cast<std::uint8_t>(bitz::TypeId::integer));
    CHECK_EQUAL(wire.integer<std::uint8_t>(), 1u);
    CHECK_EQUAL(wire.scalar(byte_order, 1), 11u);
    CHECK_EQUAL(wire.scalar(byte_order, 1), 0u);
  }
}

TEST("bitz v2 preserves masks and metadata with physical placeholders") {
  auto builder = ArrayBuilder<Data>{};
  builder.data(std::int64_t{11});
  builder.skip();
  auto batch = bitz::Batch{builder.finish(), mask({true, false}), meta(2)};
  auto encoded = bitz::encode(batch);
  REQUIRE(encoded);
  auto wire = WireReader{encoded.unwrap()};
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::native_scalar_byte_order));
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 2u);
  CHECK_EQUAL(wire.bitmap(2), 0u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::integer));
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 1u);
  CHECK_EQUAL(wire.scalar(bitz::native_scalar_byte_order, 1), 11u);
  CHECK_EQUAL(wire.scalar(bitz::native_scalar_byte_order, 1), 0u);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  CHECK(decoded.unwrap().mask.get(0));
  CHECK(not decoded.unwrap().mask.get(1));
  CHECK(match(
    decoded.unwrap().data.get(1),
    [](RowView<std::int64_t> value) {
      return *value == 0;
    },
    [](auto) {
      return false;
    }));
  CHECK_EQUAL(*decoded.unwrap().meta.name.get(0), "first");
  CHECK_EQUAL(*decoded.unwrap().meta.name.get(1), "");
  CHECK_EQUAL(*decoded.unwrap().meta.import_time.get(1), Time{});
  CHECK(not *decoded.unwrap().meta.internal.get(1));
}

TEST("bitz v2 record directory preserves absence and field order") {
  auto builder = ArrayBuilder<Data>{};
  auto first = builder.record();
  first.field("b").data(std::int64_t{1});
  first.field("a").data(std::int64_t{2});
  auto second = builder.record();
  second.field("a").data(std::int64_t{3});
  auto encoded = bitz::encode(bitz::Batch{builder.finish(), mask({true, true}),
                                          Events::Meta::make_empty(2)});
  REQUIRE(encoded);
  auto wire = WireReader{encoded.unwrap()};
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::native_scalar_byte_order));
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 2u);
  CHECK_EQUAL(wire.bitmap(2), 2u);
  CHECK_EQUAL(wire.integer<std::uint8_t>(),
              static_cast<std::uint8_t>(bitz::TypeId::record));
  CHECK_EQUAL(wire.integer<std::uint8_t>(), 0u);
  CHECK_EQUAL(wire.integer<std::uint32_t>(), 2u);
  CHECK_EQUAL(wire.string(), "b");
  CHECK_EQUAL(wire.string(), "a");
  auto shape_count = wire.integer<std::uint32_t>();
  CHECK_GREATER_EQUAL(shape_count, 3u);
  auto shapes = std::vector<std::vector<std::uint32_t>>{};
  for (auto i = std::uint32_t{0}; i < shape_count; ++i) {
    auto size = wire.integer<std::uint32_t>();
    auto& shape = shapes.emplace_back();
    for (auto j = std::uint32_t{0}; j < size; ++j) {
      shape.push_back(wire.integer<std::uint32_t>());
    }
  }
  auto first_shape = wire.integer<std::uint32_t>();
  auto second_shape = wire.integer<std::uint32_t>();
  CHECK_EQUAL(shapes[first_shape], (std::vector<std::uint32_t>{0, 1}));
  CHECK_EQUAL(shapes[second_shape], (std::vector<std::uint32_t>{1}));
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  auto field_names = [](RowView<Data> value) {
    return match(
      value,
      [](RowView<Record> row) {
        auto result = std::vector<std::string_view>{};
        for (auto [name, field] : row) {
          static_cast<void>(field);
          result.push_back(name);
        }
        return result;
      },
      [](auto) {
        return std::vector<std::string_view>{};
      });
  };
  CHECK_EQUAL(field_names(decoded.unwrap().data.get(0)),
              (std::vector<std::string_view>{"b", "a"}));
  CHECK_EQUAL(field_names(decoded.unwrap().data.get(1)),
              (std::vector<std::string_view>{"a"}));
}

TEST("bitz v2 preserves empty-named record fields") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("").data(std::int64_t{1});
  row.field("removed").data(std::int64_t{2});
  auto original = builder.finish();
  auto dropped = std::array<std::string_view, 1>{"removed"};
  auto compacted = std::move(original).without_fields(dropped, mask({true}));
  auto encoded
    = bitz::encode(bitz::Batch{Array<Data>{std::move(compacted)}, mask({true}),
                               Events::Meta::make_empty(1)});
  REQUIRE(encoded);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  auto records = std::move(decoded).unwrap().data.try_as<Record>();
  REQUIRE(records);
  REQUIRE(records->field(""));
  CHECK_EQUAL(*as<RowView<Int>>(records->field("")->data.get(0)), 1);
  CHECK(not records->field("removed"));
}

TEST("bitz v2 compacts fully removed record fields") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").data(std::int64_t{1});
  row.field("b").data(std::int64_t{2});
  row.field("c").data(std::int64_t{3});
  auto original = builder.finish();
  auto dropped = std::array<std::string_view, 2>{"a", "b"};
  auto compacted = std::move(original).without_fields(dropped, mask({true}));
  auto encoded
    = bitz::encode(bitz::Batch{Array<Data>{std::move(compacted)}, mask({true}),
                               Events::Meta::make_empty(1)});
  REQUIRE(encoded);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  auto records = std::move(decoded).unwrap().data.try_as<Record>();
  REQUIRE(records);
  CHECK(not records->field("a"));
  CHECK(not records->field("b"));
  REQUIRE(records->field("c"));
  CHECK_EQUAL(*as<RowView<Int>>(records->field("c")->data.get(0)), 3);
}

TEST("bitz v2 round-trips an empty record batch") {
  auto builder = ArrayBuilder<Record>{};
  auto encoded = bitz::encode(bitz::Batch{
    Array<Data>{builder.finish()}, mask({}), Events::Meta::make_empty(0)});
  REQUIRE(encoded);
  auto decoded = bitz::decode(encoded.unwrap());
  REQUIRE(decoded);
  CHECK_EQUAL(decoded.unwrap().length(), 0);
}

TEST("bitz v2 supports compact null arrays within explicit limits") {
  auto nulls = List{};
  nulls.resize(1'000);
  auto builder = ArrayBuilder<Data>{};
  append_data(builder, Data{std::move(nulls)});
  auto encoded = bitz::encode(
    bitz::Batch{builder.finish(), mask({true}), Events::Meta::make_empty(1)});
  REQUIRE(encoded);
  CHECK(bitz::decode(encoded.unwrap()));
  auto limits = bitz::default_decode_limits;
  limits.max_array_length = 999;
  CHECK(not bitz::decode(encoded.unwrap(), limits));
}

TEST("bitz v2 encoder enforces root array limits") {
  auto const length
    = static_cast<storage::Index>(bitz::default_decode_limits.max_rows + 1);
  auto batch = bitz::Batch{
    Array<Data>{Array<Null>{storage::NullStorage{length}}},
    storage::BitMap{length, false},
    Events::Meta::make_empty(length),
  };
  CHECK(not bitz::encode(batch));
}

TEST("bitz v2 enforces aggregate decoder resource limits") {
  auto builder = ArrayBuilder<Data>{};
  auto row = builder.record();
  row.field("field").data(std::int64_t{42});
  auto encoded = bitz::encode(
    bitz::Batch{builder.finish(), mask({true}), Events::Meta::make_empty(1)});
  REQUIRE(encoded);
  auto rejects = [&](auto configure) {
    auto limits = bitz::default_decode_limits;
    configure(limits);
    CHECK(not bitz::decode(encoded.unwrap(), limits));
  };
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_frame_bytes = 1;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_rows = 0;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_array_nodes = 1;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_fields_per_record = 0;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_shapes_per_record = 1;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_shape_entries = 0;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_field_name_bytes = 4;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_total_name_bytes = 4;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_logical_slots = 1;
  });
  rejects([](bitz::DecodeLimits& limits) {
    limits.max_decoded_bytes = 1;
  });
}

TEST("bitz v2 rejects huge record counts before allocation") {
  auto prefix = [] {
    auto result = std::vector<std::byte>{};
    append_integer(result,
                   static_cast<std::uint8_t>(bitz::native_scalar_byte_order));
    append_integer(result, std::uint32_t{1});
    append_integer(result, std::uint8_t{0});
    append_integer(result, std::uint8_t{1});
    append_integer(result, static_cast<std::uint8_t>(bitz::TypeId::record));
    append_integer(result, std::uint8_t{0});
    return result;
  };
  auto huge_fields = prefix();
  append_integer(huge_fields, std::numeric_limits<std::uint32_t>::max());
  CHECK(not bitz::decode(huge_fields));
  auto huge_shape = prefix();
  append_integer(huge_shape, std::uint32_t{1});
  append_integer(huge_shape, std::uint32_t{1});
  append_integer(huge_shape, std::uint8_t{'x'});
  append_integer(huge_shape, std::uint32_t{1});
  append_integer(huge_shape, std::numeric_limits<std::uint32_t>::max());
  CHECK(not bitz::decode(huge_shape));
}

TEST("bitz v2 enforces the nesting limit") {
  auto nested = Data{Null{}};
  for (auto i = 0; i < 4; ++i) {
    auto list = List{};
    list.push_back(std::move(nested));
    nested = Data{std::move(list)};
  }
  auto builder = ArrayBuilder<Data>{};
  append_data(builder, nested);
  auto encoded = bitz::encode(
    bitz::Batch{builder.finish(), mask({true}), Events::Meta::make_empty(1)});
  REQUIRE(encoded);
  auto limits = bitz::default_decode_limits;
  limits.max_nesting = 2;
  CHECK(not bitz::decode(encoded.unwrap(), limits));
}

TEST("bitz v2 decoder bounds nested array lengths") {
  auto builder = ArrayBuilder<Data>{};
  append_data(builder, Data{List{Data{Null{}}}});
  auto encoded = bitz::encode(
    bitz::Batch{builder.finish(), mask({true}), Events::Meta::make_empty(1)});
  REQUIRE(encoded);
  auto malicious = encoded.unwrap();
  auto wire = WireReader{malicious};
  wire.skip(5);
  wire.bitmap(1);
  wire.skip(2 + 8);
  auto const child_length_offset = wire.position();
  REQUIRE_GREATER_EQUAL(malicious.size(), child_length_offset + 4);
  for (auto i = std::size_t{0}; i < 4; ++i) {
    malicious[child_length_offset + i]
      = std::byte{static_cast<std::uint8_t>(i == 3 ? 0x7f : 0xff)};
  }
  CHECK(not bitz::decode(malicious));
}

TEST("bitz v2 decoder rejects malformed payloads") {
  auto builder = ArrayBuilder<Data>{};
  builder.null();
  auto encoded = bitz::encode(
    bitz::Batch{builder.finish(), mask({true}), Events::Meta::make_empty(1)});
  REQUIRE(encoded);
  auto truncated = encoded.unwrap();
  truncated.pop_back();
  CHECK(not bitz::decode(truncated));
  auto unknown_scalar_byte_order = encoded.unwrap();
  unknown_scalar_byte_order[0] = std::byte{2};
  CHECK(not bitz::decode(unknown_scalar_byte_order));
  auto unknown_bitmap_encoding = encoded.unwrap();
  // The active-row bitmap follows the marker and batch length.
  unknown_bitmap_encoding[5] = std::byte{0xff};
  CHECK(not bitz::decode(unknown_bitmap_encoding));
  auto unknown_array_encoding = encoded.unwrap();
  auto wire = WireReader{unknown_array_encoding};
  wire.skip(5);
  wire.bitmap(1);
  wire.skip(1); // Root type.
  unknown_array_encoding[wire.position()] = std::byte{1};
  CHECK(not bitz::decode(unknown_array_encoding));
  auto trailing = encoded.unwrap();
  trailing.push_back(std::byte{0});
  CHECK(not bitz::decode(trailing));
}
