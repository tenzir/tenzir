//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/bitz.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/secret.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/shared_owner.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/storage_fwd.hpp"
#include "tenzir/nova/structured_storage.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/result.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::nova::bitz {
namespace {

constexpr auto packed_bitmap_encoding = std::uint8_t{0};
constexpr auto dense_64_encoding = std::uint8_t{0};
constexpr auto float_64_encoding = std::uint8_t{0};
constexpr auto union_u32_encoding = std::uint8_t{0};
constexpr auto constant_false_encoding = std::uint8_t{1};
constexpr auto constant_true_encoding = std::uint8_t{2};
constexpr auto sparse_true_encoding = std::uint8_t{3};
constexpr auto sparse_false_encoding = std::uint8_t{4};
constexpr auto dense_8_encoding = std::uint8_t{1};
constexpr auto dense_16_encoding = std::uint8_t{2};
constexpr auto dense_32_encoding = std::uint8_t{3};
constexpr auto float_32_encoding = std::uint8_t{1};
constexpr auto union_u8_encoding = std::uint8_t{1};
constexpr auto max_encode_nesting
  = std::size_t{default_decode_limits.max_nesting};

class Writer {
public:
  explicit Writer(EncodeOptions const& options)
    : scalar_byte_order_{options.scalar_byte_order},
      max_array_length_{options.max_array_length} {
  }

  Writer(std::span<std::byte> output, EncodeOptions const& options)
    : output_{output},
      scalar_byte_order_{options.scalar_byte_order},
      max_array_length_{options.max_array_length} {
  }

  template <std::unsigned_integral T>
  auto integer(T value) -> void {
    auto bytes = std::array<std::byte, sizeof(T)>{};
    auto wide = static_cast<std::uint64_t>(value);
    for (auto i = std::size_t{0}; i < sizeof(T); ++i) {
      bytes[i] = std::byte{static_cast<std::uint8_t>((wide >> (i * 8)) & 0xff)};
    }
    this->bytes(bytes);
  }

  auto bytes(std::span<std::byte const> value) -> void {
    if (value.size() > std::numeric_limits<std::size_t>::max() - position_) {
      overflow_ = true;
      return;
    }
    if (not output_.empty()) {
      TENZIR_ASSERT_LEQ(position_ + value.size(), output_.size());
      std::memcpy(output_.data() + position_, value.data(), value.size());
    }
    position_ += value.size();
  }

  template <std::unsigned_integral T>
  auto scalar(T bits) -> void {
    auto bytes = std::array<std::byte, sizeof(T)>{};
    for (auto i = std::size_t{0}; i < bytes.size(); ++i) {
      auto const offset = scalar_byte_order_ == ScalarByteOrder::little
                            ? i
                            : bytes.size() - i - 1;
      bytes[offset]
        = std::byte{static_cast<std::uint8_t>((bits >> (i * 8)) & 0xff)};
    }
    this->bytes(bytes);
  }

  auto string(std::string_view value) -> void {
    integer(static_cast<std::uint32_t>(value.size()));
    bytes(std::as_bytes(std::span{value}));
  }

  auto scalar_byte_order() const -> ScalarByteOrder {
    return scalar_byte_order_;
  }

  auto max_array_length() const -> std::uint32_t {
    return max_array_length_;
  }

  auto size() const -> std::size_t {
    return position_;
  }

  auto valid() const -> bool {
    return not overflow_;
  }

private:
  std::span<std::byte> output_;
  ScalarByteOrder scalar_byte_order_;
  std::uint32_t max_array_length_;
  std::size_t position_ = 0;
  bool overflow_ = false;
};

class Reader {
public:
  explicit Reader(std::span<std::byte const> bytes, DecodeLimits const& limits)
    : bytes_{bytes}, limits_{limits} {
  }

  template <std::unsigned_integral T>
  auto integer() -> Result<T, std::string> {
    if (bytes_.size() < sizeof(T)) {
      return Err{"truncated integer"};
    }
    auto result = T{0};
    for (auto i = std::size_t{0}; i < sizeof(T); ++i) {
      result
        |= static_cast<T>(std::to_integer<std::uint8_t>(bytes_[i])) << (i * 8);
    }
    bytes_ = bytes_.subspan(sizeof(T));
    return result;
  }

  auto bytes(std::size_t size)
    -> Result<std::span<std::byte const>, std::string> {
    if (size > bytes_.size()) {
      return Err{"truncated byte sequence"};
    }
    auto result = bytes_.first(size);
    bytes_ = bytes_.subspan(size);
    return result;
  }

  template <std::unsigned_integral T>
  auto scalar() -> Result<T, std::string> {
    TRY(auto raw, bytes(sizeof(T)));
    auto result = T{0};
    for (auto i = std::size_t{0}; i < raw.size(); ++i) {
      auto const offset = scalar_byte_order_ == ScalarByteOrder::little
                            ? i
                            : raw.size() - i - 1;
      result |= static_cast<T>(std::to_integer<std::uint8_t>(raw[offset]))
                << (i * 8);
    }
    return result;
  }

  auto set_scalar_byte_order(ScalarByteOrder value) -> void {
    scalar_byte_order_ = value;
  }

  auto scalar_byte_order() const -> ScalarByteOrder {
    return scalar_byte_order_;
  }

  auto field_name() -> Result<std::string, std::string> {
    TRY(auto size, integer<std::uint32_t>());
    if (size > limits_.max_field_name_bytes) {
      return Err{"Bitz record field name exceeds the resource limit"};
    }
    TRY(charge(total_name_bytes_, size, limits_.max_total_name_bytes,
               "Bitz record field names exceed the resource limit"));
    TRY(charge_decoded(size + field_entry_overhead));
    TRY(auto value, bytes(size));
    return std::string{reinterpret_cast<char const*>(value.data()),
                       value.size()};
  }

  auto empty() const -> bool {
    return bytes_.empty();
  }

  auto remaining() const -> std::size_t {
    return bytes_.size();
  }

  static auto checked_index(std::uint32_t raw)
    -> Result<storage::Index, std::string> {
    if (raw > static_cast<std::uint32_t>(
          std::numeric_limits<storage::Index>::max())) {
      return Err{"Bitz index exceeds the supported index range"};
    }
    return static_cast<storage::Index>(raw);
  }

  auto checked_array_length(std::uint32_t raw) const
    -> Result<storage::Index, std::string> {
    TRY(auto result, checked_index(raw));
    if (raw > limits_.max_array_length) {
      return Err{"Bitz array length exceeds the resource limit"};
    }
    return result;
  }

  auto check_rows(std::uint32_t rows) const -> Result<void, std::string> {
    if (rows > limits_.max_rows) {
      return Err{"Bitz row count exceeds the resource limit"};
    }
    return {};
  }

  auto check_depth(std::size_t depth) const -> Result<void, std::string> {
    if (depth > limits_.max_nesting) {
      return Err{"Bitz array exceeds the nesting limit"};
    }
    return {};
  }

  auto enter_array(storage::Index length, std::size_t depth)
    -> Result<void, std::string> {
    TRY(check_depth(depth));
    TRY(charge(array_nodes_, 1, limits_.max_array_nodes,
               "Bitz payload has too many arrays"));
    TRY(charge(logical_slots_, static_cast<std::uint64_t>(length),
               limits_.max_logical_slots,
               "Bitz payload exceeds the logical value limit"));
    return {};
  }

  auto check_field_count(std::uint32_t count) const
    -> Result<storage::Index, std::string> {
    if (count > limits_.max_fields_per_record) {
      return Err{"Bitz record field count exceeds the resource limit"};
    }
    return checked_index(count);
  }

  auto check_shape_count(std::uint32_t count) const
    -> Result<storage::Index, std::string> {
    if (count > limits_.max_shapes_per_record) {
      return Err{"Bitz record shape count exceeds the resource limit"};
    }
    return checked_index(count);
  }

  auto charge_shape_entries(std::uint32_t count) -> Result<void, std::string> {
    TRY(charge(shape_entries_, count, limits_.max_shape_entries,
               "Bitz record shapes exceed the resource limit"));
    return charge_decoded(
      (static_cast<std::uint64_t>(count) * sizeof(storage::Index))
      + shape_entry_overhead);
  }

  auto charge_decoded(std::uint64_t bytes) -> Result<void, std::string> {
    return charge(decoded_bytes_, bytes, limits_.max_decoded_bytes,
                  "Bitz decoded data exceeds the memory limit");
  }

private:
  static constexpr auto field_entry_overhead = std::uint64_t{128};
  static constexpr auto shape_entry_overhead = std::uint64_t{256};

  static auto charge(std::uint64_t& current, std::uint64_t amount,
                     std::uint64_t limit, char const* message)
    -> Result<void, std::string> {
    if (amount > limit or current > limit - amount) {
      return Err{message};
    }
    current += amount;
    return {};
  }

  std::span<std::byte const> bytes_;
  DecodeLimits const& limits_;
  ScalarByteOrder scalar_byte_order_ = ScalarByteOrder::little;
  std::uint64_t array_nodes_ = 0;
  std::uint64_t logical_slots_ = 0;
  std::uint64_t shape_entries_ = 0;
  std::uint64_t total_name_bytes_ = 0;
  std::uint64_t decoded_bytes_ = 0;
};

auto bitmap_encoding(storage::BitMap const& bitmap) -> std::uint8_t {
  auto const true_count = static_cast<std::size_t>(bitmap.true_count());
  auto const length = static_cast<std::size_t>(bitmap.length());
  if (true_count == 0) {
    return constant_false_encoding;
  }
  if (true_count == length) {
    return constant_true_encoding;
  }
  auto const packed_bytes = (length + 7) / 8;
  if (4 + true_count * 4 < packed_bytes) {
    return sparse_true_encoding;
  }
  if (4 + (length - true_count) * 4 < packed_bytes) {
    return sparse_false_encoding;
  }
  return packed_bitmap_encoding;
}

auto write_bitmap_body(Writer& writer, storage::BitMap const& bitmap,
                       std::uint8_t encoding) -> void {
  if (encoding == constant_false_encoding
      or encoding == constant_true_encoding) {
    return;
  }
  if (encoding == sparse_true_encoding or encoding == sparse_false_encoding) {
    auto const exceptions = encoding == sparse_true_encoding
                              ? bitmap.true_count()
                              : bitmap.length() - bitmap.true_count();
    writer.integer(static_cast<std::uint32_t>(exceptions));
    for (auto i = storage::Index{0}; i < bitmap.length(); ++i) {
      if (bitmap.get(i) == (encoding == sparse_true_encoding)) {
        writer.integer(static_cast<std::uint32_t>(i));
      }
    }
    return;
  }
  auto byte = std::uint8_t{0};
  for (auto i = storage::Index{0}; i < bitmap.length(); ++i) {
    if (bitmap.get(i)) {
      byte |= static_cast<std::uint8_t>(1u << (i % 8));
    }
    if (i % 8 == 7) {
      writer.integer(byte);
      byte = 0;
    }
  }
  if (bitmap.length() % 8 != 0) {
    writer.integer(byte);
  }
}

auto write_bitmap(Writer& writer, storage::BitMap const& bitmap) -> void {
  auto const encoding = bitmap_encoding(bitmap);
  writer.integer(encoding);
  write_bitmap_body(writer, bitmap, encoding);
}

static auto
read_bitmap_body(Reader& reader, storage::Index length, std::uint8_t encoding)
  -> Result<storage::BitMap, std::string> {
  if (encoding == constant_false_encoding
      or encoding == constant_true_encoding) {
    return storage::BitMap{length, encoding == constant_true_encoding};
  }
  if (encoding == sparse_true_encoding or encoding == sparse_false_encoding) {
    TRY(auto count, reader.integer<std::uint32_t>());
    if (count > static_cast<std::uint32_t>(length)
        or count > reader.remaining() / sizeof(std::uint32_t)) {
      return Err{"invalid Bitz sparse bitmap count"};
    }
    TRY(reader.charge_decoded(((static_cast<std::uint64_t>(length) + 7) / 8)
                              + 64));
    auto bitmap = storage::BitMap::Mutable{
      storage::BitMap{length, encoding == sparse_false_encoding}};
    auto previous = storage::Index{-1};
    for (auto i = std::uint32_t{0}; i < count; ++i) {
      TRY(auto raw, reader.integer<std::uint32_t>());
      if (raw >= static_cast<std::uint32_t>(length)
          or (previous >= 0 and raw <= static_cast<std::uint32_t>(previous))) {
        return Err{"invalid Bitz sparse bitmap position"};
      }
      auto const position = static_cast<storage::Index>(raw);
      bitmap.set(position, encoding == sparse_true_encoding);
      previous = position;
    }
    return std::move(bitmap).finish();
  }
  if (encoding != packed_bitmap_encoding) {
    return Err{"unsupported Bitz bitmap encoding"};
  }
  auto byte_count = (static_cast<std::size_t>(length) + 7) / 8;
  if (byte_count > reader.remaining()) {
    return Err{"truncated bitmap"};
  }
  TRY(reader.charge_decoded(byte_count + 64));
  auto builder = storage::BitMap::Builder{};
  auto byte = std::uint8_t{0};
  for (auto i = storage::Index{0}; i < length; ++i) {
    if (i % 8 == 0) {
      TRY(auto next, reader.integer<std::uint8_t>());
      byte = next;
    }
    builder.emplace_back((byte & (1u << (i % 8))) != 0);
  }
  return builder.finish();
}

auto read_bitmap(Reader& reader, storage::Index length)
  -> Result<storage::BitMap, std::string> {
  TRY(auto encoding, reader.integer<std::uint8_t>());
  return read_bitmap_body(reader, length, encoding);
}

static auto
make_shape_membership(ShapeTable const& table, storage::Index field_count)
  -> Result<std::vector<std::vector<storage::Index>>, std::string> {
  auto result = std::vector<std::vector<storage::Index>>{};
  result.reserve(table.size());
  for (auto shape = std::size_t{0}; shape < table.size(); ++shape) {
    auto const fields = table.fields(static_cast<ShapeTable::ShapeId>(shape));
    if (std::ranges::any_of(fields, [field_count](auto field) {
          return field < 0 or field >= field_count;
        })) {
      return Err{"record shape refers to an invalid field"};
    }
    auto& membership = result.emplace_back(fields.begin(), fields.end());
    std::ranges::sort(membership);
  }
  return result;
}

template <class Tag>
constexpr auto type_id = [] {
  if constexpr (std::same_as<Tag, Null> or std::same_as<Tag, Secret>) {
    return TypeId::null;
  } else if constexpr (std::same_as<Tag, Bool>) {
    return TypeId::boolean;
  } else if constexpr (std::same_as<Tag, Int>) {
    return TypeId::integer;
  } else if constexpr (std::same_as<Tag, UInt>) {
    return TypeId::unsigned_integer;
  } else if constexpr (std::same_as<Tag, Float>) {
    return TypeId::floating_point;
  } else if constexpr (std::same_as<Tag, String>) {
    return TypeId::string;
  } else if constexpr (std::same_as<Tag, Blob>) {
    return TypeId::blob;
  } else if constexpr (std::same_as<Tag, Ip>) {
    return TypeId::ip;
  } else if constexpr (std::same_as<Tag, Subnet>) {
    return TypeId::subnet;
  } else if constexpr (std::same_as<Tag, Time>) {
    return TypeId::time;
  } else if constexpr (std::same_as<Tag, Duration>) {
    return TypeId::duration;
  } else if constexpr (std::same_as<Tag, List>) {
    return TypeId::list;
  } else {
    static_assert(std::same_as<Tag, Record>);
    return TypeId::record;
  }
}();

template <class Tag>
concept fixed_width_scalar
  = std::same_as<Tag, Int> or std::same_as<Tag, UInt>
    or std::same_as<Tag, Float> or std::same_as<Tag, Time>
    or std::same_as<Tag, Duration>;

template <class Tag>
concept bulk_copy_scalar = std::same_as<Tag, Int> or std::same_as<Tag, UInt>
                           or std::same_as<Tag, Float>;

template <fixed_width_scalar Tag>
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

template <fixed_width_scalar Tag>
auto scalar_value(std::uint64_t bits) -> typename Type<Tag>::ViewType {
  if constexpr (std::same_as<Tag, UInt>) {
    return bits;
  } else if constexpr (std::same_as<Tag, Time>) {
    return Time{Duration{std::bit_cast<std::int64_t>(bits)}};
  } else if constexpr (std::same_as<Tag, Duration>) {
    return Duration{std::bit_cast<std::int64_t>(bits)};
  } else {
    return std::bit_cast<typename Type<Tag>::ViewType>(bits);
  }
}

auto write_header(Writer& writer, TypeId id, std::uint8_t encoding) -> void {
  writer.integer(static_cast<std::uint8_t>(id));
  writer.integer(encoding);
}

template <class Tag>
  requires(std::same_as<Tag, Int> or std::same_as<Tag, UInt>)
auto integer_encoding(Array<Tag> const& array, storage::BitMap const& visible)
  -> std::uint8_t {
  auto fits = [&](auto width) {
    using Narrow = decltype(width);
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      if (visible.get(i)) {
        auto const value = *array.get(i);
        if (value < std::numeric_limits<Narrow>::min()
            or value > std::numeric_limits<Narrow>::max()) {
          return false;
        }
      }
    }
    return true;
  };
  if (fits(std::conditional_t<std::same_as<Tag, Int>, std::int8_t,
                              std::uint8_t>{})) {
    return dense_8_encoding;
  }
  if (fits(std::conditional_t<std::same_as<Tag, Int>, std::int16_t,
                              std::uint16_t>{})) {
    return dense_16_encoding;
  }
  if (fits(std::conditional_t<std::same_as<Tag, Int>, std::int32_t,
                              std::uint32_t>{})) {
    return dense_32_encoding;
  }
  return dense_64_encoding;
}

auto float_encoding(Array<Float> const& array, storage::BitMap const& visible)
  -> std::uint8_t {
  for (auto i = storage::Index{0}; i < array.length(); ++i) {
    if (visible.get(i)) {
      auto const value = *array.get(i);
      if (std::bit_cast<std::uint64_t>(
            static_cast<double>(static_cast<float>(value)))
          != std::bit_cast<std::uint64_t>(value)) {
        return float_64_encoding;
      }
    }
  }
  return float_32_encoding;
}

auto write_array(Writer& writer, Array<Data> const& array,
                 storage::BitMap const& visible, std::size_t depth)
  -> Result<void, std::string>;

auto write_concrete(Writer&, Array<Secret> const&, storage::BitMap const&,
                    std::size_t) -> Result<void, std::string> {
  return Err{"secrets cannot be serialized by Bitz"};
}

template <data_type Tag>
  requires(not std::same_as<Tag, Secret>)
auto write_concrete(Writer& writer, Array<Tag> const& array,
                    storage::BitMap const& visible, std::size_t depth)
  -> Result<void, std::string> {
  if (depth > max_encode_nesting) {
    return Err{"Bitz array exceeds the nesting limit"};
  }
  if (array.length() != visible.length()) {
    return Err{"Bitz array visibility length mismatch"};
  }
  if (std::cmp_greater(array.length(), writer.max_array_length())) {
    return Err{"Bitz array length exceeds the resource limit"};
  }
  if constexpr (std::same_as<Tag, Secret>) {
    return Err{"Bitz cannot encode secret data"};
  }
  auto encoding = std::uint8_t{0};
  if constexpr (std::same_as<Tag, Bool>) {
    auto values = storage::BitMap::Builder{};
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      values.emplace_back(visible.get(i) ? *array.get(i) : false);
    }
    auto bitmap = values.finish();
    encoding = bitmap_encoding(bitmap);
    write_header(writer, type_id<Tag>, encoding);
    write_bitmap_body(writer, bitmap, encoding);
    return {};
  } else if constexpr (std::same_as<Tag, Int> or std::same_as<Tag, UInt>) {
    encoding = integer_encoding(array, visible);
  } else if constexpr (std::same_as<Tag, Float>) {
    encoding = float_encoding(array, visible);
  }
  write_header(writer, type_id<Tag>, encoding);
  if constexpr (std::same_as<Tag, Null>) {
    return {};
  } else if constexpr (fixed_width_scalar<Tag>) {
    using Value = Type<Tag>::ViewType;
    using Primary = storage::SparseStorage<Value>;
    static_assert(sizeof(Value) == sizeof(std::uint64_t));
    if constexpr (bulk_copy_scalar<Tag>) {
      static_assert(std::is_trivially_copyable_v<Value>);
      if (encoding == dense_64_encoding
          and writer.scalar_byte_order() == native_scalar_byte_order
          and visible.true_count() == visible.length()
          and is<Primary>(array.storage())) {
        auto const& values = as<Primary>(array.storage());
        writer.bytes(std::as_bytes(
          std::span{values.data(), static_cast<std::size_t>(values.length())}));
        return {};
      }
    }
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto bits = std::uint64_t{0};
      if (visible.get(i)) {
        bits = scalar_bits<Tag>(*array.get(i));
      }
      if constexpr (std::same_as<Tag, Float>) {
        if (encoding == float_32_encoding) {
          auto const value
            = visible.get(i) ? static_cast<float>(*array.get(i)) : 0.0f;
          writer.scalar(std::bit_cast<std::uint32_t>(value));
        } else {
          writer.scalar(bits);
        }
      } else if constexpr (std::same_as<Tag, Int> or std::same_as<Tag, UInt>) {
        if (encoding == dense_8_encoding) {
          writer.scalar(static_cast<std::uint8_t>(bits));
        } else if (encoding == dense_16_encoding) {
          writer.scalar(static_cast<std::uint16_t>(bits));
        } else if (encoding == dense_32_encoding) {
          writer.scalar(static_cast<std::uint32_t>(bits));
        } else {
          writer.scalar(bits);
        }
      } else {
        writer.scalar(bits);
      }
    }
  } else if constexpr (std::same_as<Tag, String> or std::same_as<Tag, Blob>) {
    auto byte_count = std::uint32_t{0};
    writer.integer(byte_count);
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      if (visible.get(i)) {
        auto value = *array.get(i);
        if (value.size()
            > std::numeric_limits<std::uint32_t>::max() - byte_count) {
          return Err{"string or blob data exceeds the Bitz size limit"};
        }
        byte_count += static_cast<std::uint32_t>(value.size());
      }
      writer.integer(byte_count);
    }
    writer.integer(byte_count);
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      if (not visible.get(i)) {
        continue;
      }
      auto value = *array.get(i);
      if constexpr (std::same_as<Tag, String>) {
        writer.bytes(std::as_bytes(std::span{value}));
      } else {
        writer.bytes(value);
      }
    }
  } else if constexpr (std::same_as<Tag, Ip>) {
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto value = visible.get(i) ? *array.get(i) : Ip{};
      writer.bytes(as_bytes(value));
    }
  } else if constexpr (std::same_as<Tag, Subnet>) {
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto value = visible.get(i) ? *array.get(i) : Subnet{};
      writer.bytes(as_bytes(value.network()));
      writer.integer(value.length());
    }
  } else if constexpr (std::same_as<Tag, List>) {
    auto primary = array.to_primary();
    auto const& physical = as<storage::ListStorage>(primary.storage());
    auto const& spans = physical.spans();
    auto const child_length = physical.values().length();
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto span = spans[i];
      if (span.begin < 0 or span.end < span.begin or span.end > child_length) {
        if (visible.get(i)) {
          return Err{"invalid visible list span"};
        }
        span = {0, 0};
      }
      writer.integer(static_cast<std::uint32_t>(span.begin));
      writer.integer(static_cast<std::uint32_t>(span.end));
    }
    writer.integer(static_cast<std::uint32_t>(child_length));
    if (child_length == 0) {
      TRY(write_concrete(writer, Array<Null>{storage::NullStorage{0}},
                         storage::BitMap{0, false}, depth + 1));
    } else {
      auto child_visible = storage::BitMap::Mutable{child_length};
      for (auto i = storage::Index{0}; i < array.length(); ++i) {
        if (not visible.get(i)) {
          continue;
        }
        auto const span = spans[i];
        for (auto child = span.begin; child < span.end; ++child) {
          child_visible.set(child, true);
        }
      }
      TRY(write_array(writer, physical.values(),
                      std::move(child_visible).finish(), depth + 1));
    }
  } else if constexpr (std::same_as<Tag, Record>) {
    auto primary = array.to_primary();
    auto const& physical = as<storage::RecordStorage>(primary.storage());
    auto const& data = *physical;
    if (data.names_by_index.size() != data.arrays.size()) {
      return Err{"record field names and arrays have different sizes"};
    }
    auto field_remap
      = std::vector<storage::Index>(data.arrays.size(), storage::Index{-1});
    auto field_count = std::uint32_t{0};
    for (auto index = std::size_t{0}; index < data.names_by_index.size();
         ++index) {
      auto const name = data.names_by_index[index];
      auto const registered = data.names.find(name);
      if (registered == data.names.end() or registered->second != index) {
        continue;
      }
      if (name.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Err{"record field name exceeds the Bitz size limit"};
      }
      field_remap[index] = static_cast<storage::Index>(field_count++);
    }
    writer.integer(field_count);
    for (auto index = std::size_t{0}; index < data.names_by_index.size();
         ++index) {
      if (field_remap[index] >= 0) {
        writer.string(data.names_by_index[index]);
      }
    }
    writer.integer(static_cast<std::uint32_t>(data.shape_table.size()));
    for (auto shape = std::size_t{0}; shape < data.shape_table.size();
         ++shape) {
      auto fields = data.shape_table.fields(static_cast<storage::Index>(shape));
      auto live_count = std::uint32_t{0};
      for (auto field : fields) {
        if (field < 0
            or static_cast<std::size_t>(field) >= data.arrays.size()) {
          return Err{"record shape refers to an invalid field"};
        }
        live_count += field_remap[static_cast<std::size_t>(field)] >= 0;
      }
      writer.integer(live_count);
      for (auto field : fields) {
        auto const mapped = field_remap[static_cast<std::size_t>(field)];
        if (mapped >= 0) {
          writer.integer(static_cast<std::uint32_t>(mapped));
        }
      }
    }
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto shape
        = visible.get(i) ? data.shape_indices.get(i) : ShapeTable::empty_shape;
      if (shape < 0
          or static_cast<std::size_t>(shape) >= data.shape_table.size()) {
        return Err{"visible record row has an invalid shape"};
      }
      writer.integer(static_cast<std::uint32_t>(shape));
    }
    TRY(auto shape_membership,
        make_shape_membership(data.shape_table,
                              static_cast<storage::Index>(data.arrays.size())));
    for (auto field_index = std::size_t{0}; field_index < data.arrays.size();
         ++field_index) {
      if (field_remap[field_index] < 0) {
        continue;
      }
      auto const& field = data.arrays[field_index];
      if (field.data.length() != array.length()
          or field.present.length() != array.length()) {
        return Err{"record field length does not match its record array"};
      }
      auto present = storage::BitMap::Builder{};
      for (auto row = storage::Index{0}; row < array.length(); ++row) {
        auto selected = false;
        if (visible.get(row)) {
          auto const shape = data.shape_indices.get(row);
          selected = std::ranges::binary_search(
                       shape_membership[static_cast<std::size_t>(shape)],
                       static_cast<storage::Index>(field_index))
                     and field.present.get(row);
        }
        present.emplace_back(selected);
      }
      auto present_mask = present.finish();
      write_bitmap(writer, present_mask);
      TRY(write_array(writer, field.data, present_mask, depth + 1));
    }
  }
  return {};
}

auto write_array(Writer& writer, Array<Data> const& array,
                 storage::BitMap const& visible, std::size_t depth)
  -> Result<void, std::string> {
  return match(
    array,
    [&](UnionArray const& union_) -> Result<void, std::string> {
      if (depth > max_encode_nesting) {
        return Err{"Bitz array exceeds the nesting limit"};
      }
      if (union_.length() != visible.length()) {
        return Err{"Bitz union visibility length mismatch"};
      }
      if (std::cmp_greater(union_.length(), writer.max_array_length())) {
        return Err{"Bitz array length exceeds the resource limit"};
      }
      write_header(writer, TypeId::union_, union_u8_encoding);
      auto const& fields = union_.fields();
      if (fields.empty()
          or fields.size() > std::numeric_limits<std::uint32_t>::max()) {
        return Err{"invalid Bitz union alternative count"};
      }
      writer.integer(static_cast<std::uint32_t>(fields.size()));
      for (auto row = storage::Index{0}; row < union_.length(); ++row) {
        auto index = visible.get(row) ? union_.alternative_index_at(row) : 0;
        if (index < 0 or static_cast<std::size_t>(index) >= fields.size()
            or (visible.get(row)
                and not fields[static_cast<std::size_t>(index)].present.get(
                  row))) {
          return Err{"visible union row has an invalid alternative"};
        }
        writer.integer(static_cast<std::uint8_t>(index));
      }
      for (auto field_index = std::size_t{0}; field_index < fields.size();
           ++field_index) {
        auto const& field = fields[field_index];
        if (field.present.length() != union_.length()) {
          return Err{"union alternative mask length mismatch"};
        }
        auto present = storage::BitMap::Builder{};
        for (auto row = storage::Index{0}; row < union_.length(); ++row) {
          present.emplace_back(visible.get(row)
                               and union_.alternative_index_at(row)
                                     == static_cast<storage::Index>(field_index)
                               and field.present.get(row));
        }
        auto present_mask = present.finish();
        TRY(match(field.data, [&](auto const& concrete) {
          return write_concrete(writer, concrete, present_mask, depth + 1);
        }));
      }
      return {};
    },
    [&](auto const& concrete) {
      return write_concrete(writer, concrete, visible, depth);
    });
}

struct DecodedArray {
  Array<Data> data;
  TypeId type;
};

template <class Tag, std::unsigned_integral Bits>
auto read_narrow_scalar(Reader& reader, storage::Index length, TypeId type)
  -> Result<DecodedArray, std::string> {
  using Value = std::conditional_t<
    std::same_as<Tag, Float>, float,
    std::conditional_t<std::same_as<Tag, Int>, std::make_signed_t<Bits>, Bits>>;
  TRY(
    reader.charge_decoded(static_cast<std::uint64_t>(length) * sizeof(Value)));
  auto values = typename storage::SparseStorage<Value>::Mutable{length};
  for (auto i = storage::Index{0}; i < length; ++i) {
    TRY(auto bits, reader.scalar<Bits>());
    values.set(i, std::bit_cast<Value>(bits));
  }
  return DecodedArray{Array<Tag>{std::move(values).finish()}, type};
}

template <fixed_width_scalar Tag>
auto read_dense_scalar(Reader& reader, storage::Index length, TypeId type,
                       std::uint8_t encoding, std::string_view truncated)
  -> Result<DecodedArray, std::string> {
  using Value = Type<Tag>::ViewType;
  static_assert(sizeof(Value) == sizeof(std::uint64_t));
  auto width = std::size_t{8};
  if constexpr (std::same_as<Tag, Int> or std::same_as<Tag, UInt>) {
    if (encoding == dense_8_encoding) {
      width = 1;
    } else if (encoding == dense_16_encoding) {
      width = 2;
    } else if (encoding == dense_32_encoding) {
      width = 4;
    } else if (encoding != dense_64_encoding) {
      return Err{"unsupported Bitz array encoding"};
    }
  } else if constexpr (std::same_as<Tag, Float>) {
    if (encoding == float_32_encoding) {
      width = 4;
    } else if (encoding != float_64_encoding) {
      return Err{"unsupported Bitz array encoding"};
    }
  } else if (encoding != 0) {
    return Err{"unsupported Bitz array encoding"};
  }
  if (static_cast<std::size_t>(length) > reader.remaining() / width) {
    return Err{std::string{truncated}};
  }
  if constexpr (std::same_as<Tag, Int> or std::same_as<Tag, UInt>) {
    if (width == 1) {
      return read_narrow_scalar<Tag, std::uint8_t>(reader, length, type);
    }
    if (width == 2) {
      return read_narrow_scalar<Tag, std::uint16_t>(reader, length, type);
    }
  }
  if constexpr (std::same_as<Tag, Int> or std::same_as<Tag, UInt>
                or std::same_as<Tag, Float>) {
    if (width == 4) {
      return read_narrow_scalar<Tag, std::uint32_t>(reader, length, type);
    }
  }
  TRY(
    reader.charge_decoded(static_cast<std::uint64_t>(length) * sizeof(Value)));
  if (length == 0) {
    return DecodedArray{ArrayBuilder<Tag>{}.finish(), type};
  }
  auto const byte_size = static_cast<std::size_t>(length) * sizeof(Value);
  if constexpr (bulk_copy_scalar<Tag>) {
    static_assert(std::is_trivially_copyable_v<Value>);
    if (reader.scalar_byte_order() == native_scalar_byte_order) {
      TRY(auto bytes, reader.bytes(byte_size));
      auto values = typename storage::SparseStorage<Value>::Mutable{length};
      std::memcpy(values.data(), bytes.data(), byte_size);
      return DecodedArray{Array<Tag>{std::move(values).finish()}, type};
    }
  }
  auto values = typename storage::SparseStorage<Value>::Mutable{length};
  for (auto i = storage::Index{0}; i < length; ++i) {
    TRY(auto bits, reader.scalar<std::uint64_t>());
    values.set(i, scalar_value<Tag>(bits));
  }
  return DecodedArray{Array<Tag>{std::move(values).finish()}, type};
}

auto erased(DecodedArray decoded) -> Result<ErasedArray, std::string> {
  return match(
    std::move(decoded.data),
    [](UnionArray const&) -> Result<ErasedArray, std::string> {
      return Err{"a union alternative cannot itself be a union"};
    },
    [](auto concrete) -> Result<ErasedArray, std::string> {
      return ErasedArray{std::move(concrete)};
    });
}

auto read_array(Reader& reader, storage::Index length,
                storage::BitMap const& visible, std::size_t depth)
  -> Result<DecodedArray, std::string> {
  if (visible.length() != length) {
    return Err{"Bitz array visibility length mismatch"};
  }
  TRY(reader.enter_array(length, depth));
  TRY(auto raw_type, reader.integer<std::uint8_t>());
  TRY(auto encoding, reader.integer<std::uint8_t>());
  auto type = static_cast<TypeId>(raw_type);
  if (type != TypeId::boolean and type != TypeId::integer
      and type != TypeId::unsigned_integer and type != TypeId::floating_point
      and type != TypeId::union_ and type != TypeId::time
      and type != TypeId::duration and encoding != 0) {
    return Err{"unsupported Bitz array encoding"};
  }
  switch (type) {
    case TypeId::null:
      return DecodedArray{Array<Null>{storage::NullStorage{length}}, type};
    case TypeId::boolean: {
      TRY(auto values, read_bitmap_body(reader, length, encoding));
      return DecodedArray{Array<Bool>{std::move(values)}, type};
    }
    case TypeId::integer:
      return read_dense_scalar<Int>(reader, length, type, encoding,
                                    "truncated integer column");
    case TypeId::unsigned_integer:
      return read_dense_scalar<UInt>(reader, length, type, encoding,
                                     "truncated unsigned integer column");
    case TypeId::floating_point:
      return read_dense_scalar<Float>(reader, length, type, encoding,
                                      "truncated floating-point column");
    case TypeId::string:
    case TypeId::blob: {
      auto offset_count = static_cast<std::size_t>(length) + 1;
      if (offset_count > reader.remaining() / 4) {
        return Err{"truncated string or blob offsets"};
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(offset_count)
                                * sizeof(std::uint32_t)));
      auto offsets = std::vector<std::uint32_t>{};
      offsets.reserve(static_cast<std::size_t>(length) + 1);
      for (auto i = storage::Index{0}; i <= length; ++i) {
        TRY(auto offset, reader.integer<std::uint32_t>());
        if (not offsets.empty() and offset < offsets.back()) {
          return Err{"Bitz byte offsets are not ordered"};
        }
        offsets.push_back(offset);
      }
      TRY(auto byte_count, reader.integer<std::uint32_t>());
      if (offsets.back() != byte_count) {
        return Err{"Bitz byte offsets do not cover the byte buffer"};
      }
      TRY(reader.charge_decoded(
        (static_cast<std::uint64_t>(length) * sizeof(storage::Span))
        + byte_count));
      TRY(auto bytes, reader.bytes(byte_count));
      if (type == TypeId::string) {
        auto builder = ArrayBuilder<String>{};
        for (auto i = storage::Index{0}; i < length; ++i) {
          auto begin = offsets[static_cast<std::size_t>(i)];
          auto end = offsets[static_cast<std::size_t>(i) + 1];
          builder.data(std::string_view{
            reinterpret_cast<char const*>(bytes.data() + begin), end - begin});
        }
        return DecodedArray{builder.finish(), type};
      }
      auto builder = ArrayBuilder<Blob>{};
      for (auto i = storage::Index{0}; i < length; ++i) {
        auto begin = offsets[static_cast<std::size_t>(i)];
        auto end = offsets[static_cast<std::size_t>(i) + 1];
        builder.data(BlobView{bytes.begin() + begin, bytes.begin() + end});
      }
      return DecodedArray{builder.finish(), type};
    }
    case TypeId::ip: {
      if (static_cast<std::size_t>(length) > reader.remaining() / 16) {
        return Err{"truncated IP column"};
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(length) * 16));
      auto builder = ArrayBuilder<Ip>{};
      for (auto i = storage::Index{0}; i < length; ++i) {
        TRY(auto raw, reader.bytes(16));
        auto value = std::array<std::byte, 16>{};
        std::memcpy(value.data(), raw.data(), value.size());
        builder.data(Ip{value});
      }
      return DecodedArray{builder.finish(), type};
    }
    case TypeId::subnet: {
      if (static_cast<std::size_t>(length) > reader.remaining() / 17) {
        return Err{"truncated subnet column"};
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(length) * 17));
      auto builder = ArrayBuilder<Subnet>{};
      for (auto i = storage::Index{0}; i < length; ++i) {
        TRY(auto raw, reader.bytes(16));
        TRY(auto prefix, reader.integer<std::uint8_t>());
        if (prefix > 128) {
          return Err{"invalid subnet length"};
        }
        auto value = std::array<std::byte, 16>{};
        std::memcpy(value.data(), raw.data(), value.size());
        builder.data(Subnet{Ip{value}, prefix});
      }
      return DecodedArray{builder.finish(), type};
    }
    case TypeId::time:
      return read_dense_scalar<Time>(reader, length, type, encoding,
                                     "truncated time column");
    case TypeId::duration:
      return read_dense_scalar<Duration>(reader, length, type, encoding,
                                         "truncated duration column");
    case TypeId::list: {
      if (static_cast<std::size_t>(length) > reader.remaining() / 8) {
        return Err{"truncated list spans"};
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(length)
                                * sizeof(storage::Span)));
      auto spans = storage::DataOwner<storage::Span[]>::Builder{};
      for (auto i = storage::Index{0}; i < length; ++i) {
        TRY(auto begin, reader.integer<std::uint32_t>());
        TRY(auto end, reader.integer<std::uint32_t>());
        TRY(auto checked_begin, reader.checked_index(begin));
        TRY(auto checked_end, reader.checked_index(end));
        if (checked_end < checked_begin) {
          return Err{"invalid Bitz list span"};
        }
        spans.emplace_back(storage::Span{checked_begin, checked_end});
      }
      TRY(auto raw_child_length, reader.integer<std::uint32_t>());
      TRY(auto child_length, reader.checked_array_length(raw_child_length));
      auto owned_spans = spans.finish();
      auto child_visible = storage::BitMap::Builder{};
      auto ranges = std::vector<storage::Span>{};
      ranges.reserve(static_cast<std::size_t>(visible.true_count()));
      for (auto row = storage::Index{0}; row < length; ++row) {
        auto const span = owned_spans[row];
        if (span.end > child_length) {
          return Err{"Bitz list span exceeds its child array"};
        }
        if (visible.get(row)) {
          ranges.push_back(span);
        }
      }
      std::ranges::sort(ranges, {}, &storage::Span::begin);
      TRY(reader.charge_decoded(
        (static_cast<std::uint64_t>(ranges.size()) * sizeof(storage::Span))
        + ((static_cast<std::uint64_t>(child_length) + 7) / 8) + 64));
      auto covered_until = storage::Index{0};
      for (auto const span : ranges) {
        child_visible.append_n(false, std::max(span.begin - covered_until, 0));
        child_visible.append_n(
          true, std::max(span.end - std::max(span.begin, covered_until), 0));
        covered_until = std::max(covered_until, span.end);
      }
      child_visible.append_n(false, child_length - covered_until);
      TRY(auto child,
          read_array(reader, child_length, child_visible.finish(), depth + 1));
      return DecodedArray{
        Array<List>{std::move(owned_spans), std::move(child.data)}, type};
    }
    case TypeId::record: {
      TRY(auto raw_field_count, reader.integer<std::uint32_t>());
      TRY(auto field_count, reader.check_field_count(raw_field_count));
      if (static_cast<std::size_t>(field_count) > reader.remaining() / 4) {
        return Err{"truncated record field directory"};
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(field_count) * 64));
      auto names = Array<Record>::Names{};
      for (auto i = storage::Index{0}; i < field_count; ++i) {
        TRY(auto name, reader.field_name());
        auto [position, inserted] = names.try_emplace(
          storage::String<>{name}, static_cast<std::size_t>(i));
        static_cast<void>(position);
        if (not inserted) {
          return Err{"duplicate Bitz record field name"};
        }
      }
      TRY(auto raw_shape_count, reader.integer<std::uint32_t>());
      TRY(auto shape_count, reader.check_shape_count(raw_shape_count));
      if (shape_count == 0) {
        return Err{"Bitz record has no empty shape"};
      }
      if (static_cast<std::size_t>(shape_count) > reader.remaining() / 4) {
        return Err{"truncated record shape dictionary"};
      }
      TRY(reader.charge_decoded(
        (static_cast<std::uint64_t>(shape_count) * sizeof(ShapeTable::ShapeId))
        + (static_cast<std::uint64_t>(field_count) * sizeof(std::uint32_t))));
      auto table = ShapeTable{};
      auto shape_remap = std::vector<ShapeTable::ShapeId>{};
      shape_remap.reserve(static_cast<std::size_t>(shape_count));
      auto seen_generation
        = std::vector<std::uint32_t>(static_cast<std::size_t>(field_count));
      for (auto shape = storage::Index{0}; shape < shape_count; ++shape) {
        TRY(auto raw_size, reader.integer<std::uint32_t>());
        TRY(auto size, reader.checked_index(raw_size));
        if (size > field_count
            or static_cast<std::size_t>(size) > reader.remaining() / 4) {
          return Err{"invalid Bitz record shape size"};
        }
        TRY(reader.charge_shape_entries(raw_size));
        auto const generation = static_cast<std::uint32_t>(shape) + 1;
        auto fields = std::vector<storage::Index>{};
        fields.reserve(static_cast<std::size_t>(size));
        for (auto i = storage::Index{0}; i < size; ++i) {
          TRY(auto raw_field, reader.integer<std::uint32_t>());
          TRY(auto field, reader.checked_index(raw_field));
          if (field >= field_count) {
            return Err{"Bitz record shape refers to an invalid field"};
          }
          auto& seen = seen_generation[static_cast<std::size_t>(field)];
          if (seen == generation) {
            return Err{"Bitz record shape contains a duplicate field"};
          }
          seen = generation;
          fields.push_back(field);
        }
        auto const id = table.with_fields(fields);
        if (shape == 0 and id != ShapeTable::empty_shape) {
          return Err{"Bitz record shape zero is not empty"};
        }
        shape_remap.push_back(id);
      }
      if (static_cast<std::size_t>(length) > reader.remaining() / 4) {
        return Err{"truncated record shape IDs"};
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(length)
                                * sizeof(storage::Index)));
      auto indices = storage::SparseStorage<storage::Index>{
        storage::DataOwner<storage::Index[]>{}};
      if (length > 0) {
        auto mutable_indices
          = storage::SparseStorage<storage::Index>::Mutable{length};
        for (auto row = storage::Index{0}; row < length; ++row) {
          TRY(auto raw_shape, reader.integer<std::uint32_t>());
          if (raw_shape >= shape_remap.size()) {
            return Err{"Bitz record row has an invalid shape"};
          }
          mutable_indices.set(row, shape_remap[raw_shape]);
        }
        indices = std::move(mutable_indices).finish();
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(field_count) * 64));
      auto fields = Array<Record>::MaskedArrays{};
      fields.reserve(static_cast<std::size_t>(field_count));
      for (auto i = storage::Index{0}; i < field_count; ++i) {
        TRY(auto present, read_bitmap(reader, length));
        TRY(auto field,
            read_array(reader, length, visible & present, depth + 1));
        fields.push_back({std::move(field.data), std::move(present)});
      }
      return DecodedArray{Array<Record>{std::move(indices), std::move(table),
                                        std::move(names), std::move(fields)},
                          type};
    }
    case TypeId::union_: {
      if (encoding != union_u32_encoding and encoding != union_u8_encoding) {
        return Err{"unsupported Bitz array encoding"};
      }
      TRY(auto raw_count, reader.integer<std::uint32_t>());
      TRY(auto count, reader.checked_index(raw_count));
      if (count < 2 or std::cmp_greater(count, data_type_list::size)) {
        return Err{"invalid Bitz union alternative count"};
      }
      auto const tag_width = encoding == union_u8_encoding ? 1u : 4u;
      if (static_cast<std::size_t>(length) > reader.remaining() / tag_width
          or static_cast<std::size_t>(count) > reader.remaining()) {
        return Err{"truncated Bitz union"};
      }
      TRY(reader.charge_decoded(
        (static_cast<std::uint64_t>(length) * sizeof(storage::Index))
        + (static_cast<std::uint64_t>(count) * 64)));
      auto indices = storage::SparseStorage<storage::Index>{
        storage::DataOwner<storage::Index[]>{}};
      if (length > 0) {
        auto mutable_indices
          = storage::SparseStorage<storage::Index>::Mutable{length};
        for (auto row = storage::Index{0}; row < length; ++row) {
          auto raw_index = std::uint32_t{0};
          if (encoding == union_u8_encoding) {
            TRY(auto tag, reader.integer<std::uint8_t>());
            raw_index = tag;
          } else {
            TRY(auto tag, reader.integer<std::uint32_t>());
            raw_index = tag;
          }
          if (raw_index >= raw_count
              or (not visible.get(row) and raw_index != 0)) {
            return Err{"Bitz union row has an invalid alternative"};
          }
          mutable_indices.set(row, static_cast<storage::Index>(raw_index));
        }
        indices = std::move(mutable_indices).finish();
      }
      auto alternatives = storage::Vector<UnionArray::MaskedArray>{};
      alternatives.reserve(static_cast<std::size_t>(count));
      auto seen_types = std::array<bool, data_type_list::size>{};
      for (auto i = storage::Index{0}; i < count; ++i) {
        TRY(reader.charge_decoded(((static_cast<std::uint64_t>(length) + 7) / 8)
                                  + 64));
        auto present_builder = storage::BitMap::Builder{};
        for (auto row = storage::Index{0}; row < length; ++row) {
          present_builder.emplace_back(visible.get(row)
                                       and indices.get(row) == i);
        }
        auto present = present_builder.finish();
        TRY(auto alternative, read_array(reader, length, present, depth + 1));
        if (alternative.type == TypeId::union_) {
          return Err{"invalid Bitz union alternative"};
        }
        auto type_index = static_cast<std::size_t>(alternative.type);
        if (type_index >= seen_types.size() or seen_types[type_index]) {
          return Err{"duplicate Bitz union alternative type"};
        }
        seen_types[type_index] = true;
        TRY(auto data, erased(std::move(alternative)));
        alternatives.push_back({std::move(data), std::move(present)});
      }
      return DecodedArray{
        Array<Data>{UnionArray{std::move(indices), std::move(alternatives)}},
        type};
    }
  }
  return Err{"unknown Bitz logical type identifier"};
}

static auto
validate_visibility(Reader& reader, Array<Data> const& array,
                    storage::BitMap const& visible, std::size_t depth)
  -> Result<void, std::string> {
  if (array.length() != visible.length()) {
    return Err{"invalid Bitz array visibility"};
  }
  TRY(reader.check_depth(depth));
  return match(
    array,
    [&](UnionArray const& union_) -> Result<void, std::string> {
      TRY(reader.charge_decoded(
        ((static_cast<std::uint64_t>(union_.length()) + 7) / 8) + 64));
      auto covered = storage::BitMap{union_.length(), false};
      for (auto const& field : union_.fields()) {
        if (field.present.length() != union_.length()
            or field.present.and_not(visible).any()
            or (covered & field.present).any()) {
          return Err{"invalid Bitz union alternative mask"};
        }
        covered = std::move(covered) | field.present;
      }
      for (auto row = storage::Index{0}; row < union_.length(); ++row) {
        if (not visible.get(row)) {
          continue;
        }
        auto index = union_.alternative_index_at(row);
        if (index < 0
            or static_cast<std::size_t>(index) >= union_.fields().size()
            or not union_.fields()[static_cast<std::size_t>(index)].present.get(
              row)) {
          return Err{"Bitz union index does not match its masks"};
        }
      }
      for (auto const& field : union_.fields()) {
        TRY(match(field.data, [&](auto const& concrete) {
          return validate_visibility(reader, Array<Data>{concrete},
                                     field.present, depth + 1);
        }));
      }
      return {};
    },
    [&](Array<List> const& lists) -> Result<void, std::string> {
      auto primary = lists.to_primary();
      auto const& physical = as<storage::ListStorage>(primary.storage());
      // Null children carry no nested validity state. In particular, avoid
      // expanding their compact logical length into a visibility bitmap for
      // every list field in an untrusted record.
      if (physical.values().try_as<Null>()) {
        return {};
      }
      TRY(reader.charge_decoded(static_cast<std::uint64_t>(visible.true_count())
                                * sizeof(storage::Span)));
      auto visible_spans = std::vector<storage::Span>{};
      visible_spans.reserve(static_cast<std::size_t>(visible.true_count()));
      for (auto row = storage::Index{0}; row < lists.length(); ++row) {
        if (visible.get(row)) {
          visible_spans.push_back(physical.spans()[row]);
        }
      }
      std::ranges::sort(visible_spans, {}, &storage::Span::begin);
      if (physical.values().length() == 0) {
        return validate_visibility(reader, physical.values(),
                                   storage::BitMap{0, false}, depth + 1);
      }
      TRY(reader.charge_decoded(
        ((static_cast<std::uint64_t>(physical.values().length()) + 7) / 8)
        + 64));
      auto child_visible = storage::BitMap::Mutable{physical.values().length()};
      auto covered_until = storage::Index{0};
      for (auto const span : visible_spans) {
        for (auto child = std::max(span.begin, covered_until); child < span.end;
             ++child) {
          child_visible.set(child, true);
        }
        covered_until = std::max(covered_until, span.end);
      }
      return validate_visibility(reader, physical.values(),
                                 std::move(child_visible).finish(), depth + 1);
    },
    [&](Array<Record> const& records) -> Result<void, std::string> {
      auto primary = records.to_primary();
      auto const& physical = *as<storage::RecordStorage>(primary.storage());
      auto membership_bytes = std::uint64_t{0};
      for (auto shape = std::size_t{0}; shape < physical.shape_table.size();
           ++shape) {
        membership_bytes += physical.shape_table
                              .fields(static_cast<ShapeTable::ShapeId>(shape))
                              .size()
                            * sizeof(storage::Index);
      }
      TRY(reader.charge_decoded(membership_bytes));
      TRY(auto shape_membership,
          make_shape_membership(
            physical.shape_table,
            static_cast<storage::Index>(physical.arrays.size())));
      for (auto field_index = std::size_t{0};
           field_index < physical.arrays.size(); ++field_index) {
        auto const& field = physical.arrays[field_index];
        if (field.present.length() != records.length()
            or field.present.and_not(visible).any()) {
          return Err{"invalid Bitz record field presence mask"};
        }
        for (auto row = storage::Index{0}; row < records.length(); ++row) {
          if (not field.present.get(row)) {
            continue;
          }
          auto const shape = physical.shape_indices.get(row);
          if (shape < 0
              or static_cast<std::size_t>(shape) >= shape_membership.size()
              or not std::ranges::binary_search(
                shape_membership[static_cast<std::size_t>(shape)],
                static_cast<storage::Index>(field_index))) {
            return Err{"Bitz record field is present outside its shape"};
          }
        }
        TRY(validate_visibility(reader, field.data, field.present, depth + 1));
      }
      return {};
    },
    [](auto const&) -> Result<void, std::string> {
      return {};
    });
}

static auto
read_typed_meta(Reader& reader, storage::Index length, TypeId expected)
  -> Result<Array<Data>, std::string> {
  TRY(auto decoded,
      read_array(reader, length, storage::BitMap{length, true}, 0));
  if (decoded.type != expected) {
    return Err{"metadata column has the wrong type"};
  }
  TRY(
    reader.charge_decoded(((static_cast<std::uint64_t>(length) + 7) / 8) + 64));
  TRY(validate_visibility(reader, decoded.data, storage::BitMap{length, true},
                          0));
  return std::move(decoded.data);
}

} // namespace

auto encode(Batch const& batch, EncodeOptions const& options)
  -> Result<std::vector<std::byte>, std::string> {
  if (options.scalar_byte_order != ScalarByteOrder::little
      and options.scalar_byte_order != ScalarByteOrder::big) {
    return Err{"unsupported Bitz scalar byte order"};
  }
  auto length = batch.length();
  if (length < 0 or std::cmp_greater(length, options.max_rows)
      or batch.mask.length() != length or batch.meta.name.length() != length
      or batch.meta.import_time.length() != length
      or batch.meta.internal.length() != length) {
    return Err{"inconsistent Bitz batch column lengths"};
  }
  auto write = [&](Writer& writer) -> Result<void, std::string> {
    writer.integer(static_cast<std::uint8_t>(options.scalar_byte_order));
    writer.integer(static_cast<std::uint32_t>(length));
    write_bitmap(writer, batch.mask);
    TRY(write_array(writer, batch.data, batch.mask, 0));
    TRY(write_concrete(writer, batch.meta.name, batch.mask, 0));
    TRY(write_concrete(writer, batch.meta.import_time, batch.mask, 0));
    TRY(write_concrete(writer, batch.meta.internal, batch.mask, 0));
    if (not writer.valid()) {
      return Err{"Bitz payload size exceeds the platform limit"};
    }
    return {};
  };
  auto sizer = Writer{options};
  TRY(write(sizer));
  auto result = std::vector<std::byte>(sizer.size());
  auto writer = Writer{result, options};
  TRY(write(writer));
  TENZIR_ASSERT_EQ(writer.size(), result.size());
  return result;
}

auto decode(std::span<std::byte const> payload, DecodeLimits const& limits)
  -> Result<Batch, std::string> {
  if (payload.size() > limits.max_frame_bytes) {
    return Err{"Bitz frame exceeds the resource limit"};
  }
  try {
    auto reader = Reader{payload, limits};
    TRY(auto raw_scalar_byte_order, reader.integer<std::uint8_t>());
    switch (raw_scalar_byte_order) {
      case static_cast<std::uint8_t>(ScalarByteOrder::little):
        reader.set_scalar_byte_order(ScalarByteOrder::little);
        break;
      case static_cast<std::uint8_t>(ScalarByteOrder::big):
        reader.set_scalar_byte_order(ScalarByteOrder::big);
        break;
      default:
        return Err{"unsupported Bitz scalar byte order"};
    }
    TRY(auto raw_length, reader.integer<std::uint32_t>());
    TRY(reader.check_rows(raw_length));
    TRY(auto length, reader.checked_array_length(raw_length));
    TRY(auto mask, read_bitmap(reader, length));
    TRY(auto root, read_array(reader, length, mask, 0));
    TRY(validate_visibility(reader, root.data, mask, 0));
    TRY(auto raw_names, read_typed_meta(reader, length, TypeId::string));
    TRY(auto raw_times, read_typed_meta(reader, length, TypeId::time));
    TRY(auto raw_internal, read_typed_meta(reader, length, TypeId::boolean));
    auto names = raw_names.try_as<String>();
    auto times = raw_times.try_as<Time>();
    auto internal = raw_internal.try_as<Bool>();
    if (not names or not times or not internal) {
      return Err{"metadata column has the wrong physical type"};
    }
    if (not reader.empty()) {
      return Err{"trailing bytes after Bitz payload"};
    }
    return Batch{std::move(root.data), std::move(mask),
                 Events::Meta{std::move(*names), std::move(*times),
                              std::move(*internal)}};
  } catch (std::length_error const&) {
    return Err{"Bitz payload requests an invalid allocation"};
  } catch (std::bad_alloc const&) {
    return Err{"insufficient memory while decoding Bitz payload"};
  }
}

} // namespace tenzir::nova::bitz
