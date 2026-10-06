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
#include <unordered_map>
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
// Both codec directions account for the resources needed to decode a payload.
class ResourceBudget {
public:
  explicit ResourceBudget(DecodeLimits const& limits) : limits_{limits} {
  }

  static auto checked_index(std::uint32_t raw)
    -> Result<storage::Index, std::string> {
    if (raw > static_cast<std::uint32_t>(
          std::numeric_limits<storage::Index>::max())) {
      return Err{"Bitz index exceeds the supported index range"};
    }
    return static_cast<storage::Index>(raw);
  }

  auto checked_array_length(std::uint32_t raw)
    -> Result<storage::Index, std::string> {
    TRY(auto result, checked_index(raw));
    if (raw > limits_.max_array_length) {
      return limit_error("Bitz array length exceeds the resource limit");
    }
    return result;
  }

  auto check_rows(std::uint32_t rows) -> Result<void, std::string> {
    if (rows > limits_.max_rows) {
      return limit_error("Bitz row count exceeds the resource limit");
    }
    return {};
  }

  auto check_depth(std::size_t depth) -> Result<void, std::string> {
    if (depth > limits_.max_nesting) {
      return limit_error("Bitz array exceeds the nesting limit");
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

  auto check_field_count(std::uint32_t count)
    -> Result<storage::Index, std::string> {
    if (count > limits_.max_fields_per_record) {
      return limit_error("Bitz record field count exceeds the resource limit");
    }
    return checked_index(count);
  }

  auto check_shape_count(std::uint32_t count)
    -> Result<storage::Index, std::string> {
    if (count > limits_.max_shapes_per_record) {
      return limit_error("Bitz record shape count exceeds the resource limit");
    }
    return checked_index(count);
  }

  auto charge_field_name(std::size_t size) -> Result<void, std::string> {
    if (size > limits_.max_field_name_bytes) {
      return limit_error("Bitz record field name exceeds the resource limit");
    }
    TRY(charge(total_name_bytes_, size, limits_.max_total_name_bytes,
               "Bitz record field names exceed the resource limit"));
    return charge_decoded(size + field_entry_overhead);
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

  auto charge_bitmap(storage::Index length) -> Result<void, std::string> {
    return charge_decoded(((static_cast<std::uint64_t>(length) + 7) / 8) + 64);
  }

  auto limit_error(char const* message) -> Err<std::string> {
    limit_exceeded_ = true;
    return Err{std::string{message}};
  }

  auto limit_exceeded() const -> bool {
    return limit_exceeded_;
  }

private:
  bool limit_exceeded_ = false;
  static constexpr auto field_entry_overhead = std::uint64_t{128};
  static constexpr auto shape_entry_overhead = std::uint64_t{256};

  auto charge(std::uint64_t& current, std::uint64_t amount, std::uint64_t limit,
              char const* message) -> Result<void, std::string> {
    if (amount > limit or current > limit - amount) {
      return limit_error(message);
    }
    current += amount;
    return {};
  }

  DecodeLimits limits_;
  std::uint64_t array_nodes_ = 0;
  std::uint64_t logical_slots_ = 0;
  std::uint64_t shape_entries_ = 0;
  std::uint64_t total_name_bytes_ = 0;
  std::uint64_t decoded_bytes_ = 0;
};

class Writer : public ResourceBudget {
public:
  explicit Writer(EncodeOptions const& options)
    : ResourceBudget{DecodeLimits{
        .max_frame_bytes = options.max_frame_bytes,
        .max_rows = options.max_rows,
        .max_array_length = options.max_array_length,
        .max_logical_slots = options.max_logical_slots,
        .max_decoded_bytes = options.max_decoded_bytes,
      }},
      scalar_byte_order_{options.scalar_byte_order},
      max_frame_bytes_{options.max_frame_bytes} {
  }

  /// Reserves an initial output capacity.
  ///
  /// The encoder writes into a growing buffer, so a caller that can estimate
  /// the payload size cheaply saves the reallocations that growth would
  /// otherwise cost. The hint is clamped to the frame limit, because nothing
  /// larger than that can ever be written, and a bad hint costs only the
  /// reallocations it failed to avoid.
  auto reserve(std::uint64_t bytes) -> void {
    buffer_.reserve(
      static_cast<std::size_t>(std::min(bytes, max_frame_bytes_)));
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
    // The frame limit is enforced here, on the way in, rather than on a total
    // computed in advance: a payload that would exceed it stops being written
    // at the limit instead of being allocated and then rejected. The encode
    // still runs to completion so that the caller sees one error for the frame
    // rather than one per array, and `encode_batches` splits the batch and
    // tries again.
    if (position_ + value.size() > max_frame_bytes_) {
      frame_exceeded_ = true;
      return;
    }
    buffer_.insert(buffer_.end(), value.begin(), value.end());
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

  /// Appends `count` copies of `value`, in the same encoding `integer` uses.
  ///
  /// A record column whose rows all carry one shape writes the same shape id
  /// `length` times, and going through `integer` for each of them pays the
  /// buffer's bounds and growth checks once per row. Building a block of copies
  /// and appending those pays it once per block instead.
  auto repeat_integer(std::uint32_t value, storage::Index count) -> void {
    if (count <= 0) {
      return;
    }
    constexpr auto block = storage::Index{64};
    auto unit = std::array<std::byte, sizeof(value)>{};
    for (auto i = std::size_t{0}; i < unit.size(); ++i) {
      unit[i] = std::byte{static_cast<std::uint8_t>((value >> (i * 8)) & 0xff)};
    }
    auto chunk = std::array<std::byte, block * sizeof(value)>{};
    auto const copies = std::min(count, block);
    for (auto i = storage::Index{0}; i < copies; ++i) {
      std::memcpy(chunk.data() + (i * sizeof(value)), unit.data(), unit.size());
    }
    auto remaining = count;
    while (remaining > 0) {
      auto const now = std::min(remaining, block);
      bytes(
        std::span{chunk.data(), static_cast<std::size_t>(now) * sizeof(value)});
      remaining -= now;
    }
  }

  /// Appends `values` in the same encoding `integer` uses, one after another.
  ///
  /// `integer` is unconditionally little-endian, independent of the frame's
  /// scalar byte order, so on a little-endian host a run of `std::uint32_t` is
  /// already the bytes to write and goes out as one copy.
  auto integers(std::span<const std::uint32_t> values) -> void {
    if constexpr (std::endian::native == std::endian::little) {
      bytes(std::as_bytes(values));
    } else {
      for (auto value : values) {
        integer(value);
      }
    }
  }

  auto string(std::string_view value) -> void {
    integer(static_cast<std::uint32_t>(value.size()));
    bytes(std::as_bytes(std::span{value}));
  }

  auto scalar_byte_order() const -> ScalarByteOrder {
    return scalar_byte_order_;
  }

  auto size() const -> std::size_t {
    return position_;
  }

  auto valid() const -> bool {
    return not overflow_ and not frame_exceeded_;
  }

  /// Whether the frame limit, rather than the platform's own size limit, is
  /// what `valid()` is reporting.
  ///
  /// The two are a different contract: a frame over `max_frame_bytes` is a
  /// batch that `encode_batches` can split and retry, while an overflowing
  /// position is not. Callers report them with different messages, and the
  /// caller-visible message for the frame limit predates writing into a growing
  /// buffer.
  auto frame_exceeded() const -> bool {
    return frame_exceeded_;
  }

  /// Takes the encoded payload, leaving the writer empty.
  auto take() && -> std::vector<std::byte> {
    return std::move(buffer_);
  }

private:
  std::vector<std::byte> buffer_;
  ScalarByteOrder scalar_byte_order_;
  std::uint64_t max_frame_bytes_;
  std::size_t position_ = 0;
  bool overflow_ = false;
  bool frame_exceeded_ = false;
};

class Reader : public ResourceBudget {
public:
  explicit Reader(std::span<std::byte const> bytes, DecodeLimits const& limits)
    : ResourceBudget{limits}, bytes_{bytes} {
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
    TRY(charge_field_name(size));
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

private:
  std::span<std::byte const> bytes_;
  ScalarByteOrder scalar_byte_order_ = ScalarByteOrder::little;
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
                       std::uint8_t encoding) -> Result<void, std::string> {
  if (encoding == constant_false_encoding
      or encoding == constant_true_encoding) {
    return {};
  }
  TRY(writer.charge_bitmap(bitmap.length()));
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
    return {};
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
  return {};
}

auto write_bitmap(Writer& writer, storage::BitMap const& bitmap)
  -> Result<void, std::string> {
  auto const encoding = bitmap_encoding(bitmap);
  writer.integer(encoding);
  return write_bitmap_body(writer, bitmap, encoding);
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

// The scalar types whose in-memory representation *is* their Bitz
// representation, so a dense fully-visible array can be handed to the writer as
// one memcpy instead of a value at a time.
//
// `Time` and `Duration` qualify for the same reason `Int` does: both are
// `std::chrono` wrappers around a single `std::int64_t` tick count, so the
// object representation of the array is the little-endian tick sequence the
// per-value path would have written. They are only eligible because neither
// carries a narrowing encoding — `encoding` stays `dense_64_encoding` for them
// — which is what makes the fast path's encoding check trivially true.
template <class Tag>
concept bulk_copy_scalar
  = std::same_as<Tag, Int> or std::same_as<Tag, UInt>
    or std::same_as<Tag, Float> or std::same_as<Tag, Time>
    or std::same_as<Tag, Duration>;

// The storage behind an array, if its values are laid out exactly as the frame
// wants them: a dense primary buffer, every row visible, and no row selection
// to apply. Returns nullptr when any of that fails, in which case the caller
// falls back to its per-value loop.
//
// Shared by every bulk path so that the conditions are stated once. Getting one
// of them wrong does not fail loudly — it writes a frame that decodes to the
// wrong values — so there should be exactly one copy of this predicate.
template <class Value, class Array>
auto dense_values(Array const& array, storage::BitMap const& visible)
  -> storage::SparseStorage<Value> const* {
  using Primary = storage::SparseStorage<Value>;
  // `is_identity()` and not merely `not compact`: a non-compact selection still
  // gathers, and a gather gives row `i` some other source row. Copying the
  // storage would then write the right values in the wrong order, silently.
  // Equal lengths are not enough to rule that out, since a gather can cover
  // every source row and still permute them.
  if (array.selection.compact or not array.selection.is_identity()
      or visible.true_count() != visible.length()
      or not is<Primary>(array.storage())) {
    return nullptr;
  }
  auto const& values = as<Primary>(array.storage());
  if (values.length() != array.length()) {
    return nullptr;
  }
  return &values;
}

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

// A lazy gather: list children may consist of several disjoint source ranges.
// Keeping ranges instead of copied values also bounds work for large constants.
class RowSelection {
public:
  explicit RowSelection(storage::Index length = 0, bool compact = false)
    : compact{compact} {
    append({0, length});
  }

  auto append(storage::Span span) -> void {
    TENZIR_ASSERT_LEQ(span.begin, span.end);
    if (span.begin == span.end) {
      return;
    }
    TENZIR_ASSERT_LEQ(span.end - span.begin,
                      std::numeric_limits<storage::Index>::max() - length());
    ranges_.push_back({span.begin, length() + (span.end - span.begin)});
  }

  auto length() const -> storage::Index {
    return ranges_.empty() ? 0 : ranges_.back().end;
  }

  auto index(storage::Index row) const -> storage::Index {
    TENZIR_ASSERT(row >= 0 and row < length());
    auto const it = std::ranges::upper_bound(ranges_, row, {}, &Range::end);
    auto const begin = it == ranges_.begin() ? 0 : std::prev(it)->end;
    return it->source + (row - begin);
  }

  /// Whether `index(row) == row` for every row, so a caller with a row loop can
  /// skip the binary search in `index` entirely.
  ///
  /// This is not the same question as `not compact`: a non-compact selection
  /// still carries several ranges when it gathers the children of a list.
  auto is_identity() const -> bool {
    return ranges_.size() == 1 and ranges_.front().source == 0;
  }

  // Only used for sorted, disjoint list-child ranges.
  auto offset(storage::Index source) const -> storage::Index {
    auto const end
      = std::ranges::upper_bound(ranges_, source, {}, &Range::source);
    TENZIR_ASSERT(end != ranges_.begin());
    auto const it = std::prev(end);
    auto const begin = it == ranges_.begin() ? 0 : std::prev(it)->end;
    return begin + (source - it->source);
  }

  bool compact;

private:
  struct Range {
    storage::Index source;
    storage::Index end;
  };
  std::vector<Range> ranges_;
};

template <data_type Tag>
struct SelectedArray {
  Array<Tag> const& source;
  RowSelection const& selection;

  auto length() const -> storage::Index {
    return selection.length();
  }

  auto get(storage::Index row) const -> RowView<Tag> {
    return source.get(selection.index(row));
  }

  auto storage() const -> decltype(auto) {
    return source.storage();
  }

  auto to_primary() const -> Array<Tag> {
    return source.to_primary();
  }
};

auto encodes_null(Array<Data> const& array, RowSelection const& selection)
  -> bool {
  if (array.try_as<Null>()) {
    return true;
  }
  if (not selection.compact) {
    return false;
  }
  auto const* union_ = try_as<UnionArray>(array);
  if (not union_) {
    return false;
  }
  auto const& fields = union_->fields();
  auto const null = std::ranges::find_if(fields, [](auto const& field) {
    return match(field.data, []<data_type Tag>(Array<Tag> const&) {
      return std::same_as<Tag, Null>;
    });
  });
  if (null == fields.end()) {
    return false;
  }
  auto const index = static_cast<storage::Index>(null - fields.begin());
  for (auto row = storage::Index{0}; row < selection.length(); ++row) {
    if (union_->alternative_index_at(selection.index(row)) != index) {
      return false;
    }
  }
  return true;
}

// The narrow element type an encoding writes, for the storage the Nova data
// model already keeps arrays in. `Array<Int>` may be backed by `SparseStorage`
// of any of the four signed widths, `Array<Float>` by `float` or `double`, so
// an encoding is not only a wire choice: when it matches the width the array is
// *already* stored at, the frame bytes are the storage bytes.
template <class Tag, std::uint8_t Encoding>
using narrow_value = std::conditional_t<
  std::same_as<Tag, Float>, std::conditional_t<Encoding == 1, float, double>,
  std::conditional_t<
    Encoding == dense_8_encoding,
    std::conditional_t<std::same_as<Tag, Int>, std::int8_t, std::uint8_t>,
    std::conditional_t<
      Encoding == dense_16_encoding,
      std::conditional_t<std::same_as<Tag, Int>, std::int16_t, std::uint16_t>,
      std::conditional_t<
        Encoding == dense_32_encoding,
        std::conditional_t<std::same_as<Tag, Int>, std::int32_t, std::uint32_t>,
        std::conditional_t<std::same_as<Tag, Int>, std::int64_t, std::uint64_t>>>>>;

template <class Tag>
  requires(std::same_as<Tag, Int> or std::same_as<Tag, UInt>)
auto integer_encoding(SelectedArray<Tag> const& array,
                      storage::BitMap const& visible) -> std::uint8_t {
  // The storage width is deliberately not taken as the answer here. An array
  // stored as `int16` whose selected, visible values all fit in a byte still
  // encodes as `dense_8`, both because that is the narrower frame and because
  // the choice must not depend on rows a selection excludes: a value outside the
  // byte range in a filtered-out row would otherwise widen the whole column.
  //
  // One pass for the range, rather than one pass per candidate width. The
  // previous shape asked "do all values fit in 8 bits?", then 16, then 32, so a
  // column of large values — the case where narrowing buys nothing — paid three
  // full scans to find that out. The extremes answer all three questions at
  // once, and answer them identically: a range fits a width exactly when every
  // value in it does.
  using Value = Type<Tag>::ViewType;
  auto lowest = std::numeric_limits<Value>::max();
  auto highest = std::numeric_limits<Value>::min();
  for (auto i = storage::Index{0}; i < array.length(); ++i) {
    if (not visible.get(i)) {
      continue;
    }
    auto const value = *array.get(i);
    lowest = std::min(lowest, value);
    highest = std::max(highest, value);
  }
  if (lowest > highest) {
    // No visible row, so every row encodes as zero and the narrowest width
    // holds all of them.
    return dense_8_encoding;
  }
  auto fits = [&](auto width) {
    using Narrow = decltype(width);
    return lowest >= static_cast<Value>(std::numeric_limits<Narrow>::min())
           and highest
                 <= static_cast<Value>(std::numeric_limits<Narrow>::max());
  };
  if (fits(narrow_value<Tag, dense_8_encoding>{})) {
    return dense_8_encoding;
  }
  if (fits(narrow_value<Tag, dense_16_encoding>{})) {
    return dense_16_encoding;
  }
  if (fits(narrow_value<Tag, dense_32_encoding>{})) {
    return dense_32_encoding;
  }
  return dense_64_encoding;
}

auto float_encoding(SelectedArray<Float> const& array,
                    storage::BitMap const& visible) -> std::uint8_t {
  // Already stored as `float`: every value round-trips through `float` by
  // construction, which is what the scan below would conclude row by row.
  if (is<storage::SparseStorage<float>>(array.storage())) {
    return float_32_encoding;
  }
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
                 storage::BitMap const& visible, std::size_t depth,
                 RowSelection const& selection) -> Result<void, std::string>;

auto write_concrete(Writer&, SelectedArray<Secret> const&,
                    storage::BitMap const&, std::size_t)
  -> Result<void, std::string> {
  return Err{"secrets cannot be serialized by Bitz"};
}

template <data_type Tag>
  requires(not std::same_as<Tag, Secret>)
auto write_concrete(Writer& writer, SelectedArray<Tag> const& array,
                    storage::BitMap const& visible, std::size_t depth)
  -> Result<void, std::string> {
  if (array.length() != visible.length()) {
    return Err{"Bitz array visibility length mismatch"};
  }
  TRY(writer.checked_array_length(static_cast<std::uint32_t>(array.length())));
  TRY(writer.enter_array(array.length(), depth));
  auto encoding = std::uint8_t{0};
  if constexpr (std::same_as<Tag, Bool>) {
    auto values = storage::BitMap::Builder{};
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      values.emplace_back(visible.get(i) ? *array.get(i) : false);
    }
    auto bitmap = values.finish();
    encoding = bitmap_encoding(bitmap);
    write_header(writer, type_id<Tag>, encoding);
    return write_bitmap_body(writer, bitmap, encoding);
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
    static_assert(sizeof(Value) == sizeof(std::uint64_t));
    auto width = std::uint64_t{8};
    if constexpr (std::same_as<Tag, Int> or std::same_as<Tag, UInt>) {
      if (encoding != dense_64_encoding) {
        width = std::uint64_t{1} << (encoding - 1);
      }
    } else if constexpr (std::same_as<Tag, Float>) {
      if (encoding == float_32_encoding) {
        width = 4;
      }
    }
    TRY(writer.charge_decoded(static_cast<std::uint64_t>(array.length())
                              * width));
    if constexpr (bulk_copy_scalar<Tag>) {
      static_assert(std::is_trivially_copyable_v<Value>);
      // A non-native byte order writes the bytes in the other direction, so the
      // memory is no longer the frame. Nothing else disqualifies the copy: the
      // encoding picks *which* storage width has to match, not whether one can.
      if (writer.scalar_byte_order() == native_scalar_byte_order) {
        auto copied = false;
        auto copy = [&]<class Narrow>() {
          if (auto const* values = dense_values<Narrow>(array, visible)) {
            writer.bytes(std::as_bytes(std::span{
              values->data(), static_cast<std::size_t>(values->length())}));
            copied = true;
          }
        };
        // An array stored narrow and encoded at that same width needs no
        // conversion at all: `integer_encoding` and `float_encoding` above pick
        // the storage's own width when it is already narrow, so this is the
        // common case rather than a lucky one.
        if constexpr (std::same_as<Tag, Int> or std::same_as<Tag, UInt>) {
          switch (encoding) {
            case dense_8_encoding:
              copy.template operator()<narrow_value<Tag, dense_8_encoding>>();
              break;
            case dense_16_encoding:
              copy.template operator()<narrow_value<Tag, dense_16_encoding>>();
              break;
            case dense_32_encoding:
              copy.template operator()<narrow_value<Tag, dense_32_encoding>>();
              break;
            default:
              copy.template operator()<Value>();
              break;
          }
        } else if constexpr (std::same_as<Tag, Float>) {
          if (encoding == float_32_encoding) {
            copy.template operator()<float>();
          } else {
            copy.template operator()<Value>();
          }
        } else {
          copy.template operator()<Value>();
        }
        if (copied) {
          return {};
        }
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
    using Primary = Type<Tag>::PrimaryPhysicalStorage;
    // A dense string or blob column is a contiguous byte buffer plus a span per
    // row \u2014 which is what the frame is too, so when the spans run back to
    // back over the rows being written, the payload is one copy of a buffer
    // range and the offsets are a walk over the spans. The row-at-a-time path
    // below reads `array.get(i)`, which dispatches over the storage variant and
    // builds a view, twice per row, and hands the writer one call per value.
    //
    // Contiguity has to be checked rather than assumed: spans may overlap or
    // skip after a slice or a gather, and nothing in the storage says they do
    // not.
    auto const* dense = [&]() -> Primary const* {
      if (array.selection.compact or not array.selection.is_identity()
          or visible.true_count() != visible.length()
          or not is<Primary>(array.storage())) {
        return nullptr;
      }
      auto const& storage = as<Primary>(array.storage());
      return storage.length() >= array.length() ? &storage : nullptr;
    }();
    auto byte_count = std::uint32_t{0};
    auto payload = storage::Span{0, 0};
    auto contiguous = dense != nullptr;
    if (contiguous and array.length() > 0) {
      payload = dense->span(0);
      auto end = payload.begin;
      for (auto i = storage::Index{0}; i < array.length(); ++i) {
        auto const span = dense->span(i);
        if (span.begin != end) {
          contiguous = false;
          break;
        }
        end = span.end;
      }
      payload.end = end;
      if (contiguous
          and std::cmp_greater(payload.end - payload.begin,
                               std::numeric_limits<std::uint32_t>::max())) {
        return writer.limit_error(
          "string or blob data exceeds the Bitz size limit");
      }
      byte_count = static_cast<std::uint32_t>(payload.end - payload.begin);
    }
    if (contiguous) {
      // Offsets in blocks rather than one call per row. The first offset is
      // always zero and the last is the total, which the frame repeats after
      // them.
      constexpr auto block = std::size_t{256};
      auto buffer = std::array<std::uint32_t, block>{};
      auto filled = std::size_t{0};
      auto emit = [&](std::uint32_t offset) {
        buffer[filled++] = offset;
        if (filled == block) {
          writer.integers(std::span{buffer.data(), filled});
          filled = 0;
        }
      };
      emit(0);
      for (auto i = storage::Index{0}; i < array.length(); ++i) {
        emit(static_cast<std::uint32_t>(dense->span(i).end - payload.begin));
      }
      emit(byte_count);
      if (filled > 0) {
        writer.integers(std::span{buffer.data(), filled});
      }
      TRY(writer.charge_decoded(
        ((static_cast<std::uint64_t>(array.length()) + 1)
         * sizeof(std::uint32_t))
        + (static_cast<std::uint64_t>(array.length()) * sizeof(storage::Span))
        + byte_count));
      if (byte_count > 0) {
        writer.bytes(
          std::as_bytes(std::span{dense->data().begin() + payload.begin,
                                  static_cast<std::size_t>(byte_count)}));
      }
      return {};
    }
    writer.integer(byte_count);
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      if (visible.get(i)) {
        auto value = *array.get(i);
        if (value.size()
            > std::numeric_limits<std::uint32_t>::max() - byte_count) {
          return writer.limit_error(
            "string or blob data exceeds the Bitz size limit");
        }
        byte_count += static_cast<std::uint32_t>(value.size());
      }
      writer.integer(byte_count);
    }
    writer.integer(byte_count);
    TRY(writer.charge_decoded(
      ((static_cast<std::uint64_t>(array.length()) + 1) * sizeof(std::uint32_t))
      + (static_cast<std::uint64_t>(array.length()) * sizeof(storage::Span))
      + byte_count));
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
    TRY(writer.charge_decoded(static_cast<std::uint64_t>(array.length()) * 16));
    // An `ip` is one `std::array<std::uint8_t, 16>` and nothing else, so an
    // array of them is already the 16-byte-per-row frame layout. No byte-order
    // check here, unlike the scalars above: those bytes are network order in
    // memory and go out unswapped either way.
    static_assert(sizeof(Ip) == 16);
    static_assert(std::is_trivially_copyable_v<Ip>);
    if (auto const* values = dense_values<Ip>(array, visible)) {
      writer.bytes(std::as_bytes(
        std::span{values->data(), static_cast<std::size_t>(values->length())}));
      return {};
    }
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto value = visible.get(i) ? *array.get(i) : Ip{};
      writer.bytes(as_bytes(value));
    }
  } else if constexpr (std::same_as<Tag, Subnet>) {
    TRY(writer.charge_decoded(static_cast<std::uint64_t>(array.length()) * 17));
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto value = visible.get(i) ? *array.get(i) : Subnet{};
      writer.bytes(as_bytes(value.network()));
      writer.integer(value.length());
    }
  } else if constexpr (std::same_as<Tag, List>) {
    auto primary = array.to_primary();
    auto const& physical = as<storage::ListStorage>(primary.storage());
    auto const& source_spans = physical.spans();
    auto children = RowSelection{0, array.selection.compact};
    auto spans = std::vector<storage::Span>{};
    // Project spans without materializing their values. Invisible lists have no
    // children in a compacted frame; overlapping spans keep sharing children.
    auto live_ranges = std::vector<storage::Span>{};
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto span = source_spans[array.selection.index(i)];
      if (span.begin < 0 or span.end < span.begin
          or span.end > physical.values().length()) {
        if (visible.get(i)) {
          return Err{"invalid visible list span"};
        }
        span = {0, 0};
      }
      if (array.selection.compact) {
        if (not visible.get(i)) {
          span = {0, 0};
        } else if (span.begin != span.end) {
          live_ranges.push_back(span);
        }
      }
      spans.push_back(span);
    }
    if (array.selection.compact) {
      std::ranges::sort(live_ranges, {}, &storage::Span::begin);
      auto merged = storage::Span{0, 0};
      for (auto span : live_ranges) {
        if (span.begin > merged.end) {
          children.append(merged);
          merged = span;
        } else {
          if (merged.begin == merged.end) {
            merged.begin = span.begin;
          }
          merged.end = std::max(merged.end, span.end);
        }
      }
      children.append(merged);
      TRY(writer.checked_array_length(
        static_cast<std::uint32_t>(children.length())));
      for (auto& span : spans) {
        if (span.begin == span.end) {
          span = {0, 0};
        } else {
          auto const begin = children.offset(span.begin);
          span = {begin, begin + (span.end - span.begin)};
        }
      }
    } else {
      children = RowSelection{physical.values().length()};
    }
    auto const child_length = children.length();
    TRY(
      writer.charge_decoded((static_cast<std::uint64_t>(array.length())
                             + static_cast<std::uint64_t>(visible.true_count()))
                            * sizeof(storage::Span)));
    TRY(writer.charge_bitmap(child_length));
    // Visibility validation revisits non-null children after decoding. Empty
    // children are encoded as Null below, regardless of their input type, so
    // they need neither validation spans nor a second visibility bitmap.
    if (child_length > 0 and not encodes_null(physical.values(), children)) {
      TRY(writer.charge_decoded(static_cast<std::uint64_t>(visible.true_count())
                                * sizeof(storage::Span)));
      TRY(writer.charge_bitmap(child_length));
    }
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto const span = spans[static_cast<std::size_t>(i)];
      writer.integer(static_cast<std::uint32_t>(span.begin));
      writer.integer(static_cast<std::uint32_t>(span.end));
    }
    writer.integer(static_cast<std::uint32_t>(child_length));
    if (child_length == 0) {
      TRY(write_concrete(
        writer, SelectedArray{Array<Null>{storage::NullStorage{0}}, children},
        storage::BitMap{0, false}, depth + 1));
    } else {
      auto child_visible = storage::BitMap::Mutable{child_length};
      for (auto i = storage::Index{0}; i < array.length(); ++i) {
        if (not visible.get(i)) {
          continue;
        }
        auto const span = spans[static_cast<std::size_t>(i)];
        for (auto child = span.begin; child < span.end; ++child) {
          child_visible.set(child, true);
        }
      }
      TRY(write_array(writer, physical.values(),
                      std::move(child_visible).finish(), depth + 1, children));
    }
  } else if constexpr (std::same_as<Tag, Record>) {
    auto primary = array.to_primary();
    auto const& physical = as<storage::RecordStorage>(primary.storage());
    auto const& data = *physical;
    if (data.names_by_index.size() != data.arrays.size()) {
      return Err{"record field names and arrays have different sizes"};
    }
    auto shapes = std::vector<ShapeTable::ShapeId>{};
    // Indexed by shape id rather than hashed: ids are dense over the shape
    // table, and this is read once per row *per field*, which made it the
    // single most expensive lookup in the encoder. -1 means the shape is not in
    // the frame.
    auto shape_remap = std::vector<storage::Index>(data.shape_table.size(),
                                                   storage::Index{-1});
    auto live_fields
      = std::vector<bool>(data.arrays.size(), not array.selection.compact);
    auto add_shape
      = [&](ShapeTable::ShapeId shape) -> Result<void, std::string> {
      if (shape < 0 or std::cmp_greater_equal(shape, data.shape_table.size())) {
        return Err{"visible record row has an invalid shape"};
      }
      if (shape_remap[static_cast<std::size_t>(shape)] >= 0) {
        return {};
      }
      TRY(writer.check_shape_count(
        static_cast<std::uint32_t>(shapes.size() + 1)));
      shape_remap[static_cast<std::size_t>(shape)]
        = static_cast<storage::Index>(shapes.size());
      shapes.push_back(shape);
      if (array.selection.compact) {
        for (auto field : data.shape_table.fields(shape)) {
          if (field < 0 or std::cmp_greater_equal(field, data.arrays.size())) {
            return Err{"record shape refers to an invalid field"};
          }
          live_fields[static_cast<std::size_t>(field)] = true;
        }
      }
      return {};
    };
    if (array.selection.compact) {
      TRY(add_shape(ShapeTable::empty_shape));
      for (auto i = storage::Index{0}; i < array.length(); ++i) {
        if (visible.get(i)) {
          TRY(add_shape(data.shape_indices.get(array.selection.index(i))));
        }
      }
    } else {
      TRY(writer.check_shape_count(
        static_cast<std::uint32_t>(data.shape_table.size())));
      for (auto i = std::size_t{0}; i < data.shape_table.size(); ++i) {
        TRY(add_shape(static_cast<ShapeTable::ShapeId>(i)));
      }
    }
    auto field_remap
      = std::vector<storage::Index>(data.arrays.size(), storage::Index{-1});
    auto field_count = std::uint32_t{0};
    for (auto index = std::size_t{0}; index < data.names_by_index.size();
         ++index) {
      auto const name = data.names_by_index[index];
      auto const registered = data.names.find(name);
      if (not live_fields[index] or registered == data.names.end()
          or registered->second != index) {
        continue;
      }
      TRY(writer.charge_field_name(name.size()));
      field_remap[index] = static_cast<storage::Index>(field_count++);
    }
    TRY(writer.check_field_count(field_count));
    writer.integer(field_count);
    for (auto index = std::size_t{0}; index < data.names_by_index.size();
         ++index) {
      if (field_remap[index] >= 0) {
        writer.string(data.names_by_index[index]);
      }
    }
    auto const shape_count = static_cast<std::uint32_t>(shapes.size());
    TRY(writer.check_shape_count(shape_count));
    TRY(writer.charge_decoded(
      (static_cast<std::uint64_t>(field_count) * (128 + sizeof(std::uint32_t)))
      + (static_cast<std::uint64_t>(shape_count) * sizeof(ShapeTable::ShapeId))
      + (static_cast<std::uint64_t>(array.length()) * sizeof(storage::Index))));
    writer.integer(shape_count);
    // Membership inverted: per field, the shapes that carry it. The row loop
    // below asks "is this field in this shape?" once per row per field, which
    // used to be a binary search; the field loop turns its own entry of this
    // index into a flag array once and then reads it.
    //
    // Deliberately *not* a flat (shape, field) table: the two counts are
    // bounded independently \u2014 65536 shapes and 16384 fields by default
    // \u2014 so their product is a gibibyte, which a batch with few actual
    // memberships could make the encoder allocate before any limit fires. This
    // holds one entry per membership instead, which `charge_shape_entries`
    // already bounds.
    auto shapes_with_field
      = std::vector<std::vector<storage::Index>>(data.arrays.size());
    for (auto shape_index = std::size_t{0}; shape_index < shapes.size();
         ++shape_index) {
      auto fields = data.shape_table.fields(shapes[shape_index]);
      auto live_count = std::uint32_t{0};
      for (auto field : fields) {
        if (field < 0
            or static_cast<std::size_t>(field) >= data.arrays.size()) {
          return Err{"record shape refers to an invalid field"};
        }
        live_count += field_remap[static_cast<std::size_t>(field)] >= 0;
      }
      TRY(writer.charge_shape_entries(live_count));
      // The decoder also builds shape membership during visibility validation.
      // Remapping removed fields can collapse shapes, so this is an upper bound.
      TRY(writer.charge_decoded(static_cast<std::uint64_t>(live_count)
                                * sizeof(storage::Index)));
      writer.integer(live_count);
      for (auto field : fields) {
        auto const mapped = field_remap[static_cast<std::size_t>(field)];
        if (mapped >= 0) {
          writer.integer(static_cast<std::uint32_t>(mapped));
          shapes_with_field[static_cast<std::size_t>(field)].push_back(
            static_cast<storage::Index>(shape_index));
        }
      }
    }
    // `index(row) == row` for a selection that gathers nothing, which is every
    // selection but a list's children. Hoisting the test out of the row loops
    // below removes a binary search per row.
    auto const identity = array.selection.is_identity();
    auto const source_of = [&](storage::Index row) {
      return identity ? row : array.selection.index(row);
    };
    // An identity selection still need not cover the whole source: splitting an
    // oversized frame hands each half a prefix of the rows. A field's bitmap
    // can only stand in for the frame's when the two have the same length, so
    // the fast path below asks for the stronger property.
    auto const covers_source
      = identity and array.length() == array.source.length();
    auto const all_visible = visible.true_count() == visible.length();
    // Validate every row's shape and find out whether they are all the same
    // one. This pass only reads; the write follows below, so that a frame whose
    // rows share a shape can emit the column in blocks instead of a value at a
    // time. The read is an indexed load per row, where the write was a bounds
    // and growth check per row.
    auto uniform_mapped = storage::Index{-1};
    auto all_same = true;
    for (auto i = storage::Index{0}; i < array.length(); ++i) {
      auto shape = visible.get(i) ? data.shape_indices.get(source_of(i))
                                  : ShapeTable::empty_shape;
      if (shape < 0
          or static_cast<std::size_t>(shape) >= data.shape_table.size()) {
        return Err{"visible record row has an invalid shape"};
      }
      auto const mapped = shape_remap[static_cast<std::size_t>(shape)];
      if (mapped < 0) {
        return Err{"visible record row has an invalid shape"};
      }
      if (i == 0) {
        uniform_mapped = mapped;
      } else if (mapped != uniform_mapped) {
        // No early exit: this pass is also what validates the remaining rows,
        // and the write below relies on that having happened for all of them.
        all_same = false;
      }
    }
    if (all_same and array.length() > 0) {
      writer.repeat_integer(static_cast<std::uint32_t>(uniform_mapped),
                            array.length());
    } else {
      for (auto i = storage::Index{0}; i < array.length(); ++i) {
        auto const shape = visible.get(i) ? data.shape_indices.get(source_of(i))
                                          : ShapeTable::empty_shape;
        writer.integer(static_cast<std::uint32_t>(
          shape_remap[static_cast<std::size_t>(shape)]));
      }
    }
    // Every row carrying one shape, nothing hidden, and no gather: then a
    // field's presence is a property of the field rather than of the row, and
    // the per-field row loop below has nothing left to decide. This is the
    // shape of a batch whose events all have the same fields — the common case,
    // and the one that loop costs the most on, since it runs once per field.
    auto const uniform
      = all_same and all_visible and covers_source and uniform_mapped >= 0
          ? uniform_mapped
          : storage::Index{-1};
    // One flag per shape, filled from the inverted index for the field being
    // written and cleared again afterwards, so the row loop tests membership
    // with an indexed load. Allocated once for all fields: its size is the
    // shape count, which `check_shape_count` bounds.
    auto shape_has_field = std::vector<char>(shapes.size(), char{0});
    for (auto field_index = std::size_t{0}; field_index < data.arrays.size();
         ++field_index) {
      if (field_remap[field_index] < 0) {
        continue;
      }
      auto const& field = data.arrays[field_index];
      if (field.data.length() != array.source.length()
          or field.present.length() != array.source.length()) {
        return Err{"record field length does not match its record array"};
      }
      auto const& carriers = shapes_with_field[field_index];
      auto present_mask = storage::BitMap{};
      if (uniform >= 0) {
        // Presence is then a property of the field rather than of the row: the
        // single shape either carries the field, in which case the field's own
        // bitmap already says which rows have it, or it does not, in which case
        // no row does.
        //
        // The question is whether *that* shape carries the field, not whether
        // any shape does: a frame holds every shape of the table, including
        // ones no row uses, and a field array may carry presence bits for rows
        // whose shape omits it. Taking the field's bitmap on the strength of an
        // unused carrier would write those bits out, and the decoder rejects a
        // field present outside its shape.
        present_mask = std::ranges::binary_search(carriers, uniform)
                         ? field.present
                         : storage::BitMap{array.length(), false};
      } else {
        for (auto shape : carriers) {
          shape_has_field[static_cast<std::size_t>(shape)] = char{1};
        }
        auto present = storage::BitMap::Builder{};
        for (auto row = storage::Index{0}; row < array.length(); ++row) {
          auto selected = false;
          if (visible.get(row)) {
            auto const source_row = source_of(row);
            auto const shape = shape_remap[static_cast<std::size_t>(
              data.shape_indices.get(source_row))];
            selected
              = shape_has_field[static_cast<std::size_t>(shape)] != char{0}
                and field.present.get(source_row);
          }
          present.emplace_back(selected);
        }
        present_mask = std::move(present).finish();
        for (auto shape : carriers) {
          shape_has_field[static_cast<std::size_t>(shape)] = char{0};
        }
      }
      TRY(write_bitmap(writer, present_mask));
      TRY(write_array(writer, field.data, present_mask, depth + 1,
                      array.selection));
    }
  }
  return {};
}

auto write_array(Writer& writer, Array<Data> const& array,
                 storage::BitMap const& visible, std::size_t depth,
                 RowSelection const& selection) -> Result<void, std::string> {
  return match(
    array,
    [&](UnionArray const& union_) -> Result<void, std::string> {
      auto const length = selection.length();
      if (length != visible.length()) {
        return Err{"Bitz union visibility length mismatch"};
      }
      if (selection.compact and not visible.any()) {
        return write_concrete(
          writer,
          SelectedArray{Array<Null>{storage::NullStorage{length}},
                        RowSelection{length}},
          visible, depth);
      }
      TRY(writer.checked_array_length(static_cast<std::uint32_t>(length)));
      auto const& fields = union_.fields();
      if (fields.size() < 2 or fields.size() > data_type_list::size) {
        return Err{"invalid Bitz union alternative count"};
      }
      auto remap = std::vector<storage::Index>(fields.size(), -1);
      for (auto row = storage::Index{0}; row < length; ++row) {
        if (not visible.get(row)) {
          continue;
        }
        auto const source = selection.index(row);
        auto const index = union_.alternative_index_at(source);
        if (index < 0 or std::cmp_greater_equal(index, fields.size())
            or not fields[static_cast<std::size_t>(index)].present.get(
              source)) {
          return Err{"visible union row has an invalid alternative"};
        }
        remap[static_cast<std::size_t>(index)] = 0;
      }
      auto count = storage::Index{0};
      for (auto& index : remap) {
        if (not selection.compact or index >= 0) {
          index = count++;
        }
      }
      // A projected union can become monomorphic. The wire format requires at
      // least two union alternatives, so encode its remaining concrete type.
      if (count == 1) {
        auto const index = static_cast<std::size_t>(std::ranges::find(remap, 0)
                                                    - remap.begin());
        return match(fields[index].data, [&](auto const& concrete) {
          return write_concrete(writer, SelectedArray{concrete, selection},
                                visible, depth);
        });
      }
      TRY(writer.enter_array(length, depth));
      write_header(writer, TypeId::union_, union_u8_encoding);
      TRY(writer.charge_decoded(
        (static_cast<std::uint64_t>(length) * sizeof(storage::Index))
        + (static_cast<std::uint64_t>(count) * 64)));
      TRY(writer.charge_bitmap(length));
      writer.integer(static_cast<std::uint32_t>(count));
      for (auto row = storage::Index{0}; row < length; ++row) {
        auto const index
          = visible.get(row)
              ? remap[static_cast<std::size_t>(
                  union_.alternative_index_at(selection.index(row)))]
              : 0;
        writer.integer(static_cast<std::uint8_t>(index));
      }
      for (auto field_index = std::size_t{0}; field_index < fields.size();
           ++field_index) {
        if (remap[field_index] < 0) {
          continue;
        }
        auto const& field = fields[field_index];
        TRY(writer.charge_bitmap(length));
        if (field.present.length() != union_.length()) {
          return Err{"union alternative mask length mismatch"};
        }
        auto present = storage::BitMap::Builder{};
        for (auto row = storage::Index{0}; row < length; ++row) {
          auto const source = selection.index(row);
          present.emplace_back(visible.get(row)
                               and union_.alternative_index_at(source)
                                     == static_cast<storage::Index>(field_index)
                               and field.present.get(source));
        }
        auto present_mask = present.finish();
        TRY(match(field.data, [&](auto const& concrete) {
          return write_concrete(writer, SelectedArray{concrete, selection},
                                present_mask, depth + 1);
        }));
      }
      return {};
    },
    [&](auto const& concrete) {
      return write_concrete(writer, SelectedArray{concrete, selection}, visible,
                            depth);
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
  static_assert(sizeof(Value) == sizeof(Bits));
  static_assert(std::is_trivially_copyable_v<Value>);
  TRY(
    reader.charge_decoded(static_cast<std::uint64_t>(length) * sizeof(Value)));
  // A narrow column is not widened on the way in: the Nova data model keeps
  // `Array<Int>` over any of the four signed widths and `Array<Float>` over
  // `float` or `double`, so the decoded array is stored at the width the frame
  // used. That makes the frame's bytes the storage's bytes, and the whole
  // column is one copy rather than a `scalar()` call per row.
  if (length > 0 and reader.scalar_byte_order() == native_scalar_byte_order) {
    auto const byte_size = static_cast<std::size_t>(length) * sizeof(Value);
    TRY(auto bytes, reader.bytes(byte_size));
    auto values = typename storage::SparseStorage<Value>::Mutable{length};
    std::memcpy(values.data(), bytes.data(), byte_size);
    return DecodedArray{Array<Tag>{std::move(values).finish()}, type};
  }
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
      if (length == 0) {
        return DecodedArray{ArrayBuilder<Ip>{}.finish(), type};
      }
      // The column is 16 bytes per row and an `ip` is 16 bytes of the same
      // order, so the frame is already the array: take it in one copy rather
      // than reconstructing a value at a time through the builder. This mirrors
      // the encoder's bulk path, and like it depends on `ip` holding nothing
      // but its byte array.
      static_assert(sizeof(Ip) == 16);
      static_assert(std::is_trivially_copyable_v<Ip>);
      auto const byte_size = static_cast<std::size_t>(length) * sizeof(Ip);
      TRY(auto bytes, reader.bytes(byte_size));
      auto values = storage::SparseStorage<Ip>::Mutable{length};
      std::memcpy(values.data(), bytes.data(), byte_size);
      return DecodedArray{Array<Ip>{std::move(values).finish()}, type};
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

/// An initial output capacity for one encode, in bytes.
///
/// The in-memory footprint of the batch, scaled by the fraction of its rows the
/// selection keeps. It is an estimate and nothing depends on it being right:
/// the buffer grows when it is too small, and a batch that is handed back is
/// copied into a chunk by the caller anyway, so slack costs nothing lasting.
///
/// The point is only to start close. Encoded Bitz is in the same order of
/// magnitude as the arrays it comes from — it is those arrays plus light
/// metadata, minus whatever narrowing saves — so this normally lands within one
/// doubling and the growth copies disappear.
auto reserve_hint(Batch const& batch, RowSelection const& selection)
  -> std::uint64_t {
  auto const approx = static_cast<std::uint64_t>(
    batch.data.approx_bytes() + batch.mask.approx_bytes()
    + batch.meta.name.approx_bytes() + batch.meta.import_time.approx_bytes()
    + batch.meta.internal.approx_bytes());
  auto const length = batch.length();
  auto scaled = approx;
  if (selection.compact and length > 0) {
    scaled = approx * static_cast<std::uint64_t>(selection.length())
             / static_cast<std::uint64_t>(length);
  }
  // A floor, so that a tiny batch does not start from nothing and double its
  // way up through a dozen reallocations of a few hundred bytes each.
  return std::max(scaled, std::uint64_t{4} << 10);
}

auto encode_selected(Batch const& batch, EncodeOptions const& options,
                     RowSelection const& selection, bool& limit_exceeded)
  -> Result<std::vector<std::byte>, std::string> {
  limit_exceeded = false;
  if (options.scalar_byte_order != ScalarByteOrder::little
      and options.scalar_byte_order != ScalarByteOrder::big) {
    return Err{"unsupported Bitz scalar byte order"};
  }
  auto length = batch.length();
  if (length < 0 or batch.mask.length() != length
      or batch.meta.name.length() != length
      or batch.meta.import_time.length() != length
      or batch.meta.internal.length() != length) {
    return Err{"inconsistent Bitz batch column lengths"};
  }
  auto write = [&](Writer& writer) -> Result<void, std::string> {
    auto const length = selection.length();
    TRY(writer.check_rows(static_cast<std::uint32_t>(length)));
    TRY(writer.checked_array_length(static_cast<std::uint32_t>(length)));
    auto mask = batch.mask;
    if (selection.compact) {
      auto builder = storage::BitMap::Builder{};
      for (auto row = storage::Index{0}; row < length; ++row) {
        builder.emplace_back(batch.mask.get(selection.index(row)));
      }
      mask = builder.finish();
    }
    writer.integer(static_cast<std::uint8_t>(options.scalar_byte_order));
    writer.integer(static_cast<std::uint32_t>(length));
    TRY(write_bitmap(writer, mask));
    TRY(write_array(writer, batch.data, mask, 0, selection));
    // Each typed metadata column gets a validation visibility bitmap.
    for (auto i = 0; i < 3; ++i) {
      TRY(writer.charge_bitmap(length));
    }
    TRY(write_concrete(writer, SelectedArray{batch.meta.name, selection}, mask,
                       0));
    TRY(write_concrete(writer, SelectedArray{batch.meta.import_time, selection},
                       mask, 0));
    TRY(write_concrete(writer, SelectedArray{batch.meta.internal, selection},
                       mask, 0));
    if (not writer.valid()) {
      return writer.limit_error(
        writer.frame_exceeded()
          ? "Bitz frame exceeds the resource limit"
          : "Bitz payload size exceeds the platform limit");
    }
    return {};
  };
  // One pass. The encoder used to run `write` twice — once into a writer with
  // no output buffer, purely to total the bytes, and once for real — so that
  // the result could be allocated exactly once. That guarantee turned out to
  // cost half of all encoding time: profiling `write_bitz` attributed 48.8% of
  // its cycles to the sizing pass, which performs every per-row loop, every
  // encoding decision and every bitmap build of the real pass and emits
  // nothing. The copy it was protecting is far cheaper than that: the payload
  // memcpy is under a tenth of encode time, and geometric growth amortizes to
  // about one extra copy of it, further reduced by the reservation below.
  auto writer = Writer{options};
  writer.reserve(reserve_hint(batch, selection));
  auto written = write(writer);
  limit_exceeded = writer.limit_exceeded();
  TRY(std::move(written));
  return std::move(writer).take();
}

} // namespace

auto encode(Batch const& batch, EncodeOptions const& options)
  -> Result<std::vector<std::byte>, std::string> {
  if (batch.length() < 0) {
    return Err{"inconsistent Bitz batch column lengths"};
  }
  auto limit_exceeded = false;
  return encode_selected(batch, options, RowSelection{batch.length()},
                         limit_exceeded);
}

auto encode_batches(Batch batch, EncodeOptions options)
  -> generator<Result<std::vector<std::byte>, std::string>> {
  if (batch.length() < 0) {
    co_yield Err{"inconsistent Bitz batch column lengths"};
    co_return;
  }
  auto limit_exceeded = false;
  auto original = encode_selected(batch, options, RowSelection{batch.length()},
                                  limit_exceeded);
  if (original or not limit_exceeded) {
    co_yield std::move(original);
    co_return;
  }
  // Compact before splitting: unused fields, shapes, and children may be the
  // only reason a batch exceeds a budget. Split by projecting column indices,
  // not by rebuilding event rows.
  auto pending = std::vector<storage::Span>{{0, batch.length()}};
  while (not pending.empty()) {
    auto const range = pending.back();
    pending.pop_back();
    auto selection = RowSelection{0, true};
    selection.append(range);
    auto result = encode_selected(batch, options, selection, limit_exceeded);
    if (result) {
      co_yield std::move(result);
      continue;
    }
    auto const length = range.end - range.begin;
    if (not limit_exceeded or length <= 1) {
      if (limit_exceeded and length == 1) {
        co_yield Err{std::string{"single event exceeds Bitz resource limits: "}
                     + result.unwrap_err()};
      } else {
        co_yield std::move(result);
      }
      co_return;
    }
    auto const middle = range.begin + length / 2;
    pending.push_back({middle, range.end});
    pending.push_back({range.begin, middle});
  }
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
