//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/arrow_import.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/option.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/cast.h>
#include <arrow/result.h>
#include <arrow/util/bitmap_ops.h>
#include <arrow/visit_array_inline.h>

#include <bit>
#include <cstring>
#include <limits>
#include <ranges>
#include <span>
#include <unordered_set>

namespace tenzir::nova {
namespace {

// Inspect ownership before Arrow creates cached child arrays and slice views.
// The source ArrayData tree stays alive until all recursive imports finish.
class ArrowBuffers {
public:
  auto collect(std::shared_ptr<arrow::ArrayData> const& data) -> void {
    if (not data or data.use_count() != 1) {
      return;
    }
    for (auto const& buffer : data->buffers) {
      // A plain MutableBuffer can wrap unowned memory. Restrict adoption to
      // owning pool allocations; shared, immutable, or parent-backed buffers
      // take the copy path instead.
      if (buffer and buffer.use_count() == 1 and buffer->is_cpu()
          and buffer->is_mutable() and not buffer->parent()
          and dynamic_cast<arrow::ResizableBuffer*>(buffer.get())) {
        adoptable_.insert(buffer.get());
      }
    }
    for (auto const& child : data->child_data) {
      collect(child);
    }
    collect(data->dictionary);
  }

  template <class T>
  auto take(std::shared_ptr<arrow::Buffer> const& buffer, int64_t byte_offset,
            storage::Index length, bool allow_padding = false)
    -> storage::DataOwner<T[]> {
    if (length == 0 or not buffer or not adoptable_.contains(buffer.get())) {
      return {};
    }
    auto size = static_cast<int64_t>(length) * static_cast<int64_t>(sizeof(T));
    auto available = allow_padding ? buffer->capacity() : buffer->size();
    if (byte_offset < 0 or byte_offset > available
        or size > available - byte_offset) {
      return {};
    }
    auto* bytes = buffer->mutable_data() + byte_offset;
    if (reinterpret_cast<uintptr_t>(bytes) % alignof(T) != 0) {
      return {};
    }
    adoptable_.erase(buffer.get());
    auto release = [owner = buffer](T*) mutable noexcept {
      // Release the allocation through its original Arrow memory pool.
      owner.reset();
    };
    return storage::DataOwner<T[]>::adopt_mutable(reinterpret_cast<T*>(bytes),
                                                  length, std::move(release));
  }

private:
  std::unordered_set<arrow::Buffer const*> adoptable_;
};

template <std::ranges::sized_range Range>
auto copy_buffer(Range&& values)
  -> storage::DataOwner<std::ranges::range_value_t<Range>[]> {
  using T = std::ranges::range_value_t<Range>;
  if (std::ranges::empty(values)) {
    return {};
  }
  auto result = storage::DataOwner<T[]>::make_uninitialized(
    detail::narrow<storage::Index>(std::ranges::size(values)));
  // Trivial contiguous values become a bulk copy; transformed ranges construct
  // the target layout directly, without per-element builder growth checks.
  result.move_append(std::ranges::begin(values), std::ranges::end(values));
  return result.finish();
}

template <class T>
auto import_buffer(std::shared_ptr<arrow::Buffer> const& buffer,
                   int64_t byte_offset, storage::Index length,
                   ArrowBuffers& buffers) -> storage::DataOwner<T[]> {
  if (length == 0) {
    return {};
  }
  if (auto data = buffers.take<T>(buffer, byte_offset, length)) {
    return data;
  }
  auto const* bytes = buffer->data() + byte_offset;
  if (reinterpret_cast<uintptr_t>(bytes) % alignof(T) != 0) {
    auto data = storage::DataOwner<T[]>::make_value(length, T{});
    std::memcpy(data.begin(), bytes, static_cast<size_t>(length) * sizeof(T));
    return data;
  }
  return copy_buffer(
    std::span{reinterpret_cast<T const*>(bytes), static_cast<size_t>(length)});
}

auto import_bitmap(std::shared_ptr<arrow::Buffer> const& buffer, int64_t offset,
                   storage::Index length, ArrowBuffers& buffers,
                   Option<storage::Index> true_count = {}, bool invert = false)
  -> storage::BitMap {
  if (length == 0 or true_count == 0) {
    return storage::BitMap{length, false};
  }
  if (true_count == length) {
    return storage::BitMap{length, true};
  }
  using Word = storage::BitMap::Word;
  constexpr auto word_bits = storage::BitMap::word_bits;
  auto words = length / word_bits + (length % word_bits != 0);
  auto data = storage::DataOwner<Word[]>{};
  if (offset % 8 == 0) {
    data = buffers.take<Word>(buffer, offset / 8, words, true);
  }
  if (data) {
    // Nova requires complete aligned words and zero trailing bits. Arrow pool
    // allocations provide padding, but it need not already be initialized.
    auto* bytes = reinterpret_cast<uint8_t*>(data.begin());
    auto byte_count = length / 8 + (length % 8 != 0);
    std::memset(bytes + byte_count, 0, words * sizeof(Word) - byte_count);
    if (length % 8 != 0) {
      bytes[byte_count - 1] &= (uint8_t{1} << (length % 8)) - 1;
    }
    for (auto& word : data) {
      if constexpr (std::endian::native == std::endian::big) {
        word = std::byteswap(word);
      }
      if (invert) {
        word = ~word;
      }
    }
    if (length % word_bits != 0) {
      data.back() &= (Word{1} << (length % word_bits)) - 1;
    }
    return storage::BitMap{length, std::move(data), true_count};
  }
  data = storage::DataOwner<Word[]>::make_value(words, Word{0});
  auto const* bits = buffer->data();
  // Arrow handles unaligned slice offsets. Zero initialization keeps all bits
  // past the logical end clear, including padding up to Nova's word boundary.
  auto* bytes = reinterpret_cast<uint8_t*>(data.begin());
  if (invert) {
    arrow::internal::InvertBitmap(bits, offset, length, bytes, 0);
  } else {
    arrow::internal::CopyBitmap(bits, offset, length, bytes, 0);
  }
  if constexpr (std::endian::native == std::endian::big) {
    for (auto& word : data) {
      word = std::byteswap(word);
    }
  }
  return storage::BitMap{length, std::move(data), true_count};
}

auto apply_validity(Array<Data> result, arrow::Array const& input,
                    ArrowBuffers& buffers) -> Array<Data> {
  auto null_count = input.null_count();
  if (null_count == 0) {
    return result;
  }
  auto length = detail::narrow<storage::Index>(input.length());
  auto nulls
    = import_bitmap(input.null_bitmap(), input.offset(), length, buffers,
                    detail::narrow<storage::Index>(null_count), true);
  return std::move(result).null_where(std::move(nulls));
}

template <class ArrowArray>
auto copy_spans(ArrowArray const& input)
  -> storage::DataOwner<storage::Span[]> {
  if (input.length() == 0) {
    return {};
  }
  auto base = input.value_offset(0);
  auto rebase_span = [&](int64_t i) -> storage::Span {
    auto begin = input.value_offset(i) - base;
    return {detail::narrow<storage::Index>(begin),
            detail::narrow<storage::Index>(begin + input.value_length(i))};
  };
  auto rows = std::views::iota(int64_t{0}, input.length());
  return copy_buffer(rows | std::views::transform(rebase_span));
}

template <class Tag, class Char, class ArrowArray>
auto import_bytes(ArrowArray const& input, ArrowBuffers& buffers)
  -> Array<Data> {
  auto data = storage::DataOwner<Char[]>{};
  if (input.length() != 0) {
    auto base = input.value_offset(0);
    auto size = input.value_offset(input.length()) - base;
    if (size != 0) {
      data = import_buffer<Char>(input.value_data(), base,
                                 detail::narrow<storage::Index>(size), buffers);
    }
  }
  return Array<Data>{Array<Tag>{typename Type<Tag>::PrimaryPhysicalStorage{
    std::move(data), copy_spans(input)}}};
}

/// The value at `index` of an array whose class the type id identified.
template <class Tag, class ArrowArray>
auto value_at(arrow::Array const& array, int64_t index) -> Data {
  return Data{Tag{static_cast<ArrowArray const&>(array).Value(index)}};
}

auto import_ips(ip_type::array_type const& input, ArrowBuffers& buffers)
  -> Array<Data> {
  auto bytes = input.storage();
  auto read_ip = [&](int64_t i) -> Ip {
    auto address_bytes
      = std::span<uint8_t const, 16>{bytes->raw_values() + i * 16, 16};
    return Ip::v6(address_bytes);
  };
  auto rows = std::views::iota(int64_t{0}, input.length());
  auto values = rows | std::views::transform(read_ip);
  auto result = Array<Ip>{storage::SparseStorage<Ip>{copy_buffer(values)}};
  return apply_validity(Array<Data>{std::move(result)}, input, buffers);
}

auto import_subnets(subnet_type::array_type const& input, ArrowBuffers& buffers)
  -> arrow::Result<Array<Data>> {
  auto fields = input.storage();
  auto const& networks = as<ip_type::array_type>(*fields->field(0));
  auto bytes = networks.storage();
  auto const& lengths
    = static_cast<arrow::UInt8Array const&>(*fields->field(1));
  auto rows = std::views::iota(int64_t{0}, input.length());
  // Arrow permits nullable struct children independently of parent validity.
  // Check them before reading payloads, but ignore children of null subnets.
  if (networks.null_count() != 0 or lengths.null_count() != 0) {
    for (auto i : rows) {
      if (input.IsValid(i) and (networks.IsNull(i) or lengths.IsNull(i))) {
        return arrow::Status::Invalid(
          "non-null subnet contains a null address or prefix length");
      }
    }
  }
  auto read_subnet = [&](int64_t i) -> Subnet {
    // Null slots need not contain a valid prefix length.
    if (input.IsNull(i)) {
      return {};
    }
    auto address_bytes
      = std::span<uint8_t const, 16>{bytes->raw_values() + i * 16, 16};
    auto network = Ip::v6(address_bytes);
    return Subnet{network, lengths.Value(i)};
  };
  auto values = rows | std::views::transform(read_subnet);
  auto result
    = Array<Subnet>{storage::SparseStorage<Subnet>{copy_buffer(values)}};
  return apply_validity(Array<Data>{std::move(result)}, input, buffers);
}

struct ArrowImporter {
  Option<Array<Data>> result;
  ArrowBuffers& buffers;

  static auto import_owned(std::shared_ptr<arrow::Array> input)
    -> arrow::Result<Array<Data>> {
    TENZIR_ASSERT(input);
    auto buffers = ArrowBuffers{};
    if (input.use_count() != 1) {
      return import(*input, buffers);
    }
    // Drop cached Arrow child arrays before checking ArrayData ownership.
    auto data = input->data();
    input.reset();
    buffers.collect(data);
    auto array = arrow::MakeArray(data);
    return import(*array, buffers);
  }

  static auto import(arrow::Array const& input, ArrowBuffers& buffers)
    -> arrow::Result<Array<Data>> {
    if (input.length() > std::numeric_limits<storage::Index>::max()) {
      return arrow::Status::CapacityError(
        "Arrow column exceeds the supported row limit");
    }
    auto visitor = ArrowImporter{.result = {}, .buffers = buffers};
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(input, &visitor));
    TENZIR_ASSERT(visitor.result);
    return std::move(*visitor.result);
  }

  template <class ArrowArray>
  auto Visit(ArrowArray const& input) -> arrow::Status {
    ARROW_ASSIGN_OR_RAISE(auto converted, convert(input));
    using ArrowType = typename ArrowArray::TypeClass;
    // These branches already represent nulls or delegate to another import.
    // In particular, dictionaries can add nulls from their dictionary values.
    if constexpr (not concepts::one_of<ArrowType, arrow::NullType,
                                       arrow::MapType, arrow::DictionaryType,
                                       arrow::ExtensionType>) {
      converted = apply_validity(std::move(converted), input, buffers);
    }
    result = std::move(converted);
    return arrow::Status::OK();
  }

  template <class ArrowArray>
  auto convert(ArrowArray const& input) -> arrow::Result<Array<Data>> {
    using ArrowType = typename ArrowArray::TypeClass;
    auto length = detail::narrow<storage::Index>(input.length());
    if constexpr (std::same_as<ArrowType, arrow::NullType>) {
      return Array<Data>{Array<Null>{storage::NullStorage{length}}};
    } else if constexpr (arrow::is_integer_type<ArrowType>::value
                         or concepts::one_of<ArrowType, arrow::FloatType,
                                             arrow::DoubleType>) {
      using Value = typename ArrowType::c_type;
      using Tag = std::conditional_t<
        std::is_floating_point_v<Value>, Float,
        std::conditional_t<std::is_signed_v<Value>, Int, UInt>>;
      // Keep Arrow's physical width; Nova widens only when accessing a value.
      auto values = import_buffer<Value>(
        input.values(), input.offset() * sizeof(Value), length, buffers);
      return Array<Data>{
        Array<Tag>{storage::SparseStorage<Value>{std::move(values)}}};
    } else if constexpr (std::same_as<ArrowType, arrow::BooleanType>) {
      return Array<Data>{Array<Bool>{
        import_bitmap(input.values(), input.offset(), length, buffers)}};
    } else if constexpr (std::same_as<ArrowType, arrow::StringType>) {
      return import_bytes<String, char>(input, buffers);
    } else if constexpr (std::same_as<ArrowType, arrow::BinaryType>) {
      return import_bytes<Blob, std::byte>(input, buffers);
    } else if constexpr (concepts::one_of<ArrowType, arrow::TimestampType,
                                          arrow::DurationType>) {
      if (as<ArrowType>(*input.type()).unit() != arrow::TimeUnit::NANO) {
        auto target = [&]() -> std::shared_ptr<arrow::DataType> {
          if constexpr (std::same_as<ArrowType, arrow::TimestampType>) {
            return arrow::timestamp(arrow::TimeUnit::NANO);
          } else {
            return arrow::duration(arrow::TimeUnit::NANO);
          }
        }();
        ARROW_ASSIGN_OR_RAISE(auto cast, arrow::compute::Cast(input, target));
        // Apply the unchanged validity bitmap once, in Visit().
        return convert(as<ArrowArray>(*cast));
      }
      using Tag
        = std::conditional_t<std::same_as<ArrowType, arrow::TimestampType>,
                             Time, Duration>;
      auto from_nanoseconds = [](int64_t value) -> Tag {
        return Tag{duration{value}};
      };
      auto raw = std::span{input.raw_values(), static_cast<size_t>(length)};
      auto values = raw | std::views::transform(from_nanoseconds);
      return Array<Data>{
        Array<Tag>{storage::SparseStorage<Tag>{copy_buffer(values)}}};
    } else if constexpr (std::same_as<ArrowType, arrow::StructType>) {
      auto names = Array<Record>::Names{};
      auto fields = Array<Record>::MaskedArrays{};
      auto shapes = ShapeTable{};
      auto shape = ShapeTable::empty_shape;
      fields.reserve(input.num_fields());
      for (auto i = 0; i < input.num_fields(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto values, import(*input.field(i), buffers));
        auto field = MaskedArray<Array<Data>>{std::move(values),
                                              storage::BitMap{length, true}};
        auto [entry, inserted] = names.try_emplace(
          storage::String<>{input.struct_type()->field(i)->name()},
          fields.size());
        if (inserted) {
          shape = shapes.with_field(
            shape, detail::narrow<storage::Index>(fields.size()));
          fields.push_back(std::move(field));
        } else {
          fields[entry->second] = std::move(field);
        }
      }
      // Arrow structs have one shape. Build it once instead of rewriting every
      // row's shape index for each added field.
      auto indices = storage::DataOwner<storage::Index[]>{};
      if (length != 0) {
        indices
          = storage::DataOwner<storage::Index[]>::make_value(length, shape);
      }
      return Array<Data>{
        Array<Record>{Array<Record>::IndicesStorage{std::move(indices)},
                      std::move(shapes), std::move(names), std::move(fields)}};
    } else if constexpr (std::same_as<ArrowType, arrow::ListType>) {
      auto base = length == 0 ? 0 : input.value_offset(0);
      auto end = length == 0 ? 0 : input.value_offset(input.length());
      ARROW_ASSIGN_OR_RAISE(
        auto values, import(*input.values()->Slice(base, end - base), buffers));
      return Array<Data>{Array<List>{copy_spans(input), std::move(values)}};
    } else if constexpr (std::same_as<ArrowType, arrow::MapType>) {
      // Map-to-record reshaping discovers field names per row.
      if (input.keys()->type_id() != arrow::Type::STRING
          or input.items()->type_id() != arrow::Type::STRING) {
        return arrow::Status::NotImplemented(
          "only string-to-string Arrow maps are supported");
      }
      auto const& keys = as<arrow::StringArray>(*input.keys());
      auto const& values = as<arrow::StringArray>(*input.items());
      auto builder = ArrayBuilder<Data>{};
      for (auto i = int64_t{0}; i < input.length(); ++i) {
        if (input.IsNull(i)) {
          builder.null();
          continue;
        }
        auto record = builder.record();
        auto end = input.value_offset(i) + input.value_length(i);
        for (auto j = input.value_offset(i); j < end; ++j) {
          if (keys.IsNull(j)) {
            continue;
          }
          auto field = record.field(keys.GetView(j));
          if (values.IsNull(j)) {
            field.null();
          } else {
            field.data(values.GetView(j));
          }
        }
      }
      return builder.finish();
    } else if constexpr (std::same_as<ArrowType, arrow::ExtensionType>) {
      if (auto* ips = dynamic_cast<ip_type::array_type const*>(&input)) {
        return import_ips(*ips, buffers);
      }
      if (auto* subnets
          = dynamic_cast<subnet_type::array_type const*>(&input)) {
        return import_subnets(*subnets, buffers);
      }
      if (auto* enums
          = dynamic_cast<enumeration_type::array_type const*>(&input)) {
        return import(*enums->storage(), buffers);
      }
    } else if constexpr (std::same_as<ArrowType, arrow::DictionaryType>) {
      if (auto value = constant_value(input)) {
        return repeat(*value, length);
      }
      ARROW_ASSIGN_OR_RAISE(
        auto decoded, arrow::compute::Cast(input, input.dictionary()->type()));
      return import_owned(std::move(decoded));
    }
    return arrow::Status::NotImplemented("unsupported arrow type `",
                                         input.type()->ToString(), "`");
  }

  /// The value that every row of a dictionary refers to, if they all refer to
  /// the same valid value. Readers keep columns as dictionaries where they
  /// expect a single value, which then becomes a constant instead of one copy
  /// per row.
  static auto constant_value(arrow::DictionaryArray const& input)
    -> Option<Data> {
    if (input.length() == 0 or input.null_count() != 0) {
      return None{};
    }
    auto const& dictionary = *input.dictionary();
    auto index = input.GetValueIndex(0);
    // Indices are in bounds, so all rows name the only value of a dictionary
    // that has one.
    if (dictionary.length() != 1) {
      for (auto i = int64_t{1}; i < input.length(); ++i) {
        if (input.GetValueIndex(i) != index) {
          return None{};
        }
      }
    }
    if (dictionary.IsNull(index)) {
      return None{};
    }
    switch (dictionary.type_id()) {
      case arrow::Type::STRING:
        return Data{String{as<arrow::StringArray>(dictionary).GetView(index)}};
      case arrow::Type::BINARY: {
        auto view = as<arrow::BinaryArray>(dictionary).GetView(index);
        auto const* bytes = reinterpret_cast<std::byte const*>(view.data());
        return Data{Blob{bytes, bytes + view.size()}};
      }
      case arrow::Type::BOOL:
        return value_at<Bool, arrow::BooleanArray>(dictionary, index);
      case arrow::Type::INT8:
        return value_at<Int, arrow::Int8Array>(dictionary, index);
      case arrow::Type::INT16:
        return value_at<Int, arrow::Int16Array>(dictionary, index);
      case arrow::Type::INT32:
        return value_at<Int, arrow::Int32Array>(dictionary, index);
      case arrow::Type::INT64:
        return value_at<Int, arrow::Int64Array>(dictionary, index);
      case arrow::Type::UINT8:
        return value_at<UInt, arrow::UInt8Array>(dictionary, index);
      case arrow::Type::UINT16:
        return value_at<UInt, arrow::UInt16Array>(dictionary, index);
      case arrow::Type::UINT32:
        return value_at<UInt, arrow::UInt32Array>(dictionary, index);
      case arrow::Type::UINT64:
        return value_at<UInt, arrow::UInt64Array>(dictionary, index);
      case arrow::Type::FLOAT:
        return value_at<Float, arrow::FloatArray>(dictionary, index);
      case arrow::Type::DOUBLE:
        return value_at<Float, arrow::DoubleArray>(dictionary, index);
      default: {
        // Import the one value that the rows refer to, which also converts
        // units and extension types. Values that fail to import leave the
        // error to the import of the whole column, and records and lists keep
        // their import per row.
        auto buffers = ArrowBuffers{};
        auto value = import(*dictionary.Slice(index, 1), buffers);
        if (not value.ok()) {
          return None{};
        }
        auto result = to_data(value->get(0));
        if (is<List>(result) or is<Record>(result)) {
          return None{};
        }
        return result;
      }
    }
  }
};

} // namespace

auto import_arrow_array(arrow::Array const& input)
  -> Result<Array<Data>, std::string> {
  auto buffers = ArrowBuffers{};
  auto result = ArrowImporter::import(input, buffers);
  if (not result.ok()) {
    return Err{result.status().message()};
  }
  return std::move(*result);
}

auto import_arrow_array(std::shared_ptr<arrow::Array> input)
  -> Result<Array<Data>, std::string> {
  auto result = ArrowImporter::import_owned(std::move(input));
  if (not result.ok()) {
    return Err{result.status().message()};
  }
  return std::move(*result);
}

} // namespace tenzir::nova
