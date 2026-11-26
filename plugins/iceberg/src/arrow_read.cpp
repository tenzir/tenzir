//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/arrow_read.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/arrow_import.hpp>
#include <tenzir/nova/bitmap.hpp>
#include <tenzir/nova/list_array.hpp>
#include <tenzir/nova/record_array.hpp>
#include <tenzir/nova/storage.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/option.hpp>
#include <tenzir/try.hpp>

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/compute/cast.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/bitmap_ops.h>
#include <arrow/util/key_value_metadata.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

using Index = nova::storage::Index;

constexpr auto arrow_extension_name_key
  = std::string_view{"ARROW:extension:name"};
constexpr auto arrow_uuid_extension_name = std::string_view{"arrow.uuid"};

auto is_uuid_field(arrow::Field const* field) -> bool {
  if (not field or not field->metadata()) {
    return false;
  }
  auto ext_name = field->metadata()->Get(arrow_extension_name_key);
  return ext_name.ok() and *ext_name == arrow_uuid_extension_name;
}

auto is_fast_path(arrow::DataType const& type, arrow::Field const* field)
  -> bool {
  switch (type.id()) {
    case arrow::Type::NA:
    case arrow::Type::BOOL:
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
      return true;
    case arrow::Type::TIMESTAMP:
      return static_cast<arrow::TimestampType const&>(type).unit()
             == arrow::TimeUnit::NANO;
    case arrow::Type::DURATION:
      return static_cast<arrow::DurationType const&>(type).unit()
             == arrow::TimeUnit::NANO;
    case arrow::Type::STRUCT: {
      auto const& struct_type = static_cast<arrow::StructType const&>(type);
      for (auto const& child : struct_type.fields()) {
        if (not is_fast_path(*child->type(), child.get())) {
          return false;
        }
      }
      return true;
    }
    case arrow::Type::LIST: {
      auto const& list_type = static_cast<arrow::ListType const&>(type);
      auto const& value_field = list_type.value_field();
      return is_fast_path(*value_field->type(), value_field.get());
    }
    case arrow::Type::DICTIONARY: {
      auto const& dict_type = static_cast<arrow::DictionaryType const&>(type);
      return is_fast_path(*dict_type.value_type(), field);
    }
    default:
      return false;
  }
}

auto batch_is_fast_path(arrow::Schema const& schema) -> bool {
  for (auto const& field : schema.fields()) {
    if (not is_fast_path(*field->type(), field.get())) {
      return false;
    }
  }
  return true;
}

auto add_warning(std::vector<std::string>& warnings, std::string message)
  -> void {
  if (std::ranges::find(warnings, message) == warnings.end()) {
    warnings.push_back(std::move(message));
  }
}

auto make_null_bitmap(arrow::Array const& input)
  -> Option<nova::storage::BitMap> {
  auto const null_count = input.null_count();
  if (null_count == 0) {
    return None{};
  }
  auto const length = detail::narrow<Index>(input.length());
  if (null_count == length) {
    return nova::storage::BitMap{length, true};
  }
  using Word = nova::storage::BitMap::Word;
  constexpr auto word_bits = nova::storage::BitMap::word_bits;
  auto const words = length / word_bits + (length % word_bits != 0);
  auto data = nova::storage::DataOwner<Word[]>::make_value(words, Word{0});
  auto const* bits = input.null_bitmap_data();
  TENZIR_ASSERT(bits != nullptr);
  auto* bytes = reinterpret_cast<uint8_t*>(data.begin());
  // Arrow handles unaligned slice offsets; zero-initialized backing storage
  // keeps bits past `length` clear through the final word boundary.
  arrow::internal::InvertBitmap(bits, input.offset(), length, bytes, 0);
  if constexpr (std::endian::native == std::endian::big) {
    for (auto& word : data) {
      word = std::byteswap(word);
    }
  }
  return nova::storage::BitMap{length, std::move(data),
                               detail::narrow<Index>(null_count)};
}

auto apply_validity(nova::Array<nova::Data> result, arrow::Array const& input,
                    Option<nova::storage::BitMap> extra_nulls = None{})
  -> nova::Array<nova::Data> {
  auto nulls = make_null_bitmap(input);
  if (extra_nulls) {
    nulls = nulls ? std::move(*nulls) | *extra_nulls : std::move(*extra_nulls);
  }
  if (nulls) {
    return std::move(result).null_where(std::move(*nulls));
  }
  return result;
}

auto time_unit_nanos_factor(arrow::TimeUnit::type unit) -> int64_t {
  switch (unit) {
    case arrow::TimeUnit::SECOND:
      return int64_t{1'000'000'000};
    case arrow::TimeUnit::MILLI:
      return int64_t{1'000'000};
    case arrow::TimeUnit::MICRO:
      return int64_t{1'000};
    case arrow::TimeUnit::NANO:
      return int64_t{1};
  }
  TENZIR_UNREACHABLE();
}

template <class Tag, class ArrowArray>
auto convert_scaled_temporal(ArrowArray const& input, int64_t factor,
                             std::string_view kind, std::string const& path,
                             Option<nova::storage::BitMap> const& parent_nulls,
                             std::vector<std::string>& warnings)
  -> nova::Array<nova::Data> {
  auto const length = detail::narrow<Index>(input.length());
  auto storage = typename nova::storage::SparseStorage<Tag>::Mutable{length};
  auto* target = storage.data();
  auto overflow = Option<nova::storage::BitMap::Mutable>{};
  for (auto i = Index{0}; i < length; ++i) {
    if (input.IsNull(i) or (parent_nulls and parent_nulls->get(i))) {
      continue;
    }
    auto nanos = int64_t{0};
    if (__builtin_mul_overflow(int64_t{input.Value(i)}, factor, &nanos)) {
      if (not overflow) {
        overflow.emplace(length);
      }
      overflow->set(i, true);
      continue;
    }
    target[i] = Tag{std::chrono::nanoseconds{nanos}};
  }
  auto overflow_mask = Option<nova::storage::BitMap>{};
  if (overflow) {
    add_warning(warnings,
                fmt::format("column `{}` contains {} values outside the "
                            "representable range; set to null",
                            path, kind));
    overflow_mask = std::move(*overflow).finish();
  }
  return apply_validity(
    nova::Array<nova::Data>{nova::Array<Tag>{std::move(storage).finish()}},
    input, std::move(overflow_mask));
}

auto format_uuid(uint8_t const* bytes, char* out) -> void {
  constexpr auto hex = std::string_view{"0123456789abcdef"};
  auto write_bytes = [&](int begin, int end) {
    for (auto i = begin; i < end; ++i) {
      *out++ = hex[static_cast<size_t>(bytes[i] >> 4)];
      *out++ = hex[static_cast<size_t>(bytes[i] & 0x0f)];
    }
  };
  write_bytes(0, 4);
  *out++ = '-';
  write_bytes(4, 6);
  *out++ = '-';
  write_bytes(6, 8);
  *out++ = '-';
  write_bytes(8, 10);
  *out++ = '-';
  write_bytes(10, 16);
}

auto convert_uuid(arrow::FixedSizeBinaryArray const& input,
                  std::string const& path)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  if (input.byte_width() != 16) {
    return tenzir::Err{
      fmt::format("column `{}` has invalid UUID byte width {} (expected 16)",
                  path, input.byte_width())};
  }
  auto const length = detail::narrow<Index>(input.length());
  auto const valid_count = int64_t{length} - input.null_count();
  constexpr auto uuid_length = int64_t{36};
  if (valid_count > std::numeric_limits<Index>::max() / uuid_length) {
    return tenzir::Err{
      fmt::format("column `{}` exceeds the maximum string buffer size", path)};
  }
  auto storage = nova::storage::DenseStringOffsetStorage::Mutable{length};
  storage.reserve_data(static_cast<Index>(valid_count * uuid_length));
  auto buf = std::array<char, 36>{};
  for (auto i = Index{0}; i < length; ++i) {
    if (input.IsNull(i)) {
      auto const ok = storage.set(i, std::string_view{});
      TENZIR_ASSERT(ok);
      continue;
    }
    format_uuid(input.Value(i), buf.data());
    auto const ok = storage.set(i, std::string_view{buf.data(), buf.size()});
    TENZIR_ASSERT(ok);
  }
  return apply_validity(nova::Array<nova::Data>{nova::Array<nova::String>{
                          std::move(storage).finish()}},
                        input);
}

auto convert_fixed_size_binary(arrow::FixedSizeBinaryArray const& input,
                               std::string const& path)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  auto const length = detail::narrow<Index>(input.length());
  auto const width = input.byte_width();
  auto const valid_count = int64_t{length} - input.null_count();
  if (width > 0 and valid_count > std::numeric_limits<Index>::max() / width) {
    return tenzir::Err{
      fmt::format("column `{}` exceeds the maximum blob buffer size", path)};
  }
  auto storage = nova::storage::DenseBlobOffsetStorage::Mutable{length};
  storage.reserve_data(
    static_cast<Index>(valid_count * static_cast<int64_t>(width)));
  for (auto i = Index{0}; i < length; ++i) {
    if (input.IsNull(i) or width == 0) {
      auto const ok = storage.set(i, nova::BlobView{});
      TENZIR_ASSERT(ok);
      continue;
    }
    auto const* raw = reinterpret_cast<std::byte const*>(input.Value(i));
    auto const ok
      = storage.set(i, nova::BlobView{raw, static_cast<size_t>(width)});
    TENZIR_ASSERT(ok);
  }
  return apply_validity(nova::Array<nova::Data>{nova::Array<nova::Blob>{
                          std::move(storage).finish()}},
                        input);
}

auto convert_large_string(arrow::LargeStringArray const& input,
                          std::string const& path)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  auto const length = detail::narrow<Index>(input.length());
  auto total_bytes = int64_t{0};
  for (auto i = Index{0}; i < length; ++i) {
    if (not input.IsNull(i)) {
      total_bytes += input.value_length(i);
      if (total_bytes > std::numeric_limits<Index>::max()) {
        return tenzir::Err{fmt::format(
          "column `{}` exceeds the maximum string buffer size", path)};
      }
    }
  }
  auto storage = nova::storage::DenseStringOffsetStorage::Mutable{length};
  storage.reserve_data(static_cast<Index>(total_bytes));
  for (auto i = Index{0}; i < length; ++i) {
    auto const ok = input.IsNull(i) ? storage.set(i, std::string_view{})
                                    : storage.set(i, input.GetView(i));
    TENZIR_ASSERT(ok);
  }
  return apply_validity(nova::Array<nova::Data>{nova::Array<nova::String>{
                          std::move(storage).finish()}},
                        input);
}

auto convert_large_binary(arrow::LargeBinaryArray const& input,
                          std::string const& path)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  auto const length = detail::narrow<Index>(input.length());
  auto total_bytes = int64_t{0};
  for (auto i = Index{0}; i < length; ++i) {
    if (not input.IsNull(i)) {
      total_bytes += input.value_length(i);
      if (total_bytes > std::numeric_limits<Index>::max()) {
        return tenzir::Err{fmt::format(
          "column `{}` exceeds the maximum blob buffer size", path)};
      }
    }
  }
  auto storage = nova::storage::DenseBlobOffsetStorage::Mutable{length};
  storage.reserve_data(static_cast<Index>(total_bytes));
  for (auto i = Index{0}; i < length; ++i) {
    auto ok = false;
    if (input.IsNull(i)) {
      ok = storage.set(i, nova::BlobView{});
    } else {
      auto const view = input.GetView(i);
      auto const bytes
        = view.empty()
            ? nova::BlobView{}
            : nova::BlobView{reinterpret_cast<std::byte const*>(view.data()),
                             view.size()};
      ok = storage.set(i, bytes);
    }
    TENZIR_ASSERT(ok);
  }
  return apply_validity(nova::Array<nova::Data>{nova::Array<nova::Blob>{
                          std::move(storage).finish()}},
                        input);
}

auto import_array(std::shared_ptr<arrow::Array> input,
                  arrow::Field const* field, std::string const& path,
                  ArrowReadOptions const& options,
                  std::vector<std::string>& warnings,
                  Option<nova::storage::BitMap> const& parent_nulls)
  -> tenzir::Result<nova::Array<nova::Data>, std::string>;

template <class ArrowListArray>
auto import_list_array(std::shared_ptr<arrow::Array> input,
                       std::string const& path, ArrowReadOptions const& options,
                       std::vector<std::string>& warnings)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  auto const& list = static_cast<ArrowListArray const&>(*input);
  auto const length = detail::narrow<Index>(list.length());
  auto nulls = make_null_bitmap(list);
  auto const base = length == 0 ? int64_t{0} : int64_t{list.value_offset(0)};
  auto const end
    = length == 0 ? int64_t{0} : int64_t{list.value_offset(length)};
  if (end - base > std::numeric_limits<Index>::max()) {
    return tenzir::Err{fmt::format(
      "column `{}` list elements exceed the supported row limit", path)};
  }
  auto spans
    = nova::storage::DataOwner<nova::storage::Span[]>::make_uninitialized(
      length);
  for (auto i = Index{0}; i < length; ++i) {
    if (list.IsNull(i)) {
      spans.emplace_back(nova::storage::Span{0, 0});
      continue;
    }
    auto const begin
      = detail::narrow<Index>(int64_t{list.value_offset(i)} - base);
    auto const stop
      = detail::narrow<Index>(int64_t{list.value_offset(i + 1)} - base);
    spans.emplace_back(nova::storage::Span{begin, stop});
  }
  auto value_field = list.list_type()->value_field();
  auto child = list.values()->Slice(base, end - base);
  // Release the parent list so fast-path child buffers can be adopted.
  input.reset();
  TRY(auto values, import_array(std::move(child), value_field.get(), path,
                                options, warnings, None{}));
  auto result = nova::Array<nova::Data>{
    nova::Array<nova::List>{std::move(spans).finish(), std::move(values)}};
  if (nulls) {
    return std::move(result).null_where(std::move(*nulls));
  }
  return result;
}

auto import_struct_array(std::shared_ptr<arrow::Array> input,
                         std::string const& path,
                         ArrowReadOptions const& options,
                         std::vector<std::string>& warnings,
                         Option<nova::storage::BitMap> const& parent_nulls)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  auto const& struct_array = static_cast<arrow::StructArray const&>(*input);
  auto const length = detail::narrow<Index>(struct_array.length());
  auto self_nulls = make_null_bitmap(struct_array);
  auto struct_type = std::static_pointer_cast<arrow::StructType>(input->type());
  auto children = struct_array.fields();
  // Release the parent struct so fast-path child columns can adopt buffers.
  input.reset();
  if (children.empty()) {
    auto result
      = nova::Array<nova::Data>{nova::Array<nova::Record>::make_empty(length)};
    if (self_nulls) {
      return std::move(result).null_where(std::move(*self_nulls));
    }
    return result;
  }
  auto child_parent_nulls = Option<nova::storage::BitMap>{};
  if (parent_nulls and self_nulls) {
    child_parent_nulls = *parent_nulls | *self_nulls;
  } else if (self_nulls) {
    child_parent_nulls = *self_nulls;
  } else if (parent_nulls) {
    child_parent_nulls = *parent_nulls;
  }
  auto fields = std::vector<
    std::pair<std::string_view, nova::Array<nova::Record>::MaskedArray>>{};
  fields.reserve(children.size());
  for (auto i = size_t{0}; i < children.size(); ++i) {
    auto const& child_field = struct_type->field(static_cast<int>(i));
    auto child_path = fmt::format("{}.{}", path, child_field->name());
    TRY(auto imported,
        import_array(std::move(children[i]), child_field.get(), child_path,
                     options, warnings, child_parent_nulls));
    fields.emplace_back(child_field->name(),
                        nova::Array<nova::Record>::MaskedArray{
                          std::move(imported),
                          nova::storage::BitMap{length, true},
                        });
  }
  auto result
    = nova::Array<nova::Data>{nova::Array<nova::Record>::from_fields(fields)};
  if (self_nulls) {
    return std::move(result).null_where(std::move(*self_nulls));
  }
  return result;
}

auto import_map_array(std::shared_ptr<arrow::Array> input,
                      std::string const& path, ArrowReadOptions const& options,
                      std::vector<std::string>& warnings)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  auto const& map = static_cast<arrow::MapArray const&>(*input);
  auto const length = detail::narrow<Index>(map.length());
  auto const base = length == 0 ? int64_t{0} : int64_t{map.value_offset(0)};
  auto const end = length == 0 ? int64_t{0} : int64_t{map.value_offset(length)};
  if (end - base > std::numeric_limits<Index>::max()) {
    return tenzir::Err{fmt::format(
      "column `{}` map entries exceed the supported row limit", path)};
  }
  auto key_field = map.map_type()->key_field();
  auto item_field = map.map_type()->item_field();
  auto keys = map.keys()->Slice(base, end - base);
  auto items = map.items()->Slice(base, end - base);
  auto const key_type_id = keys->type_id();
  if (key_type_id == arrow::Type::STRING
      or key_type_id == arrow::Type::LARGE_STRING) {
    auto row_nulls = make_null_bitmap(map);
    auto row_spans = std::vector<nova::storage::Span>{};
    row_spans.reserve(static_cast<size_t>(length));
    for (auto i = Index{0}; i < length; ++i) {
      if (map.IsNull(i)) {
        row_spans.push_back({0, 0});
      } else {
        row_spans.push_back({
          detail::narrow<Index>(int64_t{map.value_offset(i)} - base),
          detail::narrow<Index>(int64_t{map.value_offset(i + 1)} - base),
        });
      }
    }
    // Release the parent map before importing items.
    input.reset();
    TRY(auto converted_items, import_array(std::move(items), item_field.get(),
                                           path, options, warnings, None{}));
    auto reshape = [&](auto const& string_keys) -> nova::Array<nova::Data> {
      auto builder = nova::ArrayBuilder<nova::Data>{};
      for (auto i = Index{0}; i < length; ++i) {
        if (row_nulls and row_nulls->get(i)) {
          builder.null();
          continue;
        }
        auto record = builder.record();
        auto const span = row_spans[static_cast<size_t>(i)];
        for (auto j = span.begin; j < span.end; ++j) {
          if (string_keys.IsNull(j)) {
            continue;
          }
          nova::append_row(record.field(string_keys.GetView(j)),
                           converted_items.get(j));
        }
      }
      return builder.finish();
    };
    if (key_type_id == arrow::Type::STRING) {
      return reshape(static_cast<arrow::StringArray const&>(*keys));
    }
    return reshape(static_cast<arrow::LargeStringArray const&>(*keys));
  }
  // Non-string keys: reshape losslessly into a list of `{key, value}` records.
  auto nulls = make_null_bitmap(map);
  auto spans
    = nova::storage::DataOwner<nova::storage::Span[]>::make_uninitialized(
      length);
  for (auto i = Index{0}; i < length; ++i) {
    if (map.IsNull(i)) {
      spans.emplace_back(nova::storage::Span{0, 0});
      continue;
    }
    auto const begin
      = detail::narrow<Index>(int64_t{map.value_offset(i)} - base);
    auto const stop
      = detail::narrow<Index>(int64_t{map.value_offset(i + 1)} - base);
    spans.emplace_back(nova::storage::Span{begin, stop});
  }
  input.reset();
  auto const entry_count = detail::narrow<Index>(end - base);
  TRY(auto converted_keys,
      import_array(std::move(keys), key_field.get(),
                   fmt::format("{}.key", path), options, warnings, None{}));
  TRY(auto converted_items,
      import_array(std::move(items), item_field.get(),
                   fmt::format("{}.value", path), options, warnings, None{}));
  auto entry_fields = std::array<
    std::pair<std::string_view, nova::Array<nova::Record>::MaskedArray>, 2>{{
    {"key",
     {std::move(converted_keys), nova::storage::BitMap{entry_count, true}}},
    {"value",
     {std::move(converted_items), nova::storage::BitMap{entry_count, true}}},
  }};
  auto entries = nova::Array<nova::Data>{
    nova::Array<nova::Record>::from_fields(entry_fields)};
  auto result = nova::Array<nova::Data>{
    nova::Array<nova::List>{std::move(spans).finish(), std::move(entries)}};
  if (nulls) {
    return std::move(result).null_where(std::move(*nulls));
  }
  return result;
}

auto import_array(std::shared_ptr<arrow::Array> input,
                  arrow::Field const* field, std::string const& path,
                  ArrowReadOptions const& options,
                  std::vector<std::string>& warnings,
                  Option<nova::storage::BitMap> const& parent_nulls)
  -> tenzir::Result<nova::Array<nova::Data>, std::string> {
  TENZIR_ASSERT(input);
  if (input->length() > std::numeric_limits<Index>::max()) {
    return tenzir::Err{
      fmt::format("column `{}` exceeds the supported row limit", path)};
  }
  if (is_fast_path(*input->type(), field)) {
    return nova::import_arrow_array(std::move(input));
  }
  switch (input->type_id()) {
    case arrow::Type::DECIMAL32:
    case arrow::Type::DECIMAL64:
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256: {
      auto target_type = options.decimal_format == DecimalFormat::string
                           ? arrow::utf8()
                           : arrow::float64();
      auto cast = arrow::compute::Cast(*input, std::move(target_type));
      if (not cast.ok()) {
        return tenzir::Err{
          fmt::format("failed to convert decimal column `{}`: {}", path,
                      cast.status().ToStringWithoutContextLines())};
      }
      input.reset();
      return nova::import_arrow_array(std::move(*cast));
    }
    case arrow::Type::DATE32: {
      constexpr auto nanos_per_day = int64_t{86'400} * 1'000'000'000;
      return convert_scaled_temporal<nova::Time>(
        static_cast<arrow::Date32Array const&>(*input), nanos_per_day, "date",
        path, parent_nulls, warnings);
    }
    case arrow::Type::DATE64: {
      constexpr auto nanos_per_milli = int64_t{1'000'000};
      return convert_scaled_temporal<nova::Time>(
        static_cast<arrow::Date64Array const&>(*input), nanos_per_milli, "date",
        path, parent_nulls, warnings);
    }
    case arrow::Type::TIME32: {
      auto const& array = static_cast<arrow::Time32Array const&>(*input);
      auto const unit
        = static_cast<arrow::Time32Type const&>(*input->type()).unit();
      return convert_scaled_temporal<nova::Duration>(
        array, time_unit_nanos_factor(unit), "time", path, parent_nulls,
        warnings);
    }
    case arrow::Type::TIME64: {
      auto const& array = static_cast<arrow::Time64Array const&>(*input);
      auto const unit
        = static_cast<arrow::Time64Type const&>(*input->type()).unit();
      return convert_scaled_temporal<nova::Duration>(
        array, time_unit_nanos_factor(unit), "time", path, parent_nulls,
        warnings);
    }
    case arrow::Type::TIMESTAMP: {
      auto const& array = static_cast<arrow::TimestampArray const&>(*input);
      auto const unit
        = static_cast<arrow::TimestampType const&>(*input->type()).unit();
      return convert_scaled_temporal<nova::Time>(array,
                                                 time_unit_nanos_factor(unit),
                                                 "timestamp", path,
                                                 parent_nulls, warnings);
    }
    case arrow::Type::DURATION: {
      auto const& array = static_cast<arrow::DurationArray const&>(*input);
      auto const unit
        = static_cast<arrow::DurationType const&>(*input->type()).unit();
      return convert_scaled_temporal<nova::Duration>(
        array, time_unit_nanos_factor(unit), "duration", path, parent_nulls,
        warnings);
    }
    case arrow::Type::EXTENSION: {
      auto const& ext_array = static_cast<arrow::ExtensionArray const&>(*input);
      if (ext_array.extension_type()->extension_name()
          == arrow_uuid_extension_name) {
        auto const* bytes = dynamic_cast<arrow::FixedSizeBinaryArray const*>(
          ext_array.storage().get());
        if (not bytes) {
          return tenzir::Err{
            fmt::format("column `{}` has invalid arrow.uuid storage type `{}`",
                        path, ext_array.storage()->type()->ToString())};
        }
        return convert_uuid(*bytes, path);
      }
      return nova::import_arrow_array(std::move(input));
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      auto const& fsb = static_cast<arrow::FixedSizeBinaryArray const&>(*input);
      if (fsb.byte_width() == 16 and is_uuid_field(field)) {
        return convert_uuid(fsb, path);
      }
      return convert_fixed_size_binary(fsb, path);
    }
    case arrow::Type::LARGE_STRING:
      return convert_large_string(
        static_cast<arrow::LargeStringArray const&>(*input), path);
    case arrow::Type::LARGE_BINARY:
      return convert_large_binary(
        static_cast<arrow::LargeBinaryArray const&>(*input), path);
    case arrow::Type::LIST:
      return import_list_array<arrow::ListArray>(std::move(input), path,
                                                 options, warnings);
    case arrow::Type::LARGE_LIST:
      return import_list_array<arrow::LargeListArray>(std::move(input), path,
                                                      options, warnings);
    case arrow::Type::STRUCT:
      return import_struct_array(std::move(input), path, options, warnings,
                                 parent_nulls);
    case arrow::Type::MAP:
      return import_map_array(std::move(input), path, options, warnings);
    case arrow::Type::DICTIONARY: {
      auto const& dict_type
        = static_cast<arrow::DictionaryType const&>(*input->type());
      auto decoded = arrow::compute::Cast(*input, dict_type.value_type());
      if (not decoded.ok()) {
        return tenzir::Err{
          fmt::format("failed to decode dictionary column `{}`: {}", path,
                      decoded.status().ToStringWithoutContextLines())};
      }
      input.reset();
      return import_array(std::move(*decoded), field, path, options, warnings,
                          parent_nulls);
    }
    default:
      return nova::import_arrow_array(std::move(input));
  }
}

} // namespace

auto import_iceberg_batch(std::shared_ptr<arrow::RecordBatch> batch,
                          ArrowReadOptions const& options)
  -> tenzir::Result<ArrowReadBatch, std::string> {
  if (not batch) {
    return tenzir::Err{"Arrow batch is null"};
  }
  if (batch->num_rows() > std::numeric_limits<Index>::max()) {
    return tenzir::Err{"Arrow batch exceeds the supported row limit"};
  }
  if (batch->num_rows() == 0) {
    return ArrowReadBatch{
      .events = nova::Events{
        nova::Array<nova::Record>::make_empty(0),
        nova::storage::BitMap{0, false},
        nova::Events::Meta::make_empty(0, options.schema_name),
      },
      .warnings = {},
    };
  }
  auto const rows = detail::narrow<Index>(batch->num_rows());
  if (batch->num_columns() == 0) {
    return ArrowReadBatch{
      .events = nova::Events{
        nova::Array<nova::Record>::make_empty(rows),
        nova::storage::BitMap{rows, true},
        nova::Events::Meta::make_empty(rows, options.schema_name),
      },
      .warnings = {},
    };
  }
  if (batch_is_fast_path(*batch->schema())) {
    auto struct_array = batch->ToStructArray();
    if (not struct_array.ok()) {
      return tenzir::Err{struct_array.status().ToStringWithoutContextLines()};
    }
    // Release batch aliases before handing ownership to `import_arrow_array`.
    batch.reset();
    TRY(auto imported, nova::import_arrow_array(std::move(*struct_array)));
    auto records = imported.try_as<nova::Record>();
    TENZIR_ASSERT(records);
    return ArrowReadBatch{
      .events = nova::Events{
        std::move(*records),
        nova::storage::BitMap{rows, true},
        nova::Events::Meta::make_empty(rows, options.schema_name),
      },
      .warnings = {},
    };
  }
  auto schema = batch->schema();
  auto columns = batch->columns();
  // Release batch aliases so fast-path columns can adopt Arrow buffers directly.
  batch.reset();
  auto warnings = std::vector<std::string>{};
  auto fields = std::vector<
    std::pair<std::string_view, nova::Array<nova::Record>::MaskedArray>>{};
  fields.reserve(columns.size());
  for (auto i = size_t{0}; i < columns.size(); ++i) {
    auto const& field = schema->field(static_cast<int>(i));
    TRY(auto imported, import_array(std::move(columns[i]), field.get(),
                                    field->name(), options, warnings, None{}));
    fields.emplace_back(field->name(), nova::Array<nova::Record>::MaskedArray{
                                         std::move(imported),
                                         nova::storage::BitMap{rows, true},
                                       });
  }
  return ArrowReadBatch{
    .events = nova::Events{
      nova::Array<nova::Record>::from_fields(fields),
      nova::storage::BitMap{rows, true},
      nova::Events::Meta::make_empty(rows, options.schema_name),
    },
    .warnings = std::move(warnings),
  };
}

} // namespace tenzir::plugins::iceberg
