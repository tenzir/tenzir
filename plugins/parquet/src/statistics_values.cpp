//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/statistics_values.hpp"

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/type.hpp>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/extension_type.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <string>
#include <utility>

namespace tenzir::plugins::parquet {

namespace {

/// The unit of the timestamps that a column stores, if it stores any.
auto stored_unit(::parquet::ColumnDescriptor const& column)
  -> Option<arrow::TimeUnit::type> {
  auto const& logical = *column.logical_type();
  if (not logical.is_timestamp()) {
    return None{};
  }
  switch (
    static_cast<::parquet::TimestampLogicalType const&>(logical).time_unit()) {
    case ::parquet::LogicalType::TimeUnit::MILLIS:
      return arrow::TimeUnit::MILLI;
    case ::parquet::LogicalType::TimeUnit::MICROS:
      return arrow::TimeUnit::MICRO;
    case ::parquet::LogicalType::TimeUnit::NANOS:
      return arrow::TimeUnit::NANO;
    default:
      return None{};
  }
}

} // namespace

auto statistics_bound(::parquet::Statistics const& stats, bool upper)
  -> Option<PhysicalValue> {
  if (not stats.HasMinMax()) {
    return None{};
  }
  auto bound = [&]<class Type>() -> auto const& {
    auto const& typed
      = static_cast<::parquet::TypedStatistics<Type> const&>(stats);
    return upper ? typed.max() : typed.min();
  };
  switch (stats.physical_type()) {
    case ::parquet::Type::BOOLEAN:
      return PhysicalValue{bound.operator()<::parquet::BooleanType>()};
    case ::parquet::Type::INT32:
      return PhysicalValue{bound.operator()<::parquet::Int32Type>()};
    case ::parquet::Type::INT64:
      return PhysicalValue{bound.operator()<::parquet::Int64Type>()};
    case ::parquet::Type::FLOAT:
      return PhysicalValue{bound.operator()<::parquet::FloatType>()};
    case ::parquet::Type::DOUBLE:
      return PhysicalValue{bound.operator()<::parquet::DoubleType>()};
    case ::parquet::Type::BYTE_ARRAY: {
      auto const& x = bound.operator()<::parquet::ByteArrayType>();
      return PhysicalValue{
        std::string_view{reinterpret_cast<char const*>(x.ptr), x.len}};
    }
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
      auto const& x = bound.operator()<::parquet::FLBAType>();
      auto length = static_cast<size_t>(stats.descr()->type_length());
      return PhysicalValue{
        std::string_view{reinterpret_cast<char const*>(x.ptr), length}};
    }
    default:
      // TODO: Legacy INT96 timestamps, whose statistics Arrow never reads.
      return None{};
  }
}

auto decode_value(::parquet::ColumnDescriptor const& column,
                  std::shared_ptr<::arrow::DataType> const& restored,
                  PhysicalValue const& value)
  -> Option<std::shared_ptr<::arrow::Array>> {
  auto make = [](std::shared_ptr<arrow::DataType> type,
                 auto x) -> Option<std::shared_ptr<arrow::Array>> {
    auto scalar = arrow::MakeScalar(std::move(type), std::move(x));
    if (not scalar.ok()) {
      return None{};
    }
    auto array = arrow::MakeArrayFromScalar(**scalar, 1, arrow_memory_pool());
    if (not array.ok()) {
      return None{};
    }
    return array.MoveValueUnsafe();
  };
  // Like the reader, which copies Parquet's integers into narrower ones, but
  // rejecting values that would wrap around.
  auto narrow
    = [&]<class T>(int32_t x) -> Option<std::shared_ptr<arrow::Array>> {
    if (not std::in_range<T>(x)) {
      return None{};
    }
    return make(restored, static_cast<T>(x));
  };
  auto bytes = [](std::string_view x) {
    return arrow::Buffer::FromString(std::string{x});
  };
  // Decimals are big-endian two's complement integers of any width.
  auto decimal
    = [&](std::string_view x) -> Option<std::shared_ptr<arrow::Array>> {
    auto parsed = arrow::Decimal128::FromBigEndian(
      reinterpret_cast<uint8_t const*>(x.data()),
      static_cast<int32_t>(x.size()));
    if (not parsed.ok()) {
      return None{};
    }
    return make(restored, *parsed);
  };
  auto id = restored->id();
  switch (column.physical_type()) {
    case ::parquet::Type::BOOLEAN:
      if (id != arrow::Type::BOOL) {
        return None{};
      }
      return make(restored, as<bool>(value));
    case ::parquet::Type::INT32: {
      auto x = as<int32_t>(value);
      switch (id) {
        case arrow::Type::INT8:
          return narrow.operator()<int8_t>(x);
        case arrow::Type::INT16:
          return narrow.operator()<int16_t>(x);
        case arrow::Type::INT32:
          return make(restored, x);
        case arrow::Type::UINT8:
          return narrow.operator()<uint8_t>(x);
        case arrow::Type::UINT16:
          return narrow.operator()<uint16_t>(x);
        case arrow::Type::UINT32:
          // Parquet stores unsigned integers in the bits of signed ones.
          return make(restored, static_cast<uint32_t>(x));
        case arrow::Type::DECIMAL128:
          return make(restored, arrow::Decimal128{int64_t{x}});
        default:
          // TODO: Dates and times of day, which the import rejects.
          return None{};
      }
    }
    case ::parquet::Type::INT64: {
      auto x = as<int64_t>(value);
      switch (id) {
        case arrow::Type::INT64:
        case arrow::Type::DURATION:
          return make(restored, x);
        case arrow::Type::TIMESTAMP:
          // The reader labels the stored integers with the restored type.
          // Arrow keeps the stored unit even if an embedded schema says
          // otherwise, so `timestamp[s]` reads back as milliseconds. Should
          // the units ever differ, the reader may convert the values.
          if (stored_unit(column)
              != static_cast<arrow::TimestampType const&>(*restored).unit()) {
            return None{};
          }
          return make(restored, x);
        case arrow::Type::UINT64:
          return make(restored, static_cast<uint64_t>(x));
        case arrow::Type::DECIMAL128:
          return make(restored, arrow::Decimal128{x});
        default:
          return None{};
      }
    }
    case ::parquet::Type::FLOAT:
      if (id != arrow::Type::FLOAT) {
        return None{};
      }
      return make(restored, as<float>(value));
    case ::parquet::Type::DOUBLE:
      if (id != arrow::Type::DOUBLE) {
        return None{};
      }
      return make(restored, as<double>(value));
    case ::parquet::Type::BYTE_ARRAY: {
      auto x = as<std::string_view>(value);
      // The reader keeps dictionaries of byte arrays, which the import resolves
      // to their values.
      auto type = restored;
      if (id == arrow::Type::DICTIONARY) {
        type
          = static_cast<arrow::DictionaryType const&>(*restored).value_type();
      } else if (dynamic_cast<enumeration_type::arrow_type const*>(
                   restored.get())) {
        type = arrow::utf8();
      }
      switch (type->id()) {
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
          return make(std::move(type), bytes(x));
        case arrow::Type::DECIMAL128:
          return decimal(x);
        default:
          // Large and view layouts, which the import rejects.
          return None{};
      }
    }
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
      auto x = as<std::string_view>(value);
      if (id == arrow::Type::DECIMAL128) {
        return decimal(x);
      }
      if (dynamic_cast<ip_type::arrow_type const*>(restored.get())
          and x.size() == 16) {
        auto storage = make(arrow::fixed_size_binary(16), bytes(x));
        if (not storage) {
          return None{};
        }
        return arrow::ExtensionType::WrapArray(restored, *storage);
      }
      // TODO: Half floats and other fixed-size binaries, which the import
      // rejects.
      return None{};
    }
    default:
      return None{};
  }
}

} // namespace tenzir::plugins::parquet
