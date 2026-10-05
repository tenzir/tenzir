//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_to_events.hpp"

#include "clickhouse/block_decoding.hpp"
#include "clickhouse/column_variant_traits.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/record_array.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tag.hpp"
#include "tenzir/value_path.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/json.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::clickhouse {

namespace {

using nova::storage::Index;
using ConstColumnRef = std::shared_ptr<::clickhouse::Column const>;

/// Warns about the malformed values of one column, at most once.
class MalformedWarning {
public:
  MalformedWarning(value_path const& path, diagnostic_handler& dh)
    : path_{path}, dh_{dh} {
  }

  template <class... Ts>
  auto emit(fmt::format_string<Ts...> str, Ts&&... xs) -> void {
    if (emitted_) {
      return;
    }
    emitted_ = true;
    diagnostic::warning("malformed ClickHouse values in `{}`: {}", path_,
                        fmt::format(str, std::forward<Ts>(xs)...))
      .emit(dh_);
  }

private:
  value_path const& path_;
  diagnostic_handler& dh_;
  bool emitted_ = false;
};

template <class T>
auto raw_data(::clickhouse::ColumnVector<T> const& values) -> T const* {
  return values.Size() == 0 ? nullptr : &values.At(0);
}

/// Packs `length` bytes, one per bit and nonzero for set, into a bitmap.
auto bitmap_from_bytes(uint8_t const* bytes, Index length)
  -> nova::storage::BitMap {
  using nova::storage::BitMap;
  auto const true_count
    = detail::narrow<Index>(std::count_if(bytes, bytes + length, [](uint8_t x) {
        return x != 0;
      }));
  if (true_count == 0 or true_count == length) {
    return BitMap{length, true_count != 0};
  }
  constexpr auto word_bits = BitMap::word_bits;
  auto const words = length / word_bits + (length % word_bits != 0);
  auto data = nova::storage::DataOwner<BitMap::Word[]>::make_value(
    words, BitMap::Word{0});
  for (auto w = Index{0}; w < words; ++w) {
    auto const begin = w * word_bits;
    auto const end = std::min(begin + word_bits, length);
    auto word = BitMap::Word{0};
    for (auto i = begin; i < end; ++i) {
      word |= BitMap::Word{bytes[i] != 0} << (i - begin);
    }
    data.begin()[w] = word;
  }
  return BitMap{length, std::move(data), true_count};
}

/// The rows of a column that become `null`.
class NullRows {
public:
  explicit NullRows(Index length) : length_{length} {
  }

  /// Takes the rows that `nulls`, the null map of a `Nullable` column, marks.
  NullRows(Index length, ::clickhouse::ColumnUInt8 const& nulls)
    : length_{length} {
    auto bits = bitmap_from_bytes(raw_data(nulls), length);
    if (bits.any()) {
      bits_.emplace(std::move(bits));
    }
  }

  auto contains(Index row) const -> bool {
    return bits_ and bits_->get(row);
  }

  auto set(Index row) -> void {
    if (not bits_) {
      bits_.emplace(length_);
    }
    bits_->set(row, true);
  }

  auto set_all() -> void {
    bits_.emplace(nova::storage::BitMap{length_, true});
  }

  auto apply(nova::Array<nova::Data> array) && -> nova::Array<nova::Data> {
    if (not bits_) {
      return array;
    }
    return std::move(array).null_where(std::move(*bits_).finish());
  }

private:
  Index length_;
  Option<nova::storage::BitMap::Mutable> bits_;
};

auto make_nulls(Index length) -> nova::Array<nova::Data> {
  return nova::Array<nova::Null>{nova::storage::NullStorage{length}};
}

/// Copies a numeric column without widening it.
template <class T>
auto copy_numbers(::clickhouse::ColumnVector<T> const& values, Index length)
  -> nova::Array<nova::Data> {
  auto storage = typename nova::storage::SparseStorage<T>::Mutable{length};
  if (length > 0) {
    std::memcpy(storage.data(), raw_data(values),
                static_cast<size_t>(length) * sizeof(T));
  }
  if constexpr (std::floating_point<T>) {
    return nova::Array<nova::Float>{std::move(storage).finish()};
  } else if constexpr (std::signed_integral<T>) {
    return nova::Array<nova::Int>{std::move(storage).finish()};
  } else {
    return nova::Array<nova::UInt>{std::move(storage).finish()};
  }
}

/// Decodes the rows of a column one by one. `decode(row)` returns the value
/// of a row, or `None` if it does not convert. Rows in `nulls` are skipped,
/// and rows without a value are added to it.
template <class Tag, class F>
auto decode_rows(Index length, NullRows& nulls, MalformedWarning& warning,
                 F decode) -> nova::Array<nova::Data> {
  using Storage = typename nova::Type<Tag>::PrimaryPhysicalStorage;
  using View = typename nova::Type<Tag>::ViewType;
  constexpr auto dense
    = std::same_as<Tag, nova::String> or std::same_as<Tag, nova::Blob>;
  auto storage = typename Storage::Mutable{length};
  auto skip = [&]([[maybe_unused]] Index row) {
    // Dense storages need a valid range in every row, even a null one.
    if constexpr (dense) {
      auto set = storage.set(row, View{});
      TENZIR_ASSERT(set);
    }
  };
  for (auto row = Index{0}; row < length; ++row) {
    if (nulls.contains(row)) {
      skip(row);
      continue;
    }
    auto value = decode(row);
    if (not value) {
      nulls.set(row);
      skip(row);
      continue;
    }
    if (not storage.set(row, View{*value})) {
      warning.emit("values exceed the maximum size of {} bytes per block",
                   std::numeric_limits<Index>::max());
      nulls.set(row);
      skip(row);
    }
  }
  return nova::Array<Tag>{std::move(storage).finish()};
}

auto time_from_nanos(Option<int64_t> nanos) -> Option<nova::Time> {
  return nanos.transform([](int64_t x) {
    return nova::Time{std::chrono::nanoseconds{x}};
  });
}

auto duration_from_nanos(Option<int64_t> nanos) -> Option<nova::Duration> {
  return nanos.transform([](int64_t x) {
    return nova::Duration{x};
  });
}

auto ip_from_v4(in_addr address) -> nova::Ip {
  return ip::v4(
    std::span<uint8_t const, 4>{reinterpret_cast<uint8_t const*>(&address), 4});
}

auto ip_from_v6(in6_addr address) -> nova::Ip {
  return ip::v6(std::span<uint8_t const, 16>{
    reinterpret_cast<uint8_t const*>(&address), 16});
}

/// Decodes a `LowCardinality` column through its dictionary items.
auto decode_low_cardinality(::clickhouse::ColumnLowCardinality const& values,
                            Index length, NullRows& nulls,
                            MalformedWarning& warning)
  -> nova::Array<nova::Data> {
  auto logical = unwrap_type(values.Type());
  auto item = [&](Index row) {
    return values.GetItem(detail::narrow<size_t>(row));
  };
  if (logical.nullable) {
    for (auto row = Index{0}; row < length; ++row) {
      if (item(row).type == ::clickhouse::Type::Void) {
        nulls.set(row);
      }
    }
  }
  auto rows = [&]<class Tag>(tag<Tag>, auto decode) {
    return decode_rows<Tag>(length, nulls, warning, decode);
  };
  auto unsigned_rows = [&]<class T>(tag<T>) {
    return rows(tag_v<nova::UInt>, [&](Index row) {
      return Option{uint64_t{item(row).get<T>()}};
    });
  };
  auto signed_rows = [&]<class T>(tag<T>) {
    return rows(tag_v<nova::Int>, [&](Index row) {
      return Option{int64_t{item(row).get<T>()}};
    });
  };
  auto time_rows = [&](auto to_nanos, std::string_view name) {
    return rows(tag_v<nova::Time>, [&, name](Index row) {
      auto result = time_from_nanos(to_nanos(item(row)));
      if (not result) {
        warning.emit("{} value is out of range after rescaling to nanoseconds",
                     name);
      }
      return result;
    });
  };
  auto duration_rows = [&](auto to_nanos, std::string_view name) {
    return rows(tag_v<nova::Duration>, [&, name](Index row) {
      auto result = duration_from_nanos(to_nanos(item(row)));
      if (not result) {
        warning.emit("{} value is out of range after rescaling to nanoseconds",
                     name);
      }
      return result;
    });
  };
  switch (logical.type->GetCode()) {
    case ::clickhouse::Type::Void:
      return make_nulls(length);
    case ::clickhouse::Type::Bool:
      return rows(tag_v<nova::Bool>, [&](Index row) {
        return Option{item(row).get<bool>()};
      });
    case ::clickhouse::Type::UInt8:
      return unsigned_rows(tag_v<uint8_t>);
    case ::clickhouse::Type::UInt16:
      return unsigned_rows(tag_v<uint16_t>);
    case ::clickhouse::Type::UInt32:
      return unsigned_rows(tag_v<uint32_t>);
    case ::clickhouse::Type::UInt64:
      return unsigned_rows(tag_v<uint64_t>);
    case ::clickhouse::Type::Int8:
      return signed_rows(tag_v<int8_t>);
    case ::clickhouse::Type::Int16:
      return signed_rows(tag_v<int16_t>);
    case ::clickhouse::Type::Int32:
      return signed_rows(tag_v<int32_t>);
    case ::clickhouse::Type::Int64:
      return signed_rows(tag_v<int64_t>);
    case ::clickhouse::Type::Float32:
      return rows(tag_v<nova::Float>, [&](Index row) {
        return Option{double{item(row).get<float>()}};
      });
    case ::clickhouse::Type::Float64:
      return rows(tag_v<nova::Float>, [&](Index row) {
        return Option{item(row).get<double>()};
      });
    case ::clickhouse::Type::String:
    case ::clickhouse::Type::FixedString:
      return rows(tag_v<nova::String>, [&](Index row) {
        return Option{item(row).get<std::string_view>()};
      });
    case ::clickhouse::Type::UUID:
      return rows(tag_v<nova::String>, [&](Index row) {
        return Option{format_uuid(item(row).AsBinaryData())};
      });
    case ::clickhouse::Type::Enum8: {
      auto const* enum_type = logical.type->As<::clickhouse::EnumType>();
      return rows(tag_v<nova::String>, [&](Index row) {
        return Option{enum_type->GetEnumName(item(row).get<int8_t>())};
      });
    }
    case ::clickhouse::Type::Enum16: {
      auto const* enum_type = logical.type->As<::clickhouse::EnumType>();
      return rows(tag_v<nova::String>, [&](Index row) {
        return Option{enum_type->GetEnumName(item(row).get<int16_t>())};
      });
    }
    case ::clickhouse::Type::Int128:
      return rows(tag_v<nova::String>, [&](Index row) {
        return Option{format_int128(item(row).get<::clickhouse::Int128>())};
      });
    case ::clickhouse::Type::UInt128:
      return rows(tag_v<nova::String>, [&](Index row) {
        return Option{format_uint128(item(row).get<::clickhouse::UInt128>())};
      });
    case ::clickhouse::Type::Date:
      return time_rows(
        [](::clickhouse::ItemView view) {
          return days_to_nanos(view.get<uint16_t>());
        },
        "Date");
    case ::clickhouse::Type::Date32:
      return time_rows(
        [](::clickhouse::ItemView view) {
          return days_to_nanos(view.get<int32_t>());
        },
        "Date32");
    case ::clickhouse::Type::DateTime:
      return time_rows(
        [](::clickhouse::ItemView view) {
          return seconds_to_nanos(view.get<uint32_t>());
        },
        "DateTime");
    case ::clickhouse::Type::DateTime64: {
      auto precision
        = logical.type->As<::clickhouse::DateTime64Type>()->GetPrecision();
      return time_rows(
        [precision](::clickhouse::ItemView view) {
          return rescale_decimal_to_nanos(view.get<int64_t>(), precision);
        },
        "DateTime64");
    }
    case ::clickhouse::Type::Time:
      return duration_rows(
        [](::clickhouse::ItemView view) {
          return seconds_to_nanos(view.get<int32_t>());
        },
        "Time");
    case ::clickhouse::Type::Time64: {
      auto precision
        = logical.type->As<::clickhouse::Time64Type>()->GetPrecision();
      return duration_rows(
        [precision](::clickhouse::ItemView view) {
          return rescale_decimal_to_nanos(view.get<int64_t>(), precision);
        },
        "Time64");
    }
    case ::clickhouse::Type::Decimal:
    case ::clickhouse::Type::Decimal32:
    case ::clickhouse::Type::Decimal64:
    case ::clickhouse::Type::Decimal128: {
      auto scale = logical.type->As<::clickhouse::DecimalType>()->GetScale();
      return rows(tag_v<nova::String>, [&](Index row) {
        auto result = decimal_from_bytes(item(row).AsBinaryData(), scale);
        if (not result) {
          warning.emit("expected decimal payload width of 4, 8, or 16 bytes");
        }
        return result;
      });
    }
    // The client rejects `LowCardinality` addresses before they get here.
    case ::clickhouse::Type::IPv4:
    case ::clickhouse::Type::IPv6:
    case ::clickhouse::Type::Nullable:
    case ::clickhouse::Type::Array:
    case ::clickhouse::Type::Tuple:
    case ::clickhouse::Type::LowCardinality:
    case ::clickhouse::Type::Map:
    case ::clickhouse::Type::Point:
    case ::clickhouse::Type::Ring:
    case ::clickhouse::Type::Polygon:
    case ::clickhouse::Type::MultiPolygon:
    case ::clickhouse::Type::JSON:
      break;
  }
  warning.emit("unsupported LowCardinality ClickHouse type `{}`",
               logical.type->GetName());
  nulls.set_all();
  return make_nulls(length);
}

auto column_to_array(ConstColumnRef const& column, DecodedType const& type,
                     value_path const& path, diagnostic_handler& dh)
  -> Option<nova::Array<nova::Data>>;

auto decode_blobs(::clickhouse::ColumnArray const& values, Index length,
                  NullRows& nulls, MalformedWarning& warning)
  -> nova::Array<nova::Data> {
  auto bytes = values.GetData()->As<::clickhouse::ColumnUInt8>();
  if (not bytes) {
    warning.emit("expected Array(UInt8) data");
    nulls.set_all();
    return make_nulls(length);
  }
  auto const* data = reinterpret_cast<std::byte const*>(raw_data(*bytes));
  return decode_rows<nova::Blob>(length, nulls, warning, [&](Index row) {
    auto offset = values.GetOffset(detail::narrow<size_t>(row));
    auto size = values.GetSize(detail::narrow<size_t>(row));
    return Option{blob_view{data + offset, size}};
  });
}

auto decode_list(::clickhouse::ColumnArray const& values,
                 DecodedType const& type, Index length, value_path const& path,
                 diagnostic_handler& dh) -> Option<nova::Array<nova::Data>> {
  auto elements
    = column_to_array(values.GetData(), type.children[0], path.list(), dh);
  if (not elements) {
    return None{};
  }
  auto spans
    = nova::storage::DataOwner<nova::storage::Span[]>::make_uninitialized(
      length);
  for (auto row = Index{0}; row < length; ++row) {
    auto begin = values.GetOffset(detail::narrow<size_t>(row));
    auto end = begin + values.GetSize(detail::narrow<size_t>(row));
    spans.emplace_back(nova::storage::Span{detail::narrow<Index>(begin),
                                           detail::narrow<Index>(end)});
  }
  return nova::Array<nova::List>{spans.finish(), std::move(*elements)};
}

auto decode_tuple(::clickhouse::ColumnTuple const& values,
                  DecodedType const& type, Index length, value_path const& path,
                  diagnostic_handler& dh) -> Option<nova::Array<nova::Data>> {
  if (values.TupleSize() != type.names.size()) {
    diagnostic::warning("malformed ClickHouse values in `{}`: unexpected "
                        "tuple size",
                        path)
      .emit(dh);
    return None{};
  }
  auto arrays = std::vector<nova::Array<nova::Data>>{};
  arrays.reserve(type.names.size());
  for (auto i = size_t{0}; i < type.names.size(); ++i) {
    auto field_path = path.field(type.names[i]);
    auto array
      = column_to_array(values.At(i), type.children[i], field_path, dh);
    if (not array) {
      return None{};
    }
    if (array->length() != length) {
      diagnostic::warning("malformed ClickHouse values in `{}`: unexpected "
                          "tuple field length",
                          field_path)
        .emit(dh);
      return None{};
    }
    arrays.push_back(std::move(*array));
  }
  auto fields = std::vector<
    std::pair<std::string_view, nova::Array<nova::Record>::MaskedArray>>{};
  fields.reserve(arrays.size());
  for (auto i = size_t{0}; i < arrays.size(); ++i) {
    fields.emplace_back(type.names[i], nova::Array<nova::Record>::MaskedArray{
                                         std::move(arrays[i]),
                                         nova::storage::BitMap{length, true},
                                       });
  }
  return nova::Array<nova::Record>::from_fields(fields);
}

auto column_to_array(ConstColumnRef const& column, DecodedType const& type,
                     value_path const& path, diagnostic_handler& dh)
  -> Option<nova::Array<nova::Data>> {
  auto const length = detail::narrow<Index>(column->Size());
  auto nulls = NullRows{length};
  auto effective = column;
  if (auto nullable = column->As<::clickhouse::ColumnNullable>()) {
    if (auto map = nullable->Nulls()->As<::clickhouse::ColumnUInt8>()) {
      nulls = NullRows{length, *map};
    }
    effective = nullable->Nested();
  }
  auto warning = MalformedWarning{path, dh};
  auto strings = [&](auto const& values) {
    return decode_rows<nova::String>(length, nulls, warning, [&](Index row) {
      return Option{values.At(detail::narrow<size_t>(row))};
    });
  };
  auto times = [&](auto const& values, auto to_nanos, std::string_view name) {
    return decode_rows<nova::Time>(
      length, nulls, warning, [&, name](Index row) {
        auto result
          = time_from_nanos(to_nanos(values, detail::narrow<size_t>(row)));
        if (not result) {
          warning.emit("{} value is out of range after rescaling to "
                       "nanoseconds",
                       name);
        }
        return result;
      });
  };
  auto durations = [&](auto const& values, auto to_nanos,
                       std::string_view name) {
    return decode_rows<nova::Duration>(
      length, nulls, warning, [&, name](Index row) {
        auto result
          = duration_from_nanos(to_nanos(values, detail::narrow<size_t>(row)));
        if (not result) {
          warning.emit("{} value is out of range after rescaling to "
                       "nanoseconds",
                       name);
        }
        return result;
      });
  };
  auto result = match(
    *effective,
    [&]<class Column>(Column const& values) -> Option<nova::Array<nova::Data>> {
      using namespace ::clickhouse;
      if constexpr (std::same_as<Column, ColumnInt128>) {
        return decode_rows<nova::String>(
          length, nulls, warning, [&](Index row) {
            return Option{format_int128(values.At(row))};
          });
      } else if constexpr (std::same_as<Column, ColumnUInt128>) {
        return decode_rows<nova::String>(
          length, nulls, warning, [&](Index row) {
            return Option{format_uint128(values.At(row))};
          });
      } else if constexpr (concepts::one_of<Column, ColumnInt8, ColumnInt16,
                                            ColumnInt32, ColumnInt64,
                                            ColumnUInt8, ColumnUInt16,
                                            ColumnUInt32, ColumnUInt64,
                                            ColumnFloat32, ColumnFloat64>) {
        return copy_numbers(values, length);
      } else if constexpr (std::same_as<Column, ColumnBool>) {
        return decode_rows<nova::Bool>(length, nulls, warning, [&](Index row) {
          return Option{values.At(row)};
        });
      } else if constexpr (concepts::one_of<Column, ColumnString,
                                            ColumnFixedString, ColumnJSON>) {
        return strings(values);
      } else if constexpr (concepts::one_of<Column, ColumnEnum8, ColumnEnum16>) {
        return decode_rows<nova::String>(length, nulls, warning,
                                         [&](Index row) {
                                           return Option{values.NameAt(row)};
                                         });
      } else if constexpr (std::same_as<Column, ColumnUUID>) {
        return decode_rows<nova::String>(
          length, nulls, warning, [&](Index row) {
            return Option{format_uuid(values.At(row))};
          });
      } else if constexpr (std::same_as<Column, ColumnDecimal>) {
        return decode_rows<nova::String>(
          length, nulls, warning, [&](Index row) {
            return Option{format_scaled_integer(format_int128(values.At(row)),
                                                values.GetScale())};
          });
      } else if constexpr (std::same_as<Column, ColumnDate>) {
        return times(
          values,
          [](ColumnDate const& xs, size_t row) {
            return days_to_nanos(xs.RawAt(row));
          },
          "Date");
      } else if constexpr (std::same_as<Column, ColumnDate32>) {
        return times(
          values,
          [](ColumnDate32 const& xs, size_t row) {
            return days_to_nanos(xs.RawAt(row));
          },
          "Date32");
      } else if constexpr (std::same_as<Column, ColumnDateTime>) {
        return times(
          values,
          [](ColumnDateTime const& xs, size_t row) {
            return seconds_to_nanos(xs.RawAt(row));
          },
          "DateTime");
      } else if constexpr (std::same_as<Column, ColumnDateTime64>) {
        return times(
          values,
          [](ColumnDateTime64 const& xs, size_t row) {
            return rescale_decimal_to_nanos(xs.At(row), xs.GetPrecision());
          },
          "DateTime64");
      } else if constexpr (std::same_as<Column, ColumnTime>) {
        return durations(
          values,
          [](ColumnTime const& xs, size_t row) {
            return seconds_to_nanos(xs.At(row));
          },
          "Time");
      } else if constexpr (std::same_as<Column, ColumnTime64>) {
        return durations(
          values,
          [](ColumnTime64 const& xs, size_t row) {
            return rescale_decimal_to_nanos(xs.At(row), xs.GetPrecision());
          },
          "Time64");
      } else if constexpr (std::same_as<Column, ColumnIPv4>) {
        return decode_rows<nova::Ip>(length, nulls, warning, [&](Index row) {
          return Option{ip_from_v4(values.At(row))};
        });
      } else if constexpr (std::same_as<Column, ColumnIPv6>) {
        return decode_rows<nova::Ip>(length, nulls, warning, [&](Index row) {
          return Option{ip_from_v6(values.At(row))};
        });
      } else if constexpr (std::same_as<Column, ColumnArray>) {
        if (type.kind == DecodedKind::blob) {
          return decode_blobs(values, length, nulls, warning);
        }
        return decode_list(values, type, length, path, dh);
      } else if constexpr (std::same_as<Column, ColumnTuple>) {
        return decode_tuple(values, type, length, path, dh);
      } else if constexpr (std::same_as<Column, ColumnLowCardinality>) {
        return decode_low_cardinality(values, length, nulls, warning);
      } else if constexpr (std::same_as<Column, ColumnNothing>) {
        return make_nulls(length);
      } else if constexpr (std::same_as<Column, ColumnNullable>) {
        TENZIR_UNREACHABLE();
      } else {
        // `classify` rejects every other type.
        warning.emit("unsupported ClickHouse runtime type `{}`",
                     values.GetType().GetName());
        nulls.set_all();
        return make_nulls(length);
      }
    });
  if (not result) {
    return None{};
  }
  return std::move(nulls).apply(std::move(*result));
}

} // namespace

auto block_to_events(::clickhouse::Block const& block,
                     std::string_view schema_name, diagnostic_handler& dh)
  -> Option<nova::Events> {
  if (block.GetColumnCount() == 0 or block.GetRowCount() == 0) {
    return None{};
  }
  auto const length = detail::narrow<Index>(block.GetRowCount());
  auto names = std::vector<std::string>{};
  auto arrays = std::vector<nova::Array<nova::Data>>{};
  names.reserve(block.GetColumnCount());
  arrays.reserve(block.GetColumnCount());
  auto const root = value_path{};
  for (auto i = size_t{0}; i < block.GetColumnCount(); ++i) {
    auto name = std::string{block.GetColumnName(i)};
    auto path = root.field(name);
    auto type = classify(block[i]->Type());
    if (not type) {
      emit_unsupported_column_warning(path, block[i]->Type()->GetName(), dh);
      continue;
    }
    auto array = column_to_array(block[i], *type, path, dh);
    if (not array) {
      continue;
    }
    names.push_back(std::move(name));
    arrays.push_back(std::move(*array));
  }
  if (arrays.empty()) {
    emit_empty_block_warning(schema_name, dh);
    return None{};
  }
  auto fields = std::vector<
    std::pair<std::string_view, nova::Array<nova::Record>::MaskedArray>>{};
  fields.reserve(arrays.size());
  for (auto i = size_t{0}; i < arrays.size(); ++i) {
    fields.emplace_back(names[i], nova::Array<nova::Record>::MaskedArray{
                                    std::move(arrays[i]),
                                    nova::storage::BitMap{length, true},
                                  });
  }
  return nova::Events{
    nova::Array<nova::Record>::from_fields(fields),
    nova::storage::BitMap{length, true},
    nova::Events::Meta::make_empty(length, schema_name),
  };
}

} // namespace tenzir::plugins::clickhouse
