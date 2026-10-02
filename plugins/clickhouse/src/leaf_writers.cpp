//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/arguments.hpp"
#include "clickhouse/column_writer_detail.hpp"
#include "tenzir/nova_json_printer.hpp"

#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/json.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <netinet/in.h>

#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <tuple>
#include <utility>

namespace tenzir::plugins::clickhouse {

namespace {

using nova::storage::BitMap;
using nova::storage::Index;

template <class T>
auto make_vector_column(std::vector<T>&& values,
                        Option<std::vector<uint8_t>> nulls)
  -> ::clickhouse::ColumnRef {
  return wrap_nullable(
    std::make_shared<::clickhouse::ColumnVector<T>>(std::move(values)),
    std::move(nulls));
}

/// Values that convert without loss. Their conversion never fails.
struct Unchecked {
  static constexpr auto checked = false;

  static auto invalid(std::string_view path) -> diagnostic_builder {
    TENZIR_UNUSED(path);
    TENZIR_UNREACHABLE();
  }
};

/// Values that may not fit into the column.
struct RangeChecked {
  static constexpr auto checked = true;

  static auto invalid(std::string_view path) -> diagnostic_builder {
    return diagnostic::warning("value out of range for ClickHouse column `{}`",
                               path);
  }
};

struct BoolSpec : Unchecked {
  using Value = uint8_t;
  static constexpr auto expected = std::string_view{"bool"};

  auto convert(bool x) const -> Option<Value> {
    return Value{x};
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return wrap_nullable(
      std::make_shared<::clickhouse::ColumnBool>(std::move(values)),
      std::move(nulls));
  }
};

/// Booleans in `UInt8` columns of tables created by older versions.
struct LegacyBoolSpec : Unchecked {
  using Value = uint8_t;
  static constexpr auto expected = std::string_view{"bool"};

  auto convert(bool x) const -> Option<Value> {
    return Value{x};
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return make_vector_column(std::move(values), std::move(nulls));
  }
};

struct Int64Spec : Unchecked {
  using Value = int64_t;
  static constexpr auto expected = std::string_view{"int"};

  auto convert(int64_t x) const -> Option<Value> {
    return x;
  }

  auto convert(duration x) const -> Option<Value> {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(x).count();
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return make_vector_column(std::move(values), std::move(nulls));
  }
};

struct UInt64Spec : Unchecked {
  using Value = uint64_t;
  static constexpr auto expected = std::string_view{"uint"};

  auto convert(uint64_t x) const -> Option<Value> {
    return x;
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return make_vector_column(std::move(values), std::move(nulls));
  }
};

struct Float64Spec : Unchecked {
  using Value = double;
  static constexpr auto expected = std::string_view{"float"};

  auto convert(double x) const -> Option<Value> {
    return x;
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return make_vector_column(std::move(values), std::move(nulls));
  }
};

/// Strings are sent as views into the batch, which outlives the insert.
struct StringSpec : Unchecked {
  using Value = std::string_view;
  static constexpr auto expected = std::string_view{"string"};

  auto convert(std::string_view x) const -> Option<Value> {
    return x;
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    auto column = std::make_shared<::clickhouse::ColumnString>();
    column->Reserve(values.size());
    for (auto value : values) {
      column->AppendNoManagedLifetime(value);
    }
    return wrap_nullable(std::move(column), std::move(nulls));
  }
};

auto to_in6_addr(ip const& x) -> in6_addr {
  return std::bit_cast<in6_addr>(x);
}

struct Ipv6Spec : Unchecked {
  using Value = in6_addr;
  static constexpr auto expected = std::string_view{"ip"};

  auto convert(ip x) const -> Option<Value> {
    return to_in6_addr(x);
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    auto column = std::make_shared<::clickhouse::ColumnIPv6>();
    column->Reserve(values.size());
    for (auto const& value : values) {
      column->Append(value);
    }
    return wrap_nullable(std::move(column), std::move(nulls));
  }
};

struct Ipv4Spec {
  static constexpr auto checked = true;
  using Value = uint32_t;
  static constexpr auto expected = std::string_view{"ip"};

  static auto invalid(std::string_view path) -> diagnostic_builder {
    return diagnostic::warning(
      "IPv6 value cannot be stored in IPv4 column `{}`", path);
  }

  /// Returns the address in network byte order.
  auto convert(ip x) const -> Option<Value> {
    if (not x.is_v4()) {
      return None{};
    }
    auto result = Value{};
    std::memcpy(&result, as_bytes(x).data() + 12, sizeof(result));
    return result;
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return wrap_nullable(
      std::make_shared<::clickhouse::ColumnIPv4>(std::move(values)),
      std::move(nulls));
  }
};

/// Writes `DateTime64(scale[, timezone])`. Nanoseconds are floored to the
/// column's tick, just like a conversion on the server.
struct DateTime64Spec : Unchecked {
  using Value = int64_t;
  static constexpr auto expected = std::string_view{"time"};

  size_t scale;
  Option<std::string> timezone;
  int64_t divisor;

  auto convert(time x) const -> Option<Value> {
    auto const nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         x.time_since_epoch())
                         .count();
    auto ticks = nanos / divisor;
    if (nanos % divisor != 0 and nanos < 0) {
      --ticks;
    }
    return ticks;
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    auto column
      = timezone
          ? std::make_shared<::clickhouse::ColumnDateTime64>(scale, *timezone)
          : std::make_shared<::clickhouse::ColumnDateTime64>(scale);
    column->Reserve(values.size());
    for (auto value : values) {
      column->Append(value);
    }
    return wrap_nullable(std::move(column), std::move(nulls));
  }
};

template <class T>
struct CheckedIntSpec : RangeChecked {
  using Value = T;
  static constexpr auto expected = std::string_view{"int"};

  auto convert(int64_t x) const -> Option<Value> {
    if (not std::in_range<T>(x)) {
      return None{};
    }
    return static_cast<T>(x);
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return make_vector_column(std::move(values), std::move(nulls));
  }
};

template <class T>
struct CheckedUIntSpec : RangeChecked {
  using Value = T;
  static constexpr auto expected = std::string_view{"uint"};

  auto convert(uint64_t x) const -> Option<Value> {
    if (not std::in_range<T>(x)) {
      return None{};
    }
    return static_cast<T>(x);
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return make_vector_column(std::move(values), std::move(nulls));
  }
};

/// Accepts only doubles that a float represents exactly.
struct Float32Spec : RangeChecked {
  using Value = float;
  static constexpr auto expected = std::string_view{"float"};

  auto convert(double x) const -> Option<Value> {
    if (not std::isfinite(x) or x < std::numeric_limits<float>::lowest()
        or x > std::numeric_limits<float>::max()
        or static_cast<double>(static_cast<float>(x)) != x) {
      return None{};
    }
    return static_cast<float>(x);
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    return make_vector_column(std::move(values), std::move(nulls));
  }
};

/// Subnets are a `Tuple(ip IPv6, length UInt8)`, where the elements are
/// nullable instead of the tuple.
struct SubnetSpec : Unchecked {
  using Value = std::pair<in6_addr, uint8_t>;
  static constexpr auto expected = std::string_view{"subnet"};

  auto convert(subnet x) const -> Option<Value> {
    return Value{to_in6_addr(x.network()), x.length()};
  }

  auto
  make(std::vector<Value>&& values, Option<std::vector<uint8_t>> nulls) const
    -> ::clickhouse::ColumnRef {
    auto ips = std::make_shared<::clickhouse::ColumnIPv6>();
    ips->Reserve(values.size());
    auto lengths = std::vector<uint8_t>{};
    lengths.reserve(values.size());
    for (auto const& [network, length] : values) {
      ips->Append(network);
      lengths.push_back(length);
    }
    auto length_nulls = nulls;
    return std::make_shared<::clickhouse::ColumnTuple>(
      std::vector<::clickhouse::ColumnRef>{
        wrap_nullable(std::move(ips), std::move(nulls)),
        make_vector_column(std::move(lengths), std::move(length_nulls)),
      });
  }
};

/// A leaf writer that accepts values of the types `Tags`.
template <class Spec, class... Tags>
class ScalarWriter final : public ColumnWriter {
public:
  using Value = typename Spec::Value;

  ScalarWriter(std::string path, std::string type, bool nullable, Spec spec)
    : ColumnWriter{std::move(path), std::move(type), nullable},
      spec_{std::move(spec)} {
  }

  auto create_mask(nova::Array<nova::Data> const& data, BitMap rows,
                   WriteCtx& ctx) const -> BitMap override {
    auto const length = data.length();
    auto const nulls = null_rows(data);
    auto accepted = BitMap{length, false};
    auto const alternatives = std::tuple{data.get_alternative<Tags>()...};
    std::apply(
      [&](auto const&... alternative) {
        ((alternative ? (accepted = std::move(accepted) | alternative->present)
                      : accepted),
         ...);
      },
      alternatives);
    if (not nullable()) {
      if ((rows & nulls).any()) {
        report_null(*this, ctx);
      }
    } else {
      accepted = std::move(accepted) | nulls;
    }
    if (auto mismatch = rows.and_not(accepted | nulls); mismatch.any()) {
      report_mismatch(*this, Spec::expected, data, mismatch, ctx);
    }
    rows = std::move(rows) & accepted;
    if constexpr (Spec::checked) {
      auto invalid = BitMap::Mutable{length};
      auto any_invalid = false;
      std::apply(
        [&](auto const&... alternative) {
          (
            [&] {
              if (not alternative) {
                return;
              }
              for_each_value(alternative->data, rows & alternative->present,
                             [&](Index row, auto value) {
                               if (not spec_.convert(value)) {
                                 std::ignore = invalid.set(row, true);
                                 any_invalid = true;
                               }
                             });
            }(),
            ...);
        },
        alternatives);
      if (any_invalid) {
        if (ctx.first_report(*this, WriteIssue::invalid)) {
          Spec::invalid(path()).emit(ctx.dh());
        }
        rows = std::move(rows).and_not(std::move(invalid).finish());
      }
    }
    return rows;
  }

  auto create_column(std::span<ColumnPart const> parts, WriteCtx& ctx) const
    -> ::clickhouse::ColumnRef override {
    TENZIR_UNUSED(ctx);
    auto const total = total_rows(parts);
    auto values = std::vector<Value>{};
    values.reserve(total);
    auto nulls = std::vector<uint8_t>{};
    if (nullable()) {
      nulls.reserve(total);
    }
    auto push = [&](auto value) {
      auto converted = spec_.convert(value);
      TENZIR_ASSERT(converted);
      values.push_back(std::move(*converted));
    };
    for (auto const& part : parts) {
      auto const& rows = part.present;
      auto const count = rows.true_count();
      if (count == 0) {
        continue;
      }
      auto const alternatives
        = std::tuple{part.data.get_alternative<Tags>()...};
      // Most columns hold a single type besides nulls, so their values are
      // copied in one loop.
      auto const null_count
        = nullable() ? (rows & null_rows(part.data)).true_count() : Index{0};
      auto done = false;
      std::apply(
        [&](auto const&... alternative) {
          (
            [&] {
              if (done or not alternative
                  or (rows & alternative->present).true_count() + null_count
                       != count) {
                return;
              }
              done = true;
              if (null_count == 0) {
                for_each_value(alternative->data, rows, [&](Index, auto value) {
                  push(value);
                });
                if (nullable()) {
                  nulls.insert(nulls.end(), static_cast<size_t>(count), 0);
                }
                return;
              }
              auto const& present = alternative->present;
              match(alternative->data.storage(), [&](auto const& storage) {
                for (auto row : nova::storage::true_bits(rows)) {
                  if (present.get(row)) {
                    push(storage.get(row));
                    nulls.push_back(0);
                  } else {
                    values.emplace_back();
                    nulls.push_back(1);
                  }
                }
              });
            }(),
            ...);
        },
        alternatives);
      if (done) {
        continue;
      }
      for (auto row : nova::storage::true_bits(rows)) {
        auto found = false;
        std::apply(
          [&](auto const&... alternative) {
            (
              [&] {
                if (found or not alternative
                    or not alternative->present.get(row)) {
                  return;
                }
                push(*alternative->data.get(row));
                found = true;
              }(),
              ...);
          },
          alternatives);
        if (not found) {
          TENZIR_ASSERT(nullable());
          values.emplace_back();
        }
        if (nullable()) {
          nulls.push_back(found ? 0 : 1);
        }
      }
    }
    return spec_.make(std::move(values),
                      nullable()
                        ? Option<std::vector<uint8_t>>{std::move(nulls)}
                        : Option<std::vector<uint8_t>>{None{}});
  }

private:
  Spec spec_;
};

template <class Spec, class... Tags>
auto make_scalar(std::string path, std::string_view type, bool nullable,
                 Spec spec = {}) -> Box<ColumnWriter> {
  auto name = nullable ? fmt::format("Nullable({})", type) : std::string{type};
  return make_boxed<ColumnWriter, ScalarWriter<Spec, Tags...>>(
    std::move(path), std::move(name), nullable, std::move(spec));
}

auto make_datetime64(std::string path, std::string_view type, bool nullable)
  -> Option<Box<ColumnWriter>> {
  auto arguments = unwrap_clickhouse_type_call(type, "DateTime64");
  if (not arguments) {
    return None{};
  }
  auto parts = split_top_level_clickhouse_type_arguments(*arguments);
  auto scale = size_t{0};
  if (parts.empty() or not parse_clickhouse_size(parts[0], scale)
      or scale > 9) {
    return None{};
  }
  auto timezone = Option<std::string>{};
  if (parts.size() > 1) {
    auto tz = parts[1];
    if (tz.size() >= 2 and tz.front() == '\'' and tz.back() == '\'') {
      tz = tz.substr(1, tz.size() - 2);
    }
    timezone = std::string{tz};
  }
  auto divisor = int64_t{1};
  for (auto i = scale; i < 9; ++i) {
    divisor *= 10;
  }
  return make_scalar<DateTime64Spec, nova::Time>(
    std::move(path), type, nullable,
    DateTime64Spec{{}, scale, std::move(timezone), divisor});
}

/// Sends values to a `JSON` column, which only accepts objects. Strings are
/// assumed to hold serialized JSON already. Nulls become `NULL` in a nullable
/// column and `{}` otherwise.
class JsonWriter final : public ColumnWriter {
public:
  JsonWriter(std::string path, std::string type, bool sql_nullable)
    : ColumnWriter{std::move(path), std::move(type), true},
      sql_nullable_{sql_nullable} {
  }

  auto create_mask(nova::Array<nova::Data> const& data, BitMap rows,
                   WriteCtx& ctx) const -> BitMap override {
    TENZIR_UNUSED(data, ctx);
    return rows;
  }

  auto create_column(std::span<ColumnPart const> parts, WriteCtx& ctx) const
    -> ::clickhouse::ColumnRef override {
    auto const total = total_rows(parts);
    auto column = std::make_shared<::clickhouse::ColumnJSON>();
    column->Reserve(total);
    auto nulls = std::vector<uint8_t>{};
    if (sql_nullable_) {
      nulls.reserve(total);
    }
    auto printer = nova::json_printer{json_printer_options{
      .style = no_style(),
      .oneline = true,
      .omit_null_fields = true,
      .omit_nulls_in_lists = true,
    }};
    auto push = [&](std::string_view text, bool null) {
      column->Append(text);
      if (sql_nullable_) {
        nulls.push_back(null ? 1 : 0);
      }
    };
    auto push_non_object = [&] {
      if (ctx.first_report(*this, WriteIssue::non_object)) {
        diagnostic::warning("cannot write `{}` into a ClickHouse JSON column",
                            path())
          .note("expected a JSON object, but the value is not one")
          .note("affected values are written as empty objects (`{}`)")
          .emit(ctx.dh());
      }
      push("{}", false);
    };
    for (auto const& part : parts) {
      auto const nulls_of_part = null_rows(part.data);
      auto const records = part.data.get_alternative<nova::Record>();
      auto const strings = part.data.get_alternative<nova::String>();
      for (auto row : nova::storage::true_bits(part.present)) {
        if (nulls_of_part.get(row)) {
          push("{}", true);
        } else if (records and records->present.get(row)) {
          printer.print(part.data.get(row));
          auto const bytes = printer.bytes();
          push(std::string_view{reinterpret_cast<char const*>(bytes.data()),
                                bytes.size()},
               false);
        } else if (strings and strings->present.get(row)) {
          // Strings are assumed to hold serialized JSON already.
          auto const text = *strings->data.get(row);
          auto const begin = text.find_first_not_of(" \t\n\r");
          if (begin == std::string_view::npos or text[begin] != '{') {
            push_non_object();
          } else {
            push(text, false);
          }
        } else {
          push_non_object();
        }
      }
    }
    return wrap_nullable(std::move(column),
                         sql_nullable_
                           ? Option<std::vector<uint8_t>>{std::move(nulls)}
                           : Option<std::vector<uint8_t>>{None{}});
  }

private:
  bool sql_nullable_;
};

} // namespace

auto make_scalar_writer(std::string path, std::string_view type, bool nullable,
                        ColumnMapping mapping) -> Option<Box<ColumnWriter>> {
  using namespace nova;
  if (type == "Bool") {
    return make_scalar<BoolSpec, Bool>(std::move(path), type, nullable);
  }
  if (type == "Int64") {
    return make_scalar<Int64Spec, Int, Duration>(std::move(path), type,
                                                 nullable);
  }
  if (type == "UInt64") {
    return make_scalar<UInt64Spec, UInt>(std::move(path), type, nullable);
  }
  if (type == "Float64") {
    return make_scalar<Float64Spec, Float>(std::move(path), type, nullable);
  }
  if (type == "String") {
    return make_scalar<StringSpec, String>(std::move(path), type, nullable);
  }
  if (type == "IPv6") {
    return make_scalar<Ipv6Spec, Ip>(std::move(path), type, nullable);
  }
  if (type == "UInt8") {
    // The integer mapping must not shadow booleans of older tables.
    if (mapping == ColumnMapping::lossless) {
      return make_scalar<CheckedUIntSpec<uint8_t>, UInt>(std::move(path), type,
                                                         nullable);
    }
    return make_scalar<LegacyBoolSpec, Bool>(std::move(path), type, nullable);
  }
  if (type == "IPv4" and mapping == ColumnMapping::lossless) {
    return make_scalar<Ipv4Spec, Ip>(std::move(path), type, nullable);
  }
  if (type == "UInt16") {
    return make_scalar<CheckedUIntSpec<uint16_t>, UInt>(std::move(path), type,
                                                        nullable);
  }
  if (type == "UInt32") {
    return make_scalar<CheckedUIntSpec<uint32_t>, UInt>(std::move(path), type,
                                                        nullable);
  }
  if (type == "Int8") {
    return make_scalar<CheckedIntSpec<int8_t>, Int>(std::move(path), type,
                                                    nullable);
  }
  if (type == "Int16") {
    return make_scalar<CheckedIntSpec<int16_t>, Int>(std::move(path), type,
                                                     nullable);
  }
  if (type == "Int32") {
    return make_scalar<CheckedIntSpec<int32_t>, Int>(std::move(path), type,
                                                     nullable);
  }
  if (type == "Float32") {
    return make_scalar<Float32Spec, Float>(std::move(path), type, nullable);
  }
  return make_datetime64(std::move(path), type, nullable);
}

auto make_subnet_writer(std::string path, std::string type, bool nullable)
  -> Box<ColumnWriter> {
  return make_boxed<ColumnWriter, ScalarWriter<SubnetSpec, nova::Subnet>>(
    std::move(path), std::move(type), nullable, SubnetSpec{});
}

auto make_json_writer(std::string path, std::string type, bool sql_nullable)
  -> Box<ColumnWriter> {
  return make_boxed<ColumnWriter, JsonWriter>(std::move(path), std::move(type),
                                              sql_nullable);
}

} // namespace tenzir::plugins::clickhouse
