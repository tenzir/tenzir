//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_to_events.hpp"

#include "clickhouse/block_to_table_slice.hpp"

#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/nova/list_array.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/option.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/test/test.hpp>

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nothing.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/time.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>

#include <limits>
#include <memory>
#include <vector>

using namespace tenzir;
using namespace tenzir::plugins::clickhouse;
namespace ch = ::clickhouse;

namespace {

struct Decoded {
  Option<nova::Events> events;
  std::vector<diagnostic> diagnostics;
};

auto decode(ch::Block const& block) -> Decoded {
  auto dh = collecting_diagnostic_handler{};
  auto events = block_to_events(block, "test", dh);
  return {std::move(events), std::move(dh).collect()};
}

auto rows(nova::Events const& events) -> std::vector<data> {
  auto all = nova::Array<nova::Data>{events.data};
  auto result = std::vector<data>{};
  for (auto i = nova::storage::Index{0}; i < all.length(); ++i) {
    result.push_back(nova::materialize_legacy(all.get(i)));
  }
  return result;
}

auto legacy_rows(ch::Block const& block) -> std::vector<data> {
  auto dh = null_diagnostic_handler{};
  auto slice = block_to_table_slice(block, "test", dh);
  REQUIRE(slice);
  auto result = std::vector<data>{};
  for (auto&& row : slice->values()) {
    result.emplace_back(materialize(row));
  }
  return result;
}

auto field(data const& row, std::string_view name) -> data const& {
  auto const* rec = try_as<record>(row);
  REQUIRE(rec);
  auto it = rec->find(name);
  REQUIRE(it != rec->end());
  return it->second;
}

auto make_block(std::vector<std::pair<std::string, ch::ColumnRef>> columns)
  -> ch::Block {
  auto block = ch::Block{};
  for (auto& [name, column] : columns) {
    block.AppendColumn(name, column);
  }
  return block;
}

auto ip_of(std::string_view text) -> ip {
  auto result = to<ip>(text);
  REQUIRE(result);
  return *result;
}

/// Builds a `Nullable` column from `values`, where `None` is null.
template <class Column, class T>
auto make_nullable(std::vector<Option<T>> const& values)
  -> std::shared_ptr<ch::ColumnNullable> {
  auto nested = std::make_shared<Column>();
  auto nulls = std::make_shared<ch::ColumnUInt8>();
  for (auto const& value : values) {
    nested->Append(value.value_or(T{}));
    nulls->Append(value ? 0 : 1);
  }
  return std::make_shared<ch::ColumnNullable>(nested, nulls);
}

/// A block with the types that decode the same with either data model.
auto mixed_block() -> ch::Block {
  auto i8 = std::make_shared<ch::ColumnInt8>();
  auto u16 = std::make_shared<ch::ColumnUInt16>();
  auto i64 = std::make_shared<ch::ColumnInt64>();
  auto u64 = std::make_shared<ch::ColumnUInt64>();
  auto f32 = std::make_shared<ch::ColumnFloat32>();
  auto boolean = std::make_shared<ch::ColumnBool>();
  auto str = std::make_shared<ch::ColumnString>();
  auto fixed = std::make_shared<ch::ColumnFixedString>(3);
  auto i128 = std::make_shared<ch::ColumnInt128>();
  auto u128 = std::make_shared<ch::ColumnUInt128>();
  auto uuid = std::make_shared<ch::ColumnUUID>();
  auto dec = std::make_shared<ch::ColumnDecimal>(30, 3);
  auto date = std::make_shared<ch::ColumnDate>();
  auto date32 = std::make_shared<ch::ColumnDate32>();
  auto datetime = std::make_shared<ch::ColumnDateTime>("Europe/Berlin");
  auto dt64 = std::make_shared<ch::ColumnDateTime64>(3);
  auto tm = std::make_shared<ch::ColumnTime>();
  auto tm64 = std::make_shared<ch::ColumnTime64>(6);
  auto v4 = std::make_shared<ch::ColumnIPv4>();
  auto v6 = std::make_shared<ch::ColumnIPv6>();
  auto nullable = make_nullable<ch::ColumnInt64>(
    std::vector<Option<int64_t>>{None{}, int64_t{3}});
  auto bytes = std::make_shared<ch::ColumnArrayT<ch::ColumnUInt8>>();
  auto list = std::make_shared<ch::ColumnArrayT<ch::ColumnString>>();
  auto nested
    = std::make_shared<ch::ColumnArrayT<ch::ColumnArrayT<ch::ColumnInt32>>>();
  auto lc_string
    = std::make_shared<ch::ColumnLowCardinalityT<ch::ColumnString>>();
  auto lc_nullable = std::make_shared<ch::ColumnLowCardinality>(
    make_nullable<ch::ColumnString>(
      std::vector<Option<std::string_view>>{None{}, "y"}));
  auto nothing = std::make_shared<ch::ColumnNothing>(2);
  for (auto row = 0; row < 2; ++row) {
    i8->Append(row == 0 ? int8_t{-128} : int8_t{127});
    u16->Append(row == 0 ? uint16_t{0} : uint16_t{65535});
    i64->Append(row == 0 ? std::numeric_limits<int64_t>::min() : int64_t{42});
    u64->Append(row == 0 ? std::numeric_limits<uint64_t>::max() : uint64_t{7});
    f32->Append(row == 0 ? 1.5f : -0.25f);
    boolean->Append(row == 0);
    str->Append(row == 0 ? std::string_view{"foo"} : std::string_view{""});
    fixed->Append(row == 0 ? std::string_view{"ab"} : std::string_view{"xyz"});
    i128->Append(row == 0 ? -(ch::Int128{1} << 100) : ch::Int128{12});
    u128->Append(row == 0 ? ch::UInt128{1} << 127 : ch::UInt128{0});
    uuid->Append(ch::UUID{0x0123456789abcdefULL, 0xfedcba9876543210ULL});
    dec->Append(row == 0 ? ch::Int128{-12345} : ch::Int128{5});
    date->AppendRaw(row == 0 ? uint16_t{0} : uint16_t{20000});
    date32->AppendRaw(row == 0 ? int32_t{-1000} : int32_t{20000});
    datetime->AppendRaw(row == 0 ? uint32_t{0} : uint32_t{1'700'000'000});
    dt64->Append(row == 0 ? int64_t{-1} : int64_t{1'700'000'000'123});
    tm->Append(row == 0 ? int32_t{-5} : int32_t{86'399});
    tm64->Append(row == 0 ? int64_t{1'234'567} : int64_t{0});
    v4->Append(std::string{row == 0 ? "10.0.0.1" : "192.168.1.255"});
    v6->Append(std::string{row == 0 ? "2001:db8::1" : "::ffff:1.2.3.4"});
    bytes->Append(row == 0 ? std::vector<uint8_t>{0, 255, 7}
                           : std::vector<uint8_t>{});
    list->Append(row == 0 ? std::vector<std::string>{"a", "b"}
                          : std::vector<std::string>{});
    nested->Append(row == 0 ? std::vector<std::vector<int32_t>>{{1, 2}, {}, {3}}
                            : std::vector<std::vector<int32_t>>{});
    lc_string->Append(row == 0 ? std::string_view{"x"} : std::string_view{"x"});
  }
  auto tuple_a = std::make_shared<ch::ColumnInt64>();
  auto tuple_b = make_nullable<ch::ColumnString>(
    std::vector<Option<std::string_view>>{"b", None{}});
  tuple_a->Append(1);
  tuple_a->Append(2);
  auto named = std::make_shared<ch::ColumnTuple>(
    std::vector<ch::ColumnRef>{tuple_a, tuple_b},
    std::vector<std::string>{"a", "b"});
  auto unnamed_x = std::make_shared<ch::ColumnUInt8>();
  auto unnamed_y = std::make_shared<ch::ColumnString>();
  unnamed_x->Append(1);
  unnamed_x->Append(2);
  unnamed_y->Append("p");
  unnamed_y->Append("q");
  auto unnamed = std::make_shared<ch::ColumnTuple>(
    std::vector<ch::ColumnRef>{unnamed_x, unnamed_y});
  return make_block({
    {"i8", i8},
    {"u16", u16},
    {"i64", i64},
    {"u64", u64},
    {"f32", f32},
    {"boolean", boolean},
    {"str", str},
    {"fixed", fixed},
    {"i128", i128},
    {"u128", u128},
    {"uuid", uuid},
    {"dec", dec},
    {"date", date},
    {"date32", date32},
    {"datetime", datetime},
    {"dt64", dt64},
    {"tm", tm},
    {"tm64", tm64},
    {"v4", v4},
    {"v6", v6},
    {"nullable", nullable},
    {"bytes", bytes},
    {"list", list},
    {"nested", nested},
    {"lc_string", lc_string},
    {"lc_nullable", lc_nullable},
    {"nothing", nothing},
    {"named", named},
    {"unnamed", unnamed},
  });
}

} // namespace

TEST("decoding matches the table slice decoder") {
  auto block = mixed_block();
  auto decoded = decode(block);
  REQUIRE(decoded.events);
  CHECK(decoded.diagnostics.empty());
  CHECK_EQUAL(*decoded.events->meta.name.get(0), "test");
  CHECK_EQUAL(rows(*decoded.events), legacy_rows(block));
}

TEST("decoded values") {
  auto decoded = decode(mixed_block());
  REQUIRE(decoded.events);
  auto result = rows(*decoded.events);
  REQUIRE_EQUAL(result.size(), size_t{2});
  auto const& first = result[0];
  CHECK_EQUAL(field(first, "i8"), data{int64_t{-128}});
  CHECK_EQUAL(field(first, "u64"), data{std::numeric_limits<uint64_t>::max()});
  CHECK_EQUAL(field(first, "fixed"), (data{std::string{"ab\0", 3}}));
  CHECK_EQUAL(field(first, "i128"), data{"-1267650600228229401496703205376"});
  CHECK_EQUAL(field(first, "u128"),
              data{"170141183460469231731687303715884105728"});
  CHECK_EQUAL(field(first, "uuid"),
              data{"01234567-89ab-cdef-fedc-ba9876543210"});
  CHECK_EQUAL(field(first, "dec"), data{"-12.345"});
  CHECK_EQUAL(field(first, "date32"),
              data{tenzir::time{} - std::chrono::days{1000}});
  CHECK_EQUAL(field(first, "dt64"),
              data{tenzir::time{} - std::chrono::milliseconds{1}});
  CHECK_EQUAL(field(first, "tm"), data{duration{std::chrono::seconds{-5}}});
  CHECK_EQUAL(field(first, "tm64"),
              data{duration{std::chrono::microseconds{1'234'567}}});
  CHECK_EQUAL(field(first, "v4"), data{ip_of("10.0.0.1")});
  CHECK_EQUAL(field(first, "v6"), data{ip_of("2001:db8::1")});
  CHECK_EQUAL(field(first, "nullable"), data{});
  CHECK_EQUAL(field(first, "bytes"),
              (data{blob{std::byte{0}, std::byte{255}, std::byte{7}}}));
  CHECK_EQUAL(field(first, "nested"), (data{list{list{int64_t{1}, int64_t{2}},
                                                 list{}, list{int64_t{3}}}}));
  CHECK_EQUAL(field(first, "lc_string"), data{"x"});
  CHECK_EQUAL(field(first, "lc_nullable"), data{});
  CHECK_EQUAL(field(first, "nothing"), data{});
  CHECK_EQUAL(field(first, "named"),
              (data{record{{"a", int64_t{1}}, {"b", "b"}}}));
  CHECK_EQUAL(field(first, "unnamed"),
              (data{record{{"field0", uint64_t{1}}, {"field1", "p"}}}));
  auto const& second = result[1];
  CHECK_EQUAL(field(second, "nullable"), data{int64_t{3}});
  CHECK_EQUAL(field(second, "bytes"), data{blob{}});
  CHECK_EQUAL(field(second, "list"), data{list{}});
  CHECK_EQUAL(field(second, "lc_nullable"), data{"y"});
  CHECK_EQUAL(field(second, "named"),
              (data{record{{"a", int64_t{2}}, {"b", data{}}}}));
}

TEST("narrow integers keep their width") {
  auto decoded = decode(mixed_block());
  REQUIRE(decoded.events);
  auto i8 = decoded.events->data.field("i8");
  REQUIRE(i8);
  auto ints = i8->data.try_as<nova::Int>();
  REQUIRE(ints);
  CHECK(is<nova::storage::SparseStorage<int8_t>>(ints->storage()));
}

TEST("values out of range become null with one warning per column") {
  auto date32 = std::make_shared<ch::ColumnDate32>();
  date32->AppendRaw(200'000);
  date32->AppendRaw(1);
  date32->AppendRaw(300'000);
  auto dt64 = std::make_shared<ch::ColumnDateTime64>(0);
  dt64->Append(int64_t{1});
  dt64->Append(std::numeric_limits<int64_t>::max());
  dt64->Append(int64_t{2});
  auto decoded = decode(make_block({{"date32", date32}, {"dt64", dt64}}));
  REQUIRE(decoded.events);
  REQUIRE_EQUAL(decoded.diagnostics.size(), size_t{2});
  CHECK_EQUAL(decoded.diagnostics[0].severity, severity::warning);
  CHECK_EQUAL(decoded.diagnostics[0].message,
              "malformed ClickHouse values in `date32`: Date32 value is out "
              "of range after rescaling to nanoseconds");
  CHECK_EQUAL(decoded.diagnostics[1].message,
              "malformed ClickHouse values in `dt64`: DateTime64 value is out "
              "of range after rescaling to nanoseconds");
  auto result = rows(*decoded.events);
  REQUIRE_EQUAL(result.size(), size_t{3});
  CHECK_EQUAL(field(result[0], "date32"), data{});
  CHECK_EQUAL(field(result[1], "date32"),
              data{tenzir::time{} + std::chrono::days{1}});
  CHECK_EQUAL(field(result[2], "date32"), data{});
  CHECK_EQUAL(field(result[0], "dt64"),
              data{tenzir::time{} + std::chrono::seconds{1}});
  CHECK_EQUAL(field(result[1], "dt64"), data{});
}

TEST("unsupported columns are dropped") {
  auto ok = std::make_shared<ch::ColumnUInt64>();
  ok->Append(1);
  auto map
    = std::make_shared<ch::ColumnMapT<ch::ColumnString, ch::ColumnUInt64>>(
      std::make_shared<ch::ColumnString>(),
      std::make_shared<ch::ColumnUInt64>());
  map->Append(std::map<std::string, uint64_t>{{"k", 1}});
  auto decoded = decode(make_block({{"ok", ok}, {"dropped", map}}));
  REQUIRE(decoded.events);
  REQUIRE_EQUAL(decoded.diagnostics.size(), size_t{1});
  CHECK_EQUAL(decoded.diagnostics[0].message,
              "dropping ClickHouse column `dropped` with unsupported type "
              "`Map(String, UInt64)`");
  CHECK_EQUAL(rows(*decoded.events),
              (std::vector<data>{record{{"ok", uint64_t{1}}}}));
  auto only_map = decode(make_block({{"dropped", map}}));
  CHECK(not only_map.events);
  REQUIRE_EQUAL(only_map.diagnostics.size(), size_t{2});
  CHECK_EQUAL(only_map.diagnostics[1].message,
              "dropping ClickHouse block for schema `test` because no "
              "supported columns remained");
}

TEST("empty blocks decode to nothing") {
  auto empty = std::make_shared<ch::ColumnUInt64>();
  auto decoded = decode(make_block({{"x", empty}}));
  CHECK(not decoded.events);
  CHECK(decoded.diagnostics.empty());
}

TEST("empty lists keep their element type") {
  auto tuple = std::make_shared<ch::ColumnTuple>(
    std::vector<ch::ColumnRef>{std::make_shared<ch::ColumnInt64>()},
    std::vector<std::string>{"a"});
  auto records = std::make_shared<ch::ColumnArray>(tuple->CloneEmpty());
  auto nested
    = std::make_shared<ch::ColumnArrayT<ch::ColumnArrayT<ch::ColumnInt32>>>();
  for (auto row = 0; row < 2; ++row) {
    records->AppendAsColumn(tuple->CloneEmpty());
    nested->Append(std::vector<std::vector<int32_t>>{});
  }
  auto decoded = decode(make_block({{"records", records}, {"nested", nested}}));
  REQUIRE(decoded.events);
  CHECK(decoded.diagnostics.empty());
  auto values = [&](std::string_view name) {
    auto column = decoded.events->data.field(name);
    REQUIRE(column);
    auto lists = column->data.try_as<nova::List>();
    REQUIRE(lists);
    return as<nova::storage::ListStorage>(lists->storage()).values();
  };
  CHECK(values("records").try_as<nova::Record>());
  CHECK(values("nested").try_as<nova::List>());
  CHECK_EQUAL(
    rows(*decoded.events),
    (std::vector<data>{record{{"records", list{}}, {"nested", list{}}},
                       record{{"records", list{}}, {"nested", list{}}}}));
}

TEST("null maps spanning several bitmap words") {
  auto rows_with_nulls = std::vector<Option<int64_t>>{};
  for (auto row = int64_t{0}; row < 300; ++row) {
    rows_with_nulls.push_back(row % 3 == 0 ? Option<int64_t>{} : row);
  }
  auto values = make_nullable<ch::ColumnInt64>(rows_with_nulls);
  auto decoded = decode(make_block({{"x", values}}));
  REQUIRE(decoded.events);
  auto result = rows(*decoded.events);
  REQUIRE_EQUAL(result.size(), size_t{300});
  for (auto row = int64_t{0}; row < 300; ++row) {
    CHECK_EQUAL(field(result[row], "x"), row % 3 == 0 ? data{} : data{row});
  }
}
