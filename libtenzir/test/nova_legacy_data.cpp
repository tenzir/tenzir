//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/record_array_builder.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace tenzir::nova;

namespace {

/// Appends `value` as the single row of a `Data` array and reads it back, so
/// that a round trip can be compared against the input.
auto round_trip(const data& value) -> data {
  auto dh = null_diagnostic_handler{};
  auto builder = ArrayBuilder<Data>{};
  append_legacy_data(builder, value, dh);
  const auto array = builder.finish();
  REQUIRE_EQUAL(array.length(), 1);
  return materialize_legacy(array.get(0));
}

/// Like `round_trip`, but routes the value through a record field, which is
/// how operators that build events reach the helper.
auto round_trip_field(const data& value) -> data {
  auto dh = null_diagnostic_handler{};
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  append_legacy_data(row.field("x"), value, dh);
  const auto array = builder.finish();
  REQUIRE_EQUAL(array.length(), 1);
  const auto materialized = materialize_legacy(Array<Data>{array}.get(0));
  const auto* rec = try_as<record>(materialized);
  REQUIRE(rec);
  const auto it = rec->find("x");
  REQUIRE(it != rec->end());
  return it->second;
}

} // namespace

TEST("append_legacy_data round-trips scalars") {
  CHECK_EQUAL(round_trip(data{}), data{});
  CHECK_EQUAL(round_trip(true), data{true});
  CHECK_EQUAL(round_trip(int64_t{-7}), data{int64_t{-7}});
  CHECK_EQUAL(round_trip(uint64_t{7}), data{uint64_t{7}});
  CHECK_EQUAL(round_trip(2.5), data{2.5});
  CHECK_EQUAL(round_trip(std::string{"hello"}), data{std::string{"hello"}});
  CHECK_EQUAL(round_trip(duration{5}), data{duration{5}});
  CHECK_EQUAL(round_trip(tenzir::time{duration{5}}),
              data{tenzir::time{duration{5}}});
}

TEST("append_legacy_data round-trips blobs") {
  const auto bytes = blob{std::byte{0x01}, std::byte{0x02}};
  CHECK_EQUAL(round_trip(bytes), data{bytes});
}

TEST("append_legacy_data round-trips nested records and lists") {
  const auto value = record{
    {"a", int64_t{1}},
    {"b", record{{"c", std::string{"x"}}}},
    {"d", list{int64_t{1}, int64_t{2}}},
  };
  CHECK_EQUAL(round_trip(value), data{value});
}

TEST("append_legacy_data keeps explicit nulls inside a record") {
  const auto value = record{{"a", data{}}, {"b", int64_t{1}}};
  CHECK_EQUAL(round_trip(value), data{value});
}

TEST("append_legacy_data keeps explicit nulls in a list of records") {
  const auto value = list{
    record{{"a", int64_t{1}}, {"s", data{}}},
    record{{"a", int64_t{2}}, {"s", std::string{"x"}}},
  };
  CHECK_EQUAL(round_trip(value), data{value});
}

TEST("append_legacy_data keeps empty records and lists") {
  CHECK_EQUAL(round_trip(record{}), data{record{}});
  CHECK_EQUAL(round_trip(list{}), data{list{}});
}

TEST("append_legacy_data reaches the same result through a record field") {
  const auto value = record{
    {"example", record{{"requests",
                        list{
                          record{{"id", std::string{"a"}}, {"schema", data{}}},
                          record{{"id", std::string{"b"}},
                                 {"schema", std::string{"never"}}},
                        }}}},
  };
  CHECK_EQUAL(round_trip_field(value), data{value});
}
