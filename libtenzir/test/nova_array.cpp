//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_util.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/stringify.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/test/test.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir::nova;

namespace {

/// Builds an array for `Tag` from `values` via `.data(v)` and checks that
/// every row reads back its original value at its true index. `.none()` rows
/// are deliberately not covered here: `.none()` produces an intentionally
/// invalid entry, and reading it back via `.get()` is expected to fail an
/// assertion rather than yield an observable value.
template <fundamental_type Tag>
auto check_scalar_roundtrip(std::vector<typename Type<Tag>::ViewType> values)
  -> void {
  auto builder = ArrayBuilder<Tag>{};
  for (const auto& v : values) {
    builder.data(v);
  }
  auto arr = builder.finish();
  CHECK_EQUAL(arr.length(), static_cast<storage::Index>(values.size()));
  for (auto i = std::size_t{0}; i < values.size(); ++i) {
    CHECK_EQUAL(*arr.get(static_cast<storage::Index>(i)), values[i]);
  }
}

/// Collects the field names of a `RowView<Record>` in shape order.
auto record_field_names(RowView<Record> row) -> std::vector<std::string> {
  auto names = std::vector<std::string>{};
  for (auto [name, value] : row) {
    static_cast<void>(value);
    names.emplace_back(name);
  }
  return names;
}

auto bitmap(std::initializer_list<bool> values) -> storage::BitMap {
  auto result = storage::BitMap::Builder{};
  for (auto value : values) {
    result.emplace_back(value);
  }
  return result.finish();
}

auto make_field_path(std::initializer_list<std::string_view> names)
  -> tenzir::ast::field_path {
  REQUIRE(names.size() > 0);
  auto it = names.begin();
  auto expr = tenzir::ast::expression{tenzir::ast::root_field{
    tenzir::ast::identifier{std::string{*it}, tenzir::location::unknown}}};
  for (++it; it != names.end(); ++it) {
    expr = tenzir::ast::expression{tenzir::ast::field_access{
      std::move(expr),
      tenzir::location::unknown,
      false,
      tenzir::ast::identifier{std::string{*it}, tenzir::location::unknown},
    }};
  }
  auto path = tenzir::ast::field_path::try_from(std::move(expr));
  REQUIRE(path.is_some());
  return std::move(*path);
}

auto as_int(RowView<Data> value) -> std::int64_t {
  return match(
    value,
    [](RowView<Int> x) {
      return *x;
    },
    [](auto) {
      FAIL("expected an int row");
      return std::int64_t{0};
    });
}

auto as_string(RowView<Data> value) -> std::string_view {
  return match(
    value,
    [](RowView<Type<String>::ViewType> x) {
      return *x;
    },
    [](auto) {
      FAIL("expected a string row");
      return std::string_view{};
    });
}

auto is_null(RowView<Data> value) -> bool {
  return match(
    value,
    [](RowView<Null>) {
      return true;
    },
    [](auto) {
      return false;
    });
}

auto is_null(Array<Data> const& array) -> bool {
  return match(
    array,
    [](Array<Null> const&) {
      return true;
    },
    [](auto const&) {
      return false;
    });
}

auto bitmap_data(Array<Bool> const& array) -> storage::BitMap::Word const* {
  return match(array.storage(), [](auto const& bitmap) {
    return bitmap.data().data();
  });
}

auto bitmap_data(ErasedArray const& array) -> storage::BitMap::Word const* {
  constexpr auto index = ErasedDataAlternatives::unique_index_of<Array<Bool>>;
  auto concrete = tenzir::variant_traits<ErasedArray>::get<index>(array);
  return bitmap_data(concrete);
}

} // namespace

TEST("shared owner as_unique copies lvalues") {
  auto source = storage::SharedOwner<std::string[]>::make_value(2, "value");
  auto const* source_data = source.begin();
  auto result = source.as_unique();
  CHECK_NOT_EQUAL(result.begin(), source_data);
  CHECK_EQUAL(result[0], "value");
  result[0] = "changed";
  CHECK_EQUAL(source[0], "value");
}

TEST("scalar shared owner as_unique copies shared rvalues") {
  auto source = storage::SharedOwner<std::string>::make("value");
  auto alias = source;
  auto const* shared_data = source.get();
  auto result = std::move(source).as_unique();
  CHECK_NOT_EQUAL(result.get(), shared_data);
  CHECK_EQUAL(alias.get(), shared_data);
  *result = "changed";
  CHECK_EQUAL(*alias, "value");
}

TEST("shared owner as_unique reuses unique rvalues") {
  auto source = storage::SharedOwner<std::int64_t[]>::make_value(2, 42);
  auto const* source_data = source.begin();
  auto result = std::move(source).as_unique();
  CHECK_EQUAL(result.begin(), source_data);
  CHECK(not source);
}

TEST("shared owner as_unique copies shared rvalues") {
  auto source = storage::SharedOwner<std::int64_t[]>::make_value(2, 42);
  auto alias = source;
  auto const* shared_data = source.begin();
  auto result = std::move(source).as_unique();
  CHECK_NOT_EQUAL(result.begin(), shared_data);
  CHECK_EQUAL(alias.begin(), shared_data);
  result[0] = 7;
  CHECK_EQUAL(alias[0], 42);
}

TEST("multi-buffer storage as_unique uniquifies every direct owner") {
  auto data = storage::SharedOwner<char[]>::make_value(3, 'x');
  auto ranges
    = storage::SharedOwner<storage::Span[]>::make_value(1, storage::Span{0, 3});
  auto source
    = storage::DenseStringOffsetStorage{std::move(data), std::move(ranges)};
  auto alias = source;
  auto const* shared_data = source.data().begin();
  auto result = std::move(source).as_unique();
  CHECK_NOT_EQUAL(result.data().begin(), shared_data);
  CHECK_EQUAL(alias.data().begin(), shared_data);
  CHECK_EQUAL(result.get(0), "xxx");
}

TEST("fundamental and data arrays dispatch as_unique") {
  auto builder = ArrayBuilder<Bool>{};
  builder.data(true);
  builder.data(false);
  auto concrete = builder.finish();
  auto erased = Array<Data>{concrete};
  auto alias = erased;
  auto result = std::move(erased).as_unique();
  auto result_bool = result.try_as<Bool>();
  auto alias_bool = alias.try_as<Bool>();
  REQUIRE(result_bool.is_some());
  REQUIRE(alias_bool.is_some());
  CHECK_NOT_EQUAL(bitmap_data(*result_bool), bitmap_data(*alias_bool));
}

TEST("erased array dispatches as_unique") {
  auto builder = ArrayBuilder<Bool>{};
  builder.data(true);
  builder.data(false);
  auto source = ErasedArray{builder.finish()};
  auto alias = source;
  auto result = std::move(source).as_unique();
  CHECK_NOT_EQUAL(bitmap_data(result), bitmap_data(alias));
}

TEST("union array as_unique preserves nested sharing") {
  auto builder = ArrayBuilder<Bool>{};
  builder.data(true);
  builder.data(false);
  auto alternatives = storage::Vector<UnionArray::MaskedArray>{};
  alternatives.push_back({
    .data = ErasedArray{builder.finish()},
    .present = storage::BitMap{2, true},
  });
  auto indices_owner = storage::SharedOwner<storage::Index[]>::make_value(2, 0);
  auto source = UnionArray{
    storage::SparseStorage<storage::Index>{std::move(indices_owner)},
    std::move(alternatives),
  };
  auto alias = source;
  auto result = std::move(source).as_unique();
  CHECK_NOT_EQUAL(result.fields().data(), alias.fields().data());
  CHECK_EQUAL(bitmap_data(result.fields()[0].data),
              bitmap_data(alias.fields()[0].data));
}

TEST("list array as_unique only uniquifies its array storage") {
  auto value_builder = ArrayBuilder<Int>{};
  value_builder.data(std::int64_t{42});
  auto spans
    = storage::SharedOwner<storage::Span[]>::make_value(1, storage::Span{0, 1});
  auto source
    = Array<List>{std::move(spans), Array<Data>{value_builder.finish()}};
  auto alias = source;
  auto const* nested_spans
    = as<storage::ListStorage>(source.storage()).spans().begin();
  auto result = std::move(source).as_unique();
  CHECK_NOT_EQUAL(&as<storage::ListStorage>(result.storage()).values(),
                  &as<storage::ListStorage>(alias.storage()).values());
  CHECK_EQUAL(as<storage::ListStorage>(result.storage()).spans().begin(),
              nested_spans);
  CHECK_EQUAL(as<storage::ListStorage>(alias.storage()).spans().begin(),
              nested_spans);
}

TEST("list array as_unique reuses unique array storage") {
  auto value_builder = ArrayBuilder<Int>{};
  value_builder.data(std::int64_t{42});
  auto spans
    = storage::SharedOwner<storage::Span[]>::make_value(1, storage::Span{0, 1});
  auto source
    = Array<List>{std::move(spans), Array<Data>{value_builder.finish()}};
  auto const* values = &as<storage::ListStorage>(source.storage()).values();
  auto result = std::move(source).as_unique();
  CHECK_EQUAL(&as<storage::ListStorage>(result.storage()).values(), values);
}

TEST("record array as_unique copies shared array storage") {
  auto values = ArrayBuilder<Int>{};
  values.data(std::int64_t{42});
  auto source = Array<Record>::make_empty(1).with_field_overwrite(
    "x", {.data = Array<Data>{values.finish()},
          .present = storage::BitMap{1, true}});
  auto alias = source;
  auto result = std::move(source).as_unique();
  auto extracted = std::move(result).dangerously_extract_field("x");
  REQUIRE(extracted.is_some());
  auto alias_field = alias.field("x");
  REQUIRE(alias_field.is_some());
  CHECK_EQUAL(as_int(alias_field->data.get(0)), 42);
}

TEST("bool array data/none roundtrip") {
  check_scalar_roundtrip<Bool>({true, false, true, true, false});
}

TEST("int array data/none roundtrip") {
  check_scalar_roundtrip<Int>(
    {std::int64_t{-42}, std::int64_t{0}, std::int64_t{7}, std::int64_t{1000}});
}

TEST("uint array data/none roundtrip") {
  check_scalar_roundtrip<UInt>(
    {std::uint64_t{0}, std::uint64_t{42}, std::uint64_t{1000}});
}

TEST("float array data/none roundtrip") {
  check_scalar_roundtrip<Float>({0.0, -1.5, 3.25, 100.0});
}

TEST("string array data/none roundtrip") {
  check_scalar_roundtrip<String>({std::string_view{"hello"},
                                  std::string_view{""},
                                  std::string_view{"a longer string value"}});
}

TEST("duration array data/none roundtrip") {
  check_scalar_roundtrip<Duration>({Duration{std::chrono::seconds{0}},
                                    Duration{std::chrono::seconds{5}},
                                    Duration{std::chrono::milliseconds{-250}}});
}

TEST("null array length tracks null/none calls") {
  auto builder = ArrayBuilder<Null>{};
  builder.null();
  builder.skip();
  builder.null();
  auto arr = builder.finish();
  CHECK_EQUAL(arr.length(), 3);
}

TEST("null array builder resets length after finish") {
  auto builder = ArrayBuilder<Null>{};
  builder.null();
  builder.null();
  auto first = builder.finish();
  CHECK_EQUAL(first.length(), 2);
  builder.null();
  auto second = builder.finish();
  CHECK_EQUAL(second.length(), 1);
}

TEST("record array tracks per-row shapes") {
  auto builder = ArrayBuilder<Record>{};
  auto row0 = builder.record();
  row0.field("a").data(std::int64_t{1});
  auto row1 = builder.record();
  row1.field("a").data(std::int64_t{2});
  row1.field("b").data(std::int64_t{3});
  builder.skip();
  auto arr = builder.finish();
  CHECK_EQUAL(arr.length(), 3);

  auto row0_names = record_field_names(arr.get(0));
  REQUIRE_EQUAL(row0_names.size(), 1u);
  CHECK_EQUAL(row0_names[0], "a");

  auto row1_names = record_field_names(arr.get(1));
  REQUIRE_EQUAL(row1_names.size(), 2u);
  CHECK_EQUAL(row1_names[0], "a");
  CHECK_EQUAL(row1_names[1], "b");

  // Row 2 was built via `.none()`; `Array<Record>::get` asserts on a valid
  // shape index, so it isn't safe to call here. The absence of that call is
  // itself the coverage: row 0 and row 1's shapes above must stay correct
  // regardless of the `.none()` row sitting between records with different
  // shapes and the fully-populated ones.
}

TEST("record array reuses shape definitions for identical field sets") {
  auto builder = ArrayBuilder<Record>{};
  auto row0 = builder.record();
  row0.field("a").data(std::int64_t{1});
  auto row1 = builder.record();
  row1.field("a").data(std::int64_t{2});
  auto arr = builder.finish();
  CHECK_EQUAL(arr.length(), 2);

  auto row0_names = record_field_names(arr.get(0));
  auto row1_names = record_field_names(arr.get(1));
  CHECK_EQUAL(row0_names, row1_names);
}

TEST("shape table bulk-imports requested definitions") {
  auto source = ShapeTable{};
  auto const source_a = source.with_field(ShapeTable::empty_shape, 0);
  auto const source_ac = source.with_field(source_a, 2);
  auto const source_b = source.with_field(ShapeTable::empty_shape, 1);
  auto destination = ShapeTable{};
  auto destination_ac = destination.with_field(ShapeTable::empty_shape, 4);
  destination_ac = destination.with_field(destination_ac, 9);
  auto const field_remap = std::array<storage::Index, 3>{4, 7, 9};
  auto const requested = std::array<ShapeTable::ShapeId, 1>{source_ac};

  auto imported = destination.import_shapes(source, field_remap, requested);
  REQUIRE(imported.contains(source_ac));
  CHECK(not imported.contains(source_a));
  CHECK(not imported.contains(source_b));
  CHECK_EQUAL(imported.at(source_ac), destination_ac);
  CHECK_EQUAL(destination.with_field(imported.at(source_ac), 4),
              destination_ac);
}

TEST("list array tracks empty-list vs none rows") {
  auto builder = ArrayBuilder<List>{};
  auto row0 = builder.list();
  static_cast<void>(row0);
  builder.skip();
  auto row2 = builder.list();
  static_cast<void>(row2);
  auto arr = builder.finish();
  CHECK_EQUAL(arr.length(), 3);

  auto row0_view = arr.get(0);
  CHECK_EQUAL(row0_view.length(), 0);
  CHECK(row0_view.begin() == row0_view.end());

  auto row2_view = arr.get(2);
  CHECK_EQUAL(row2_view.length(), 0);
  CHECK(row2_view.begin() == row2_view.end());
}

TEST("data array builder switches between alternatives") {
  auto builder = ArrayBuilder<Data>{};
  builder.data(std::int64_t{1});
  builder.data(std::string_view{"value"});
  builder.null();
  builder.data(std::int64_t{2});
  auto arr = builder.finish();
  CHECK_EQUAL(arr.length(), 4);
}

TEST("record array with_field overwrites an existing field in place") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto arr = builder.finish();

  auto replacement = ArrayBuilder<Record>{};
  replacement.record().field("x").data(std::int64_t{2});
  auto new_x = *replacement.finish().field("x");

  auto updated = arr.with_field_overwrite("x", new_x);
  CHECK_EQUAL(updated.length(), 1);
  auto x = updated.field("x");
  CHECK(x->present.get(0));
  auto ints = x->data.try_as<Int>();
  REQUIRE(ints.is_some());
  CHECK_EQUAL(*ints->get(0), 2);

  auto original_x = arr.field("x");
  REQUIRE(original_x.is_some());
  auto original_ints = original_x->data.try_as<Int>();
  REQUIRE(original_ints.is_some());
  CHECK_EQUAL(*original_ints->get(0), 1);
}

TEST("record array consuming overwrite detaches shared storage") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto original = builder.finish();
  auto consumed_alias = original;
  auto replacement_builder = ArrayBuilder<Record>{};
  replacement_builder.record().field("y").data(std::int64_t{2});
  auto replacement = *replacement_builder.finish().field("y");

  auto updated = std::move(consumed_alias)
                   .with_field_overwrite("y", std::move(replacement));

  CHECK_EQUAL(record_field_names(original.get(0)),
              (std::vector<std::string>{"x"}));
  CHECK(not original.field("y"));
  CHECK_EQUAL(record_field_names(updated.get(0)),
              (std::vector<std::string>{"x", "y"}));
}

TEST("record array dangerously extracts and restores a field") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto record = builder.finish();

  auto extracted = std::move(record).dangerously_extract_field("x");

  REQUIRE(extracted.is_some());
  auto values = extracted->data.try_as<Int>();
  REQUIRE(values.is_some());
  CHECK_EQUAL(*values->get(0), 1);
  record = std::move(record).with_field_overwrite("x", std::move(*extracted));
  CHECK_EQUAL(record_field_names(record.get(0)),
              (std::vector<std::string>{"x"}));
}

TEST("record array dangerous extraction detaches shared storage") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto original = builder.finish();
  auto record = original;

  auto extracted = std::move(record).dangerously_extract_field("x");

  REQUIRE(extracted.is_some());
  record = std::move(record).with_field_overwrite("x", std::move(*extracted));
  auto original_x = original.field("x");
  REQUIRE(original_x.is_some());
  auto original_values = original_x->data.try_as<Int>();
  REQUIRE(original_values.is_some());
  CHECK_EQUAL(*original_values->get(0), 1);
  CHECK_EQUAL(record_field_names(record.get(0)),
              (std::vector<std::string>{"x"}));
}

TEST("record array consuming overwrite reuses shape index storage") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::int64_t{2});
  auto arr = builder.finish();
  auto replacement_builder = ArrayBuilder<Record>{};
  replacement_builder.record().field("y").data(std::int64_t{10});
  replacement_builder.record().field("y").data(std::int64_t{20});
  auto replacement = *replacement_builder.finish().field("y");
  auto const allocations_before = tenzir::memory::nova_data_allocator()
                                    .stats()
                                    .allocations_cumulative.load();

  auto updated
    = std::move(arr).with_field_overwrite("y", std::move(replacement));

  auto const allocations_after = tenzir::memory::nova_data_allocator()
                                   .stats()
                                   .allocations_cumulative.load();
  CHECK_EQUAL(allocations_after, allocations_before);
  CHECK_EQUAL(record_field_names(updated.get(0)),
              (std::vector<std::string>{"x", "y"}));
  CHECK_EQUAL(record_field_names(updated.get(1)),
              (std::vector<std::string>{"x", "y"}));
}

TEST("nested field assignment consumes uniquely owned records") {
  auto nested_builder = ArrayBuilder<Record>{};
  nested_builder.record().field("a").data(std::int64_t{1});
  nested_builder.record().field("a").data(std::int64_t{2});
  auto outer = Array<Record>::make_empty(2).with_field_overwrite(
    "nested", MaskedArray<Array<Data>>{
                Array<Data>{nested_builder.finish()},
                storage::BitMap{2, true},
              });
  auto replacement_builder = ArrayBuilder<Record>{};
  replacement_builder.record().field("b").data(std::int64_t{10});
  replacement_builder.record().field("b").data(std::int64_t{20});
  auto replacement = *replacement_builder.finish().field("b");
  auto path = make_field_path({"nested", "b"});

  auto updated = assign_nested_field(std::move(outer), path.path(),
                                     std::move(replacement));

  auto nested_field = updated.field("nested");
  REQUIRE(nested_field.is_some());
  auto nested = nested_field->data.try_as<Record>();
  REQUIRE(nested);
  CHECK_EQUAL(record_field_names(nested->get(0)),
              (std::vector<std::string>{"a", "b"}));
  CHECK_EQUAL(record_field_names(nested->get(1)),
              (std::vector<std::string>{"a", "b"}));
}

TEST("record array with_field rewrites shapes for newly present values") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("y").data(std::int64_t{2});
  auto arr = builder.finish();
  auto replacement_builder = ArrayBuilder<Record>{};
  replacement_builder.record().field("x").data(std::int64_t{10});
  replacement_builder.record().field("x").data(std::int64_t{20});
  auto replacement = *replacement_builder.finish().field("x");
  replacement.present = bitmap({false, true});

  auto updated = arr.with_field_overwrite("x", std::move(replacement));

  CHECK_EQUAL(record_field_names(updated.get(0)),
              (std::vector<std::string>{"x"}));
  CHECK_EQUAL(record_field_names(updated.get(1)),
              (std::vector<std::string>{"y", "x"}));
  auto x = updated.field("x");
  REQUIRE(x.is_some());
  CHECK(not x->present.get(0));
  CHECK(x->present.get(1));
  auto values = x->data.try_as<Int>();
  REQUIRE(values.is_some());
  CHECK_EQUAL(*values->get(0), 10);
  CHECK_EQUAL(*values->get(1), 20);
}

TEST("record array with_field adds a new field visible on every row") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::int64_t{2});
  auto arr = builder.finish();

  auto y_source = ArrayBuilder<Record>{};
  y_source.record().field("y").data(std::int64_t{10});
  y_source.record().field("y").data(std::int64_t{20});
  auto new_y = *y_source.finish().field("y");

  auto updated = arr.with_field_overwrite("y", new_y);
  CHECK_EQUAL(updated.length(), 2);
  for (auto i = storage::Index{0}; i < 2; ++i) {
    auto names = record_field_names(updated.get(i));
    REQUIRE_EQUAL(names.size(), 2u);
    CHECK_EQUAL(names[0], "x");
    CHECK_EQUAL(names[1], "y");
  }
  auto y = updated.field("y");
  auto y_ints = y->data.try_as<Int>();
  REQUIRE(y_ints.is_some());
  CHECK_EQUAL(*y_ints->get(0), 10);
  CHECK_EQUAL(*y_ints->get(1), 20);
}

TEST("record array with_field preserves heterogeneous shapes for untouched "
     "fields") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("a").data(std::int64_t{1});
  builder.record().field("b").data(std::int64_t{2});
  auto arr = builder.finish();

  auto z_source = ArrayBuilder<Record>{};
  z_source.record().field("z").data(std::int64_t{100});
  z_source.record().field("z").data(std::int64_t{200});
  auto new_z = *z_source.finish().field("z");

  auto updated = arr.with_field_overwrite("z", new_z);
  CHECK_EQUAL(updated.length(), 2);

  auto row0_names = record_field_names(updated.get(0));
  REQUIRE_EQUAL(row0_names.size(), 2u);
  CHECK_EQUAL(row0_names[0], "a");
  CHECK_EQUAL(row0_names[1], "z");

  auto row1_names = record_field_names(updated.get(1));
  REQUIRE_EQUAL(row1_names.size(), 2u);
  CHECK_EQUAL(row1_names[0], "b");
  CHECK_EQUAL(row1_names[1], "z");
}

TEST("record array with_field replaces record columns") {
  auto old_nested_builder = ArrayBuilder<Record>{};
  for (auto i = std::int64_t{0}; i < 3; ++i) {
    auto row = old_nested_builder.record();
    row.field("a").data(i + 1);
    row.field("b").data(i + 10);
  }
  auto old_nested = old_nested_builder.finish();
  auto outer = Array<Record>::make_empty(3).with_field_overwrite(
    "nested", MaskedArray<Array<Data>>{
                Array<Data>{std::move(old_nested)},
                storage::BitMap{3, true},
              });

  auto new_nested_builder = ArrayBuilder<Record>{};
  new_nested_builder.record().field("a").data(std::int64_t{100});
  new_nested_builder.record().field("a").data(std::int64_t{200});
  new_nested_builder.record().field("a").data(std::int64_t{300});
  auto new_nested = new_nested_builder.finish();
  auto updated
    = outer.with_field_overwrite("nested", MaskedArray<Array<Data>>{
                                             Array<Data>{std::move(new_nested)},
                                             bitmap({false, true, false}),
                                           });

  auto nested_field = updated.field("nested");
  REQUIRE(nested_field.is_some());
  CHECK(not nested_field->present.get(0));
  CHECK(nested_field->present.get(1));
  CHECK(not nested_field->present.get(2));
  auto nested = nested_field->data.try_as<Record>();
  REQUIRE(nested);
  CHECK_EQUAL(record_field_names(nested->get(0)),
              (std::vector<std::string>{"a"}));
  CHECK_EQUAL(record_field_names(nested->get(1)),
              (std::vector<std::string>{"a"}));
  CHECK_EQUAL(record_field_names(nested->get(2)),
              (std::vector<std::string>{"a"}));
  auto a = nested->field("a");
  REQUIRE(a.is_some());
  auto a_values = a->data.try_as<Int>();
  REQUIRE(a_values.is_some());
  CHECK_EQUAL(*a_values->get(0), 100);
  CHECK_EQUAL(*a_values->get(1), 200);
  CHECK_EQUAL(*a_values->get(2), 300);
  CHECK(not nested->field("b"));
}

TEST("record array with_field replaces columns containing null fields") {
  auto old_nested_builder = ArrayBuilder<Record>{};
  old_nested_builder.record();
  old_nested_builder.record().field("x").data(std::int64_t{1});
  auto outer = Array<Record>::make_empty(2).with_field_overwrite(
    "nested", MaskedArray<Array<Data>>{
                Array<Data>{old_nested_builder.finish()},
                storage::BitMap{2, true},
              });
  auto new_nested_builder = ArrayBuilder<Record>{};
  new_nested_builder.record().field("x").null();
  new_nested_builder.record();
  auto updated = outer.with_field_overwrite(
    "nested", MaskedArray<Array<Data>>{
                Array<Data>{new_nested_builder.finish()},
                bitmap({true, false}),
              });

  auto nested_field = updated.field("nested");
  REQUIRE(nested_field.is_some());
  CHECK(nested_field->present.get(0));
  CHECK(not nested_field->present.get(1));
  auto nested = nested_field->data.try_as<Record>();
  REQUIRE(nested);
  CHECK_EQUAL(record_field_names(nested->get(0)),
              (std::vector<std::string>{"x"}));
  CHECK(record_field_names(nested->get(1)).empty());
  auto x = nested->field("x");
  REQUIRE(x.is_some());
  auto nulls = x->data.get_alternative<Null>();
  REQUIRE(nulls.is_some());
  CHECK(nulls->present.get(0));
  CHECK(not x->data.get_alternative<Int>());
}

TEST("record array with_field replaces a column with a different type") {
  auto old_builder = ArrayBuilder<Record>{};
  old_builder.record().field("x").data(std::int64_t{1});
  old_builder.record().field("x").data(std::int64_t{2});
  old_builder.record().field("x").data(std::int64_t{3});
  auto outer = old_builder.finish();
  auto new_builder = ArrayBuilder<Record>{};
  new_builder.record().field("x").data(std::string_view{"a"});
  new_builder.record().field("x").data(std::string_view{"b"});
  new_builder.record().field("x").data(std::string_view{"c"});
  auto replacement = *new_builder.finish().field("x");
  replacement.present = bitmap({false, true, false});
  auto updated = outer.with_field_overwrite("x", std::move(replacement));

  auto x = updated.field("x");
  REQUIRE(x.is_some());
  CHECK(not x->present.get(0));
  CHECK(x->present.get(1));
  CHECK(not x->present.get(2));
  auto strings = x->data.try_as<String>();
  REQUIRE(strings.is_some());
  CHECK_EQUAL(*strings->get(0), "a");
  CHECK_EQUAL(*strings->get(1), "b");
  CHECK_EQUAL(*strings->get(2), "c");
}

TEST("data array null_where uses true bits as null positions") {
  auto builder = ArrayBuilder<Int>{};
  builder.data(std::int64_t{1});
  builder.data(std::int64_t{2});
  builder.data(std::int64_t{3});
  builder.data(std::int64_t{4});
  auto const data = Array<Data>{builder.finish()};

  auto unchanged = data.null_where(storage::BitMap{4, false});
  auto unchanged_ints = unchanged.try_as<Int>();
  REQUIRE(unchanged_ints.is_some());
  CHECK_EQUAL(*unchanged_ints->get(0), 1);
  CHECK_EQUAL(*unchanged_ints->get(3), 4);
  auto all_null = data.null_where(storage::BitMap{4, true});
  CHECK(is_null(all_null));

  auto sparse = data.null_where(bitmap({false, false, false, true}));
  auto const* sparse_union = tenzir::try_as<UnionArray>(sparse);
  REQUIRE(sparse_union);
  CHECK_EQUAL(sparse_union->alternative_index_at(0), 0);
  CHECK_EQUAL(sparse_union->alternative_index_at(1), 0);
  CHECK_EQUAL(sparse_union->alternative_index_at(2), 0);
  CHECK_EQUAL(sparse_union->alternative_index_at(3), 1);
  CHECK_EQUAL(as_int(sparse.get(0)), 1);
  CHECK(is_null(sparse.get(3)));

  auto dense = data.null_where(bitmap({true, true, true, false}));
  auto const* dense_union = tenzir::try_as<UnionArray>(dense);
  REQUIRE(dense_union);
  CHECK_EQUAL(dense_union->alternative_index_at(0), 0);
  CHECK_EQUAL(dense_union->alternative_index_at(1), 0);
  CHECK_EQUAL(dense_union->alternative_index_at(2), 0);
  CHECK_EQUAL(dense_union->alternative_index_at(3), 1);
  CHECK(is_null(dense.get(0)));
  CHECK_EQUAL(as_int(dense.get(3)), 4);

  auto tied = data.null_where(bitmap({true, false, true, false}));
  auto const* tied_union = tenzir::try_as<UnionArray>(tied);
  REQUIRE(tied_union);
  CHECK_EQUAL(tied_union->alternative_index_at(0), 1);
  CHECK_EQUAL(tied_union->alternative_index_at(1), 0);
  CHECK_EQUAL(tied_union->alternative_index_at(2), 1);
  CHECK_EQUAL(tied_union->alternative_index_at(3), 0);
  CHECK(is_null(tied.get(0)));
  CHECK_EQUAL(as_int(tied.get(1)), 2);
}

TEST("union array null_where preserves unmasked indices") {
  auto builder = ArrayBuilder<Data>{};
  builder.data(std::int64_t{1});
  builder.data(std::string_view{"two"});
  builder.data(std::int64_t{3});
  auto const source = builder.finish();
  auto const* source_union = tenzir::try_as<UnionArray>(source);
  REQUIRE(source_union);
  REQUIRE_EQUAL(source_union->fields().size(), 2u);

  auto result = source.null_where(bitmap({false, true, false}));
  auto const* result_union = tenzir::try_as<UnionArray>(result);
  REQUIRE(result_union);
  REQUIRE_EQUAL(result_union->fields().size(), 3u);
  CHECK_EQUAL(result_union->alternative_index_at(0),
              source_union->alternative_index_at(0));
  CHECK_EQUAL(result_union->alternative_index_at(2),
              source_union->alternative_index_at(2));
  CHECK(is_null(result.get(1)));
  CHECK_EQUAL(as_string(source.get(1)), "two");
}

TEST("union array null_where reuses an existing null alternative") {
  auto builder = ArrayBuilder<Int>{};
  builder.data(std::int64_t{1});
  builder.data(std::int64_t{2});
  builder.data(std::int64_t{3});
  auto const data = Array<Data>{builder.finish()};
  auto const source = data.null_where(bitmap({false, true, false}));
  auto const* source_union = tenzir::try_as<UnionArray>(source);
  REQUIRE(source_union);
  REQUIRE_EQUAL(source_union->fields().size(), 2u);

  auto result = source.null_where(bitmap({true, false, false}));
  auto const* result_union = tenzir::try_as<UnionArray>(result);
  REQUIRE(result_union);
  REQUIRE_EQUAL(result_union->fields().size(), 2u);
  CHECK(is_null(result.get(0)));
  CHECK(is_null(result.get(1)));
  CHECK_EQUAL(as_int(result.get(2)), 3);
  CHECK_EQUAL(as_int(source.get(0)), 1);
}

TEST("sparse storage mutable copies shared input before mutation") {
  auto builder = storage::SparseStorage<storage::Index>::Mutable{3};
  builder.set(0, 10);
  builder.set(1, 20);
  builder.set(2, 30);
  auto const source = std::move(builder).finish();
  auto mutable_copy = storage::SparseStorage<storage::Index>::Mutable{source};
  mutable_copy.set(1, 99);
  auto result = std::move(mutable_copy).finish();
  CHECK_EQUAL(source.get(0), 10);
  CHECK_EQUAL(source.get(1), 20);
  CHECK_EQUAL(source.get(2), 30);
  CHECK_EQUAL(result.get(0), 10);
  CHECK_EQUAL(result.get(1), 99);
  CHECK_EQUAL(result.get(2), 30);
}

TEST("array merge recursively merges shared union alternatives") {
  auto old_builder = ArrayBuilder<Data>{};
  old_builder.data(std::int64_t{1});
  old_builder.data(std::string_view{"old-1"});
  old_builder.data(std::int64_t{3});
  old_builder.data(std::string_view{"old-3"});
  auto new_builder = ArrayBuilder<Data>{};
  new_builder.data(std::string_view{"new-0"});
  new_builder.data(std::string_view{"new-1"});
  new_builder.data(std::int64_t{30});
  new_builder.data(std::int64_t{40});
  auto old
    = MaskedArray<Array<Data>>{old_builder.finish(), storage::BitMap{4, true}};
  auto new_ = MaskedArray<Array<Data>>{new_builder.finish(),
                                       bitmap({false, true, true, false})};

  auto merged = with_merged(old, new_);
  auto const* values = tenzir::try_as<UnionArray>(merged);
  REQUIRE(values);
  auto ints = values->get_alternative<Int>();
  REQUIRE(ints.is_some());
  CHECK(ints->present.get(0));
  CHECK(not ints->present.get(1));
  CHECK(ints->present.get(2));
  CHECK(not ints->present.get(3));
  CHECK_EQUAL(*ints->data.get(0), 1);
  CHECK_EQUAL(*ints->data.get(2), 30);
  auto strings = values->get_alternative<String>();
  REQUIRE(strings.is_some());
  CHECK(not strings->present.get(0));
  CHECK(strings->present.get(1));
  CHECK(not strings->present.get(2));
  CHECK(strings->present.get(3));
  CHECK_EQUAL(*strings->data.get(1), "new-1");
  CHECK_EQUAL(*strings->data.get(3), "old-3");
}

TEST("array merge normalizes different physical storages") {
  auto old_storage = storage::SharedOwner<std::int8_t[]>::Builder{};
  old_storage.emplace_back(std::int8_t{1});
  old_storage.emplace_back(std::int8_t{2});
  old_storage.emplace_back(std::int8_t{3});
  auto old = MaskedArray<Array<Data>>{
    Array<Data>{
      Array<Int>{storage::SparseStorage<std::int8_t>{old_storage.finish()}}},
    storage::BitMap{3, true},
  };
  auto new_ = MaskedArray<Array<Data>>{
    Array<Data>{Array<Int>{storage::ConstantStorage<std::int64_t>{3, 42}}},
    bitmap({false, true, false}),
  };

  auto merged = with_merged(old, new_);
  auto ints = merged.try_as<Int>();
  REQUIRE(ints.is_some());
  CHECK_EQUAL(*ints->get(0), 1);
  CHECK_EQUAL(*ints->get(1), 42);
  CHECK_EQUAL(*ints->get(2), 3);
  match(
    ints->storage(),
    [](storage::SparseStorage<std::int64_t> const&) {
      CHECK(true);
    },
    [](auto const&) {
      FAIL("expected primary int storage");
    });
}

TEST("array merge copies primary old storage before sparse replacements") {
  auto old_ints_builder = ArrayBuilder<Int>{};
  old_ints_builder.data(std::int64_t{1});
  old_ints_builder.data(std::int64_t{2});
  old_ints_builder.data(std::int64_t{3});
  old_ints_builder.data(std::int64_t{4});
  auto old_ints = MaskedArray<Array<Data>>{
    Array<Data>{old_ints_builder.finish()}, storage::BitMap{4, true}};
  auto new_ints = MaskedArray<Array<Data>>{
    Array<Data>{
      Array<Int>{storage::ConstantStorage<std::int64_t>{4, std::int64_t{42}}}},
    bitmap({false, true, false, true}),
  };
  auto merged = with_merged(old_ints, new_ints);
  auto ints = merged.try_as<Int>();
  REQUIRE(ints.is_some());
  CHECK_EQUAL(*ints->get(0), 1);
  CHECK_EQUAL(*ints->get(1), 42);
  CHECK_EQUAL(*ints->get(2), 3);
  CHECK_EQUAL(*ints->get(3), 42);
  match(
    ints->storage(),
    [](storage::SparseStorage<std::int64_t> const&) {
      CHECK(true);
    },
    [](auto const&) {
      FAIL("expected primary int storage");
    });
  auto old_bools_builder = ArrayBuilder<Bool>{};
  old_bools_builder.data(true);
  old_bools_builder.data(false);
  old_bools_builder.data(false);
  old_bools_builder.data(true);
  auto new_bools_builder = ArrayBuilder<Bool>{};
  new_bools_builder.data(false);
  new_bools_builder.data(true);
  new_bools_builder.data(true);
  new_bools_builder.data(false);
  auto old_bools = MaskedArray<Array<Data>>{
    Array<Data>{old_bools_builder.finish()}, storage::BitMap{4, true}};
  auto new_bools = MaskedArray<Array<Data>>{
    Array<Data>{new_bools_builder.finish()},
    bitmap({false, true, true, false}),
  };
  merged = with_merged(old_bools, new_bools);
  auto bools = merged.try_as<Bool>();
  REQUIRE(bools.is_some());
  CHECK_EQUAL(*bools->get(0), true);
  CHECK_EQUAL(*bools->get(1), true);
  CHECK_EQUAL(*bools->get(2), true);
  CHECK_EQUAL(*bools->get(3), true);
}

TEST("array merge copies primary string storage before appending "
     "replacements") {
  auto old_builder = ArrayBuilder<String>{};
  old_builder.data(std::string_view{"a"});
  old_builder.data(std::string_view{"bb"});
  old_builder.data(std::string_view{"ccc"});
  old_builder.data(std::string_view{"dddd"});
  auto old = MaskedArray<Array<Data>>{Array<Data>{old_builder.finish()},
                                      storage::BitMap{4, true}};
  auto new_ = MaskedArray<Array<Data>>{
    Array<Data>{Array<String>{
      storage::ConstantStorage<std::string, std::string_view>{4, "new"}}},
    bitmap({false, true, false, true}),
  };
  auto merged = with_merged(old, new_);
  auto strings = merged.try_as<String>();
  REQUIRE(strings.is_some());
  CHECK_EQUAL(*strings->get(0), "a");
  CHECK_EQUAL(*strings->get(1), "new");
  CHECK_EQUAL(*strings->get(2), "ccc");
  CHECK_EQUAL(*strings->get(3), "new");
  match(
    strings->storage(),
    [](storage::DenseStringOffsetStorage const& storage) {
      CHECK_EQUAL(storage.data().length(), 13);
    },
    [](auto const&) {
      FAIL("expected primary string storage");
    });
}

TEST("array merge retains zipped string storage fallback") {
  auto old = MaskedArray<Array<Data>>{
    Array<Data>{Array<String>{
      storage::ConstantStorage<std::string, std::string_view>{4, "old"}}},
    storage::BitMap{4, true},
  };
  auto new_builder = ArrayBuilder<String>{};
  new_builder.data(std::string_view{"unused-0"});
  new_builder.data(std::string_view{"new-1"});
  new_builder.data(std::string_view{"unused-2"});
  new_builder.data(std::string_view{"new-3"});
  auto new_ = MaskedArray<Array<Data>>{Array<Data>{new_builder.finish()},
                                       bitmap({false, true, false, true})};
  auto merged = with_merged(old, new_);
  auto strings = merged.try_as<String>();
  REQUIRE(strings.is_some());
  CHECK_EQUAL(*strings->get(0), "old");
  CHECK_EQUAL(*strings->get(1), "new-1");
  CHECK_EQUAL(*strings->get(2), "old");
  CHECK_EQUAL(*strings->get(3), "new-3");
}

TEST("blob array builder round-trips variable length payloads") {
  auto const a = std::array<std::byte, 1>{std::byte{0x00}};
  auto const b = std::array<std::byte, 3>{std::byte{0xde}, std::byte{0xad},
                                          std::byte{0xff}};
  auto builder = ArrayBuilder<Blob>{};
  builder.data(BlobView{a});
  builder.data(BlobView{b});
  builder.data(BlobView{});
  auto blobs = builder.finish();
  REQUIRE_EQUAL(blobs.length(), 3);
  CHECK_EQUAL(*blobs.get(0), BlobView{a});
  CHECK_EQUAL(*blobs.get(1), BlobView{b});
  CHECK_EQUAL((*blobs.get(2)).size(), 0u);
}

TEST("array merge copies primary blob storage before appending replacements") {
  auto const one = std::array<std::byte, 1>{std::byte{0xa0}};
  auto const two = std::array<std::byte, 2>{std::byte{0xb0}, std::byte{0xb1}};
  auto const three = std::array<std::byte, 3>{std::byte{0xc0}, std::byte{0xc1},
                                              std::byte{0xc2}};
  auto const four = std::array<std::byte, 4>{std::byte{0xd0}, std::byte{0xd1},
                                             std::byte{0xd2}, std::byte{0xd3}};
  auto old_builder = ArrayBuilder<Blob>{};
  old_builder.data(BlobView{one});
  old_builder.data(BlobView{two});
  old_builder.data(BlobView{three});
  old_builder.data(BlobView{four});
  auto old = MaskedArray<Array<Data>>{Array<Data>{old_builder.finish()},
                                      storage::BitMap{4, true}};
  auto const replacement = Blob{std::byte{0xee}, std::byte{0xff}};
  auto new_ = MaskedArray<Array<Data>>{
    Array<Data>{
      Array<Blob>{storage::ConstantStorage<Blob, BlobView>{4, replacement}}},
    bitmap({false, true, false, true}),
  };
  auto merged = with_merged(old, new_);
  auto blobs = merged.try_as<Blob>();
  REQUIRE(blobs.is_some());
  CHECK_EQUAL(*blobs->get(0), BlobView{one});
  CHECK_EQUAL(*blobs->get(1), BlobView{replacement});
  CHECK_EQUAL(*blobs->get(2), BlobView{three});
  CHECK_EQUAL(*blobs->get(3), BlobView{replacement});
  match(
    blobs->storage(),
    [](storage::DenseBlobOffsetStorage const& storage) {
      // 1 + 2 + 3 + 4 copied verbatim, plus one shared copy of the
      // two-byte replacement thanks to `set_same_as`.
      CHECK_EQUAL(storage.data().length(), 12);
    },
    [](auto const&) {
      FAIL("expected primary blob storage");
    });
}

TEST("array merge retains zipped blob storage fallback") {
  auto const old_value = Blob{std::byte{0x01}};
  auto old = MaskedArray<Array<Data>>{
    Array<Data>{
      Array<Blob>{storage::ConstantStorage<Blob, BlobView>{4, old_value}}},
    storage::BitMap{4, true},
  };
  auto const unused
    = std::array<std::byte, 2>{std::byte{0x77}, std::byte{0x88}};
  auto const new_1 = std::array<std::byte, 1>{std::byte{0x11}};
  auto const new_3 = std::array<std::byte, 3>{std::byte{0x31}, std::byte{0x32},
                                              std::byte{0x33}};
  auto new_builder = ArrayBuilder<Blob>{};
  new_builder.data(BlobView{unused});
  new_builder.data(BlobView{new_1});
  new_builder.data(BlobView{unused});
  new_builder.data(BlobView{new_3});
  auto new_ = MaskedArray<Array<Data>>{Array<Data>{new_builder.finish()},
                                       bitmap({false, true, false, true})};
  auto merged = with_merged(old, new_);
  auto blobs = merged.try_as<Blob>();
  REQUIRE(blobs.is_some());
  CHECK_EQUAL(*blobs->get(0), BlobView{old_value});
  CHECK_EQUAL(*blobs->get(1), BlobView{new_1});
  CHECK_EQUAL(*blobs->get(2), BlobView{old_value});
  CHECK_EQUAL(*blobs->get(3), BlobView{new_3});
}

TEST("array merge handles union and concrete inputs in both directions") {
  auto union_builder = ArrayBuilder<Data>{};
  union_builder.data(std::int64_t{1});
  union_builder.data(std::string_view{"old"});
  union_builder.data(std::int64_t{3});
  auto ints_builder = ArrayBuilder<Int>{};
  ints_builder.data(std::int64_t{10});
  ints_builder.data(std::int64_t{20});
  ints_builder.data(std::int64_t{30});
  auto old_union = MaskedArray<Array<Data>>{union_builder.finish(),
                                            storage::BitMap{3, true}};
  auto new_ints = MaskedArray<Array<Data>>{Array<Data>{ints_builder.finish()},
                                           bitmap({false, false, true})};

  auto merged = with_merged(old_union, new_ints);
  auto const* values = tenzir::try_as<UnionArray>(merged);
  REQUIRE(values);
  auto ints = values->get_alternative<Int>();
  REQUIRE(ints.is_some());
  CHECK_EQUAL(*ints->data.get(0), 1);
  CHECK_EQUAL(*ints->data.get(2), 30);
  auto strings = values->get_alternative<String>();
  REQUIRE(strings.is_some());
  CHECK(strings->present.get(1));
  CHECK_EQUAL(*strings->data.get(1), "old");

  auto old_ints_builder = ArrayBuilder<Int>{};
  old_ints_builder.data(std::int64_t{1});
  old_ints_builder.data(std::int64_t{2});
  old_ints_builder.data(std::int64_t{3});
  auto new_union_builder = ArrayBuilder<Data>{};
  new_union_builder.data(std::string_view{"new"});
  new_union_builder.data(std::int64_t{20});
  new_union_builder.data(std::string_view{"unused"});
  auto old_ints = MaskedArray<Array<Data>>{
    Array<Data>{old_ints_builder.finish()}, storage::BitMap{3, true}};
  auto new_union = MaskedArray<Array<Data>>{new_union_builder.finish(),
                                            bitmap({true, true, false})};

  merged = with_merged(old_ints, new_union);
  values = tenzir::try_as<UnionArray>(merged);
  REQUIRE(values);
  ints = values->get_alternative<Int>();
  REQUIRE(ints.is_some());
  CHECK(not ints->present.get(0));
  CHECK(ints->present.get(1));
  CHECK(ints->present.get(2));
  CHECK_EQUAL(*ints->data.get(1), 20);
  CHECK_EQUAL(*ints->data.get(2), 3);
  strings = values->get_alternative<String>();
  REQUIRE(strings.is_some());
  CHECK(strings->present.get(0));
  CHECK_EQUAL(*strings->data.get(0), "new");
}

TEST("array merge keeps plain record and int alternatives separate") {
  auto old_builder = ArrayBuilder<Record>{};
  old_builder.record().field("a").data(std::int64_t{1});
  static_cast<void>(old_builder.record());
  old_builder.record().field("b").null();
  auto new_builder = ArrayBuilder<Int>{};
  new_builder.data(std::int64_t{10});
  new_builder.data(std::int64_t{20});
  new_builder.data(std::int64_t{30});
  auto old = MaskedArray<Array<Data>>{Array<Data>{old_builder.finish()},
                                      storage::BitMap{3, true}};
  auto new_ = MaskedArray<Array<Data>>{Array<Data>{new_builder.finish()},
                                       bitmap({false, true, false})};

  auto merged = with_merged(old, new_);
  match(
    merged.get(0),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record), (std::vector<std::string>{"a"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
  CHECK_EQUAL(as_int(merged.get(1)), 20);
  match(
    merged.get(2),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record), (std::vector<std::string>{"b"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });

  auto old_ints_builder = ArrayBuilder<Int>{};
  old_ints_builder.data(std::int64_t{1});
  old_ints_builder.data(std::int64_t{2});
  old_ints_builder.data(std::int64_t{3});
  auto new_records_builder = ArrayBuilder<Record>{};
  new_records_builder.record().field("unused").data(std::int64_t{10});
  new_records_builder.record().field("new").data(std::int64_t{20});
  new_records_builder.record().field("unused").data(std::int64_t{30});
  old = MaskedArray<Array<Data>>{Array<Data>{old_ints_builder.finish()},
                                 storage::BitMap{3, true}};
  new_ = MaskedArray<Array<Data>>{Array<Data>{new_records_builder.finish()},
                                  bitmap({false, true, false})};

  merged = with_merged(old, new_);
  CHECK_EQUAL(as_int(merged.get(0)), 1);
  match(
    merged.get(1),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record),
                  (std::vector<std::string>{"new"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
  CHECK_EQUAL(as_int(merged.get(2)), 3);
}

TEST("array merge handles one-sided record union alternatives") {
  auto old_builder = ArrayBuilder<Data>{};
  old_builder.record().field("old-a").data(std::int64_t{1});
  old_builder.data(std::string_view{"replace"});
  old_builder.record().field("old-b").null();
  auto new_ints_builder = ArrayBuilder<Int>{};
  new_ints_builder.data(std::int64_t{10});
  new_ints_builder.data(std::int64_t{20});
  new_ints_builder.data(std::int64_t{30});
  auto old
    = MaskedArray<Array<Data>>{old_builder.finish(), storage::BitMap{3, true}};
  auto new_ = MaskedArray<Array<Data>>{Array<Data>{new_ints_builder.finish()},
                                       bitmap({false, true, false})};

  auto merged = with_merged(old, new_);
  match(
    merged.get(0),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record),
                  (std::vector<std::string>{"old-a"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
  CHECK_EQUAL(as_int(merged.get(1)), 20);
  match(
    merged.get(2),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record),
                  (std::vector<std::string>{"old-b"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });

  auto old_ints_builder = ArrayBuilder<Int>{};
  old_ints_builder.data(std::int64_t{1});
  old_ints_builder.data(std::int64_t{2});
  old_ints_builder.data(std::int64_t{3});
  auto new_union_builder = ArrayBuilder<Data>{};
  new_union_builder.data(std::string_view{"unused"});
  new_union_builder.record().field("new").data(std::int64_t{20});
  new_union_builder.data(std::string_view{"unused"});
  old = MaskedArray<Array<Data>>{Array<Data>{old_ints_builder.finish()},
                                 storage::BitMap{3, true}};
  new_ = MaskedArray<Array<Data>>{new_union_builder.finish(),
                                  bitmap({false, true, false})};

  merged = with_merged(old, new_);
  CHECK_EQUAL(as_int(merged.get(0)), 1);
  match(
    merged.get(1),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record),
                  (std::vector<std::string>{"new"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
  CHECK_EQUAL(as_int(merged.get(2)), 3);
}

TEST("array merge recurses through shared structured union alternatives") {
  auto old_builder = ArrayBuilder<Data>{};
  auto old_list = old_builder.list();
  old_list.data(std::int64_t{1});
  old_list.data(std::string_view{"old-list"});
  old_builder.record().field("old").data(std::int64_t{11});
  old_builder.list().data(std::int64_t{3});
  old_builder.record().field("keep").data(std::int64_t{13});
  auto new_builder = ArrayBuilder<Data>{};
  new_builder.list().data(std::string_view{"unused"});
  new_builder.record().field("new").data(std::int64_t{21});
  auto new_list = new_builder.list();
  new_list.data(std::string_view{"new-list"});
  new_list.data(std::int64_t{30});
  new_builder.record().field("unused").data(std::int64_t{23});
  auto old
    = MaskedArray<Array<Data>>{old_builder.finish(), storage::BitMap{4, true}};
  auto new_ = MaskedArray<Array<Data>>{new_builder.finish(),
                                       bitmap({false, true, true, false})};

  auto merged = with_merged(old, new_);
  match(
    merged.get(0),
    [](RowView<List> list) {
      REQUIRE_EQUAL(list.length(), 2);
      CHECK_EQUAL(as_int(list.get(0)), 1);
      CHECK_EQUAL(as_string(list.get(1)), "old-list");
    },
    [](auto) {
      FAIL("expected a list row");
    });
  match(
    merged.get(1),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record),
                  (std::vector<std::string>{"new"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
  match(
    merged.get(2),
    [](RowView<List> list) {
      REQUIRE_EQUAL(list.length(), 2);
      CHECK_EQUAL(as_string(list.get(0)), "new-list");
      CHECK_EQUAL(as_int(list.get(1)), 30);
    },
    [](auto) {
      FAIL("expected a list row");
    });
  match(
    merged.get(3),
    [](RowView<Record> record) {
      CHECK_EQUAL(record_field_names(record),
                  (std::vector<std::string>{"keep"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
}

TEST("array merge aligns heterogeneous list values by row") {
  auto old_builder = ArrayBuilder<List>{};
  auto old_0 = old_builder.list();
  old_0.data(std::int64_t{1});
  old_0.data(std::string_view{"old-0"});
  auto old_1 = old_builder.list();
  old_1.data(std::int64_t{2});
  old_1.data(std::string_view{"old-1"});
  auto new_builder = ArrayBuilder<List>{};
  auto new_0 = new_builder.list();
  new_0.data(std::string_view{"unused"});
  new_0.data(std::int64_t{10});
  auto new_1 = new_builder.list();
  new_1.data(std::string_view{"new-1"});
  new_1.data(std::int64_t{20});
  auto old = MaskedArray<Array<Data>>{Array<Data>{old_builder.finish()},
                                      storage::BitMap{2, true}};
  auto new_ = MaskedArray<Array<Data>>{Array<Data>{new_builder.finish()},
                                       bitmap({false, true})};

  auto merged = with_merged(old, new_);
  auto lists = merged.try_as<List>();
  REQUIRE(lists);
  REQUIRE_EQUAL(lists->get(0).length(), 2);
  CHECK_EQUAL(as_int(lists->get(0).get(0)), 1);
  CHECK_EQUAL(as_string(lists->get(0).get(1)), "old-0");
  REQUIRE_EQUAL(lists->get(1).length(), 2);
  CHECK_EQUAL(as_string(lists->get(1).get(0)), "new-1");
  CHECK_EQUAL(as_int(lists->get(1).get(1)), 20);
}

TEST("array merge rebuilds lists with shifted row boundaries") {
  auto old_builder = ArrayBuilder<List>{};
  auto old_0 = old_builder.list();
  old_0.data(std::int64_t{1});
  old_0.data(std::string_view{"old-0"});
  old_builder.list().data(std::int64_t{2});
  auto new_builder = ArrayBuilder<List>{};
  new_builder.list().data(std::int64_t{10});
  auto new_1 = new_builder.list();
  new_1.data(std::string_view{"new-1"});
  new_1.data(std::int64_t{20});
  auto old = MaskedArray<Array<Data>>{Array<Data>{old_builder.finish()},
                                      storage::BitMap{2, true}};
  auto new_ = MaskedArray<Array<Data>>{Array<Data>{new_builder.finish()},
                                       bitmap({false, true})};

  auto merged = with_merged(old, new_);
  auto lists = merged.try_as<List>();
  REQUIRE(lists);
  REQUIRE_EQUAL(lists->get(0).length(), 2);
  CHECK_EQUAL(as_int(lists->get(0).get(0)), 1);
  CHECK_EQUAL(as_string(lists->get(0).get(1)), "old-0");
  REQUIRE_EQUAL(lists->get(1).length(), 2);
  CHECK_EQUAL(as_string(lists->get(1).get(0)), "new-1");
  CHECK_EQUAL(as_int(lists->get(1).get(1)), 20);
}

TEST("array merge preserves absent rows and identity selections") {
  auto old_source = ArrayBuilder<Record>{};
  old_source.record().field("x").data(std::int64_t{1});
  old_source.record().field("x").data(std::int64_t{2});
  old_source.record().field("x").data(std::int64_t{3});
  auto old_x = *old_source.finish().field("x");
  old_x.present = bitmap({true, false, true});
  auto new_source = ArrayBuilder<Record>{};
  new_source.record().field("x").data(std::string_view{"a"});
  new_source.record().field("x").data(std::string_view{"b"});
  new_source.record().field("x").data(std::string_view{"c"});
  auto new_x = *new_source.finish().field("x");
  new_x.present = bitmap({false, false, true});

  auto merged = with_merged(old_x, new_x);
  auto const* alternatives = tenzir::try_as<UnionArray>(merged);
  REQUIRE(alternatives);
  auto ints = alternatives->get_alternative<Int>();
  REQUIRE(ints.is_some());
  CHECK(ints->present.get(0));
  CHECK(not ints->present.get(1));
  CHECK(not ints->present.get(2));
  auto strings = alternatives->get_alternative<String>();
  REQUIRE(strings.is_some());
  CHECK(not strings->present.get(0));
  CHECK(not strings->present.get(1));
  CHECK(strings->present.get(2));

  auto all_old = new_x;
  all_old.present = storage::BitMap{3, false};
  auto old_identity = with_merged(old_x, all_old);
  REQUIRE(old_identity.try_as<Int>().is_some());
  auto all_new = new_x;
  all_new.present = storage::BitMap{3, true};
  auto new_identity = with_merged(old_x, all_new);
  REQUIRE(new_identity.try_as<String>().is_some());
}

TEST("record array with_field replaces list columns") {
  auto old_lists_builder = ArrayBuilder<List>{};
  auto old_0 = old_lists_builder.list();
  old_0.data(std::int64_t{1});
  old_0.data(std::int64_t{2});
  old_lists_builder.list().data(std::int64_t{3});
  static_cast<void>(old_lists_builder.list());
  auto outer = Array<Record>::make_empty(3).with_field_overwrite(
    "xs", MaskedArray<Array<Data>>{
            Array<Data>{old_lists_builder.finish()},
            storage::BitMap{3, true},
          });
  auto new_lists_builder = ArrayBuilder<List>{};
  new_lists_builder.list().data(std::int64_t{10});
  auto new_1 = new_lists_builder.list();
  new_1.data(std::int64_t{20});
  new_1.data(std::int64_t{21});
  new_lists_builder.list().data(std::int64_t{30});
  auto updated = outer.with_field_overwrite(
    "xs", MaskedArray<Array<Data>>{
            Array<Data>{new_lists_builder.finish()},
            bitmap({false, true, false}),
          });

  auto xs_field = updated.field("xs");
  REQUIRE(xs_field.is_some());
  auto xs = xs_field->data.try_as<List>();
  REQUIRE(xs);
  CHECK(not xs_field->present.get(0));
  CHECK(xs_field->present.get(1));
  CHECK(not xs_field->present.get(2));
  CHECK_EQUAL(xs->get(0).length(), 1);
  CHECK_EQUAL(as_int(xs->get(0).get(0)), 10);
  CHECK_EQUAL(xs->get(1).length(), 2);
  CHECK_EQUAL(as_int(xs->get(1).get(0)), 20);
  CHECK_EQUAL(as_int(xs->get(1).get(1)), 21);
  CHECK_EQUAL(xs->get(2).length(), 1);
  CHECK_EQUAL(as_int(xs->get(2).get(0)), 30);
}

TEST("record array with_field replaces records inside lists") {
  auto old_lists_builder = ArrayBuilder<List>{};
  old_lists_builder.list().record().field("old").data(std::int64_t{1});
  old_lists_builder.list().record().field("old").data(std::int64_t{2});
  auto outer = Array<Record>::make_empty(2).with_field_overwrite(
    "xs", MaskedArray<Array<Data>>{
            Array<Data>{old_lists_builder.finish()},
            storage::BitMap{2, true},
          });
  auto new_lists_builder = ArrayBuilder<List>{};
  new_lists_builder.list().record().field("new").data(std::int64_t{10});
  new_lists_builder.list().record().field("new").data(std::int64_t{20});
  auto updated = outer.with_field_overwrite(
    "xs", MaskedArray<Array<Data>>{
            Array<Data>{new_lists_builder.finish()},
            bitmap({false, true}),
          });

  auto xs_field = updated.field("xs");
  REQUIRE(xs_field.is_some());
  auto xs = xs_field->data.try_as<List>();
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->get(0).length(), 1);
  match(
    xs->get(0).get(0),
    [](RowView<Record> row) {
      CHECK_EQUAL(record_field_names(row), (std::vector<std::string>{"new"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
  REQUIRE_EQUAL(xs->get(1).length(), 1);
  match(
    xs->get(1).get(0),
    [](RowView<Record> row) {
      CHECK_EQUAL(record_field_names(row), (std::vector<std::string>{"new"}));
    },
    [](auto) {
      FAIL("expected a record row");
    });
}

TEST("record array with_field replaces the field for an empty update") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::int64_t{2});
  auto outer = builder.finish();
  auto replacement_builder = ArrayBuilder<Record>{};
  replacement_builder.record().field("x").data(std::int64_t{10});
  replacement_builder.record().field("x").data(std::int64_t{20});
  auto replacement = *replacement_builder.finish().field("x");
  replacement.present = storage::BitMap{2, false};
  auto updated = outer.with_field_overwrite("x", std::move(replacement));
  auto x = updated.field("x");
  REQUIRE(x.is_some());
  CHECK(not x->present.get(0));
  CHECK(not x->present.get(1));
  auto values = x->data.try_as<Int>();
  REQUIRE(values.is_some());
  CHECK_EQUAL(*values->get(0), 10);
  CHECK_EQUAL(*values->get(1), 20);
}

TEST("record array without_fields removes a top-level field") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto row = builder.record();
  row.field("x").data(std::int64_t{2});
  row.field("y").data(std::int64_t{3});
  auto arr = builder.finish();

  auto names = std::array<std::string_view, 1>{"x"};
  auto updated = arr.without_fields(names, storage::BitMap{arr.length(), true});
  CHECK_EQUAL(updated.length(), 2);

  auto row1_names = record_field_names(updated.get(1));
  REQUIRE_EQUAL(row1_names.size(), 1u);
  CHECK_EQUAL(row1_names[0], "y");

  CHECK(not updated.field("x"));

  auto y = updated.field("y");
  CHECK(y->present.get(1));
  auto y_ints = y->data.try_as<Int>();
  REQUIRE(y_ints.is_some());
  CHECK_EQUAL(*y_ints->get(1), 3);
}

TEST("record array without_fields ignores unknown names") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto arr = builder.finish();

  auto names = std::array<std::string_view, 1>{"nope"};
  auto updated = arr.without_fields(names, storage::BitMap{arr.length(), true});
  CHECK_EQUAL(updated.length(), 1);
  auto updated_names = record_field_names(updated.get(0));
  REQUIRE_EQUAL(updated_names.size(), 1u);
  CHECK_EQUAL(updated_names[0], "x");
  auto alias = arr;
  auto moved = std::move(alias).without_fields(names, storage::BitMap{1, true});
  CHECK(&as<storage::RecordStorage>(moved.storage()).data()
        == &as<storage::RecordStorage>(arr.storage()).data());
}

TEST("record array without_fields preserves heterogeneous shapes") {
  auto builder = ArrayBuilder<Record>{};
  auto row0 = builder.record();
  row0.field("a").data(std::int64_t{1});
  row0.field("z").data(std::int64_t{10});
  auto row1 = builder.record();
  row1.field("b").data(std::int64_t{2});
  row1.field("z").data(std::int64_t{20});
  auto arr = builder.finish();

  auto names = std::array<std::string_view, 1>{"z"};
  auto updated = arr.without_fields(names, storage::BitMap{arr.length(), true});
  CHECK_EQUAL(updated.length(), 2);

  auto row0_names = record_field_names(updated.get(0));
  REQUIRE_EQUAL(row0_names.size(), 1u);
  CHECK_EQUAL(row0_names[0], "a");

  auto row1_names = record_field_names(updated.get(1));
  REQUIRE_EQUAL(row1_names.size(), 1u);
  CHECK_EQUAL(row1_names[0], "b");
}

TEST("record array without_fields drops a field present on only some rows") {
  auto builder = ArrayBuilder<Record>{};
  auto row0 = builder.record();
  row0.field("a").data(std::int64_t{1});
  row0.field("b").data(std::int64_t{2});
  auto row1 = builder.record();
  row1.field("a").data(std::int64_t{3});
  auto arr = builder.finish();

  auto names = std::array<std::string_view, 1>{"b"};
  auto updated = arr.without_fields(names, storage::BitMap{arr.length(), true});
  CHECK_EQUAL(updated.length(), 2);

  auto row0_names = record_field_names(updated.get(0));
  REQUIRE_EQUAL(row0_names.size(), 1u);
  CHECK_EQUAL(row0_names[0], "a");

  auto row1_names = record_field_names(updated.get(1));
  REQUIRE_EQUAL(row1_names.size(), 1u);
  CHECK_EQUAL(row1_names[0], "a");
}

TEST("record array without_fields removes multiple fields at once") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").data(std::int64_t{1});
  row.field("b").data(std::int64_t{2});
  row.field("c").data(std::int64_t{3});
  auto arr = builder.finish();

  auto names = std::array<std::string_view, 2>{"a", "c"};
  auto updated = arr.without_fields(names, storage::BitMap{arr.length(), true});
  CHECK_EQUAL(updated.length(), 1);

  auto updated_names = record_field_names(updated.get(0));
  REQUIRE_EQUAL(updated_names.size(), 1u);
  CHECK_EQUAL(updated_names[0], "b");

  CHECK(not updated.field("a"));
  CHECK(not updated.field("c"));
}

TEST("record array without_fields composes with with_field") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto arr = builder.finish();

  auto dropped_names = std::array<std::string_view, 1>{"x"};
  auto dropped
    = arr.without_fields(dropped_names, storage::BitMap{arr.length(), true});

  auto replacement = ArrayBuilder<Record>{};
  replacement.record().field("x").data(std::int64_t{99});
  auto new_x = *replacement.finish().field("x");

  auto updated = dropped.with_field_overwrite("x", new_x);
  auto updated_names = record_field_names(updated.get(0));
  REQUIRE_EQUAL(updated_names.size(), 1u);
  CHECK_EQUAL(updated_names[0], "x");

  auto x = updated.field("x");
  CHECK(x->present.get(0));
  auto x_ints = x->data.try_as<Int>();
  REQUIRE(x_ints.is_some());
  CHECK_EQUAL(*x_ints->get(0), 99);
}

TEST("record array without_fields on the empty array") {
  auto arr = Array<Record>::make_empty(3);
  auto names = std::array<std::string_view, 1>{"x"};
  auto updated = arr.without_fields(names, storage::BitMap{arr.length(), true});
  CHECK_EQUAL(updated.length(), 3);
  for (auto i = storage::Index{0}; i < 3; ++i) {
    auto names_at_row = record_field_names(updated.get(i));
    CHECK_EQUAL(names_at_row.size(), 0u);
  }
}

TEST("record array empty factory produces zero fields at requested length") {
  auto arr = Array<Record>::make_empty(3);
  CHECK_EQUAL(arr.length(), 3);
  for (auto i = storage::Index{0}; i < 3; ++i) {
    auto names = record_field_names(arr.get(i));
    CHECK_EQUAL(names.size(), 0u);
  }
}

TEST("record array empty composes with with_field to build fields") {
  auto arr = Array<Record>::make_empty(2);
  auto source = ArrayBuilder<Record>{};
  source.record().field("x").data(std::int64_t{1});
  source.record().field("x").data(std::int64_t{2});
  auto x_column = *source.finish().field("x");
  auto updated = arr.with_field_overwrite("x", x_column);
  CHECK_EQUAL(updated.length(), 2);
  auto names0 = record_field_names(updated.get(0));
  REQUIRE_EQUAL(names0.size(), 1u);
  CHECK_EQUAL(names0[0], "x");
}

TEST("data array builder preserves earlier values once an alternative is "
     "later backfilled with none") {
  // Regression test: `x`'s first value (`1`) is written before this
  // alternative's builder ever needs an offset array (no `.none()` has
  // happened yet). Switching to a different alternative for the second row
  // then calls `.none()` on `x`'s builder for the first time, which must
  // retroactively record row 0's offset -- losing it would silently corrupt
  // `x`'s reported value or crash on read-back.
  auto builder = ArrayBuilder<Data>{};
  builder.data(std::int64_t{1});
  builder.data(2.5);
  auto arr = builder.finish();
  CHECK_EQUAL(arr.length(), 2);
  match(
    arr,
    [](const UnionArray& u) {
      auto int_alt = u.get_alternative<Int>();
      REQUIRE(int_alt.is_some());
      CHECK(int_alt->present.get(0));
      CHECK(not int_alt->present.get(1));
      CHECK_EQUAL(int_alt->data.length(), 2);
      CHECK_EQUAL(*int_alt->data.get(0), 1);
    },
    [](const auto&) {
      REQUIRE(false);
    });
}

namespace {

template <class T>
concept has_list_layout = requires(T const& array) {
  array.values();
  array.spans();
};
static_assert(not has_list_layout<Array<List>>);
static_assert(has_list_layout<storage::ListStorage>);

template <structured_type T>
auto constant_array(storage::Index length, T value) -> Array<T> {
  return Array<T>{
    storage::ConstantStorage<T, RowView<T>>{length, std::move(value)}};
}

} // namespace

TEST("record array empty_where fills rows the record alternative skipped") {
  // Regression test: a `UnionArray`'s `Record` alternative spans every row,
  // but the rows outside its `present` mask carry the absent shape (-1).
  // Taking the alternative and dropping `present` left those rows unreadable:
  // `Array<Record>::get` asserts on a valid shape index.
  auto builder = UnionArrayBuilder{};
  builder.record().field("x").data(std::int64_t{1});
  builder.data(std::int64_t{2});
  builder.record().field("y").data(std::int64_t{3});
  auto values = builder.finish();
  auto alt = values.get_alternative<Record>();
  REQUIRE(alt.is_some());
  CHECK(not alt->present.get(1));
  auto absent = alt->present.make_inverted();
  auto records = std::move(alt->data).empty_where(std::move(absent));
  REQUIRE_EQUAL(records.length(), 3);

  auto row0 = record_field_names(records.get(0));
  REQUIRE_EQUAL(row0.size(), 1u);
  CHECK_EQUAL(row0[0], "x");

  CHECK_EQUAL(record_field_names(records.get(1)).size(), 0u);

  auto row2 = record_field_names(records.get(2));
  REQUIRE_EQUAL(row2.size(), 1u);
  CHECK_EQUAL(row2[0], "y");
}

TEST("record array empty_where empties selected rows and keeps the others") {
  auto builder = ArrayBuilder<Record>{};
  auto row0 = builder.record();
  row0.field("a").data(std::int64_t{1});
  row0.field("b").data(std::int64_t{2});
  builder.record().field("a").data(std::int64_t{3});
  auto arr = builder.finish();

  auto updated = arr.empty_where(bitmap({true, false}));
  CHECK_EQUAL(updated.length(), 2);
  CHECK_EQUAL(record_field_names(updated.get(0)).size(), 0u);

  auto row1 = record_field_names(updated.get(1));
  REQUIRE_EQUAL(row1.size(), 1u);
  CHECK_EQUAL(row1[0], "a");

  // The emptied row must no longer be present in any field column, while the
  // untouched row keeps its value.
  auto a = updated.field("a");
  REQUIRE(a.is_some());
  CHECK(not a->present.get(0));
  CHECK(a->present.get(1));
  auto a_ints = a->data.try_as<Int>();
  REQUIRE(a_ints.is_some());
  CHECK_EQUAL(*a_ints->get(1), 3);

  auto b = updated.field("b");
  REQUIRE(b.is_some());
  CHECK(not b->present.get(0));
}

TEST("record array empty_where handles constant masks") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::int64_t{2});
  auto arr = builder.finish();

  auto none = arr.empty_where(storage::BitMap{arr.length(), false});
  CHECK_EQUAL(none.length(), 2);
  auto kept = record_field_names(none.get(0));
  REQUIRE_EQUAL(kept.size(), 1u);
  CHECK_EQUAL(kept[0], "x");

  auto all = arr.empty_where(storage::BitMap{arr.length(), true});
  CHECK_EQUAL(all.length(), 2);
  for (auto i = storage::Index{0}; i < all.length(); ++i) {
    CHECK_EQUAL(record_field_names(all.get(i)).size(), 0u);
  }
  CHECK(not all.field("x"));
}

TEST("record array empty_where expands constant record storage") {
  auto arr = constant_array(2, Record{{"x", Int{1}}});
  auto updated = arr.empty_where(bitmap({true, false}));
  CHECK(is<storage::RecordStorage>(updated.storage()));
  CHECK_EQUAL(record_field_names(updated.get(0)).size(), 0u);
  auto row1 = record_field_names(updated.get(1));
  REQUIRE_EQUAL(row1.size(), 1u);
  CHECK_EQUAL(row1[0], "x");
  // The source is untouched.
  CHECK_EQUAL(record_field_names(arr.get(0)).size(), 1u);
}

TEST("record array empty_where detaches shared storage") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::int64_t{2});
  auto arr = builder.finish();
  auto alias = arr;
  auto updated = std::move(arr).empty_where(bitmap({true, false}));
  CHECK_EQUAL(record_field_names(updated.get(0)).size(), 0u);

  auto alias_names = record_field_names(alias.get(0));
  REQUIRE_EQUAL(alias_names.size(), 1u);
  CHECK_EQUAL(alias_names[0], "x");
  auto alias_x = alias.field("x");
  REQUIRE(alias_x.is_some());
  CHECK(alias_x->present.get(0));
  auto alias_ints = alias_x->data.try_as<Int>();
  REQUIRE(alias_ints.is_some());
  CHECK_EQUAL(*alias_ints->get(0), 1);
}

TEST("structured constants borrow recursive owning values") {
  auto record
    = Record{{"z", List{Int{42}, Null{}, String{"hello"}}}, {"a", Record{}}};
  auto source = constant_array(3, record);
  auto alias = source;
  CHECK(&source.storage() != &alias.storage());
  auto names = std::vector<std::string>{"z", "a"};
  CHECK_EQUAL(record_field_names(source.get(2)), names);
  auto field = source.field("z");
  REQUIRE(field);
  CHECK_EQUAL(field->present.true_count(), 3);
  auto list = field->data.get_alternative<List>();
  REQUIRE(list);
  CHECK(
    (is<storage::ConstantStorage<List, RowView<List>>>(list->data.storage())));
  CHECK_EQUAL(list->data.get(1).length(), 3);
  CHECK(equal(list->data.get(1).get(0), RowView<Data>{RowView<Int>{42}}));
  auto iterator = source.get(0).begin();
  CHECK_EQUAL((*iterator).first, "z");
  ++iterator;
  CHECK_EQUAL((*iterator).first, "a");
  auto primary = source.to_primary();
  CHECK(is<storage::RecordStorage>(primary.storage()));
  for (auto i = storage::Index{0}; i < source.length(); ++i) {
    CHECK(equal(RowView<Data>{source.get(i)}, RowView<Data>{primary.get(i)}));
  }
  record.emplace("later", Bool{true});
  CHECK(not source.field("later"));
  auto detached = source.as_unique();
  CHECK(&source.storage() != &detached.storage());
  CHECK(equal(RowView<Data>{source.get(0)}, RowView<Data>{detached.get(0)}));
  auto representation = std::move(alias).storage();
  CHECK_EQUAL(source.length(), 3);
  CHECK_EQUAL(match(representation,
                    [](auto const& physical) {
                      return physical.length();
                    }),
              3);
}

TEST("constant lists preserve nested views through erasure and masks") {
  auto value = List{String{"borrowed"}, Record{{"null", Null{}}}, List{},
                    Blob{std::byte{1}, std::byte{2}}};
  auto source = constant_array(3, value);
  auto const& physical
    = as<storage::ConstantStorage<List, RowView<List>>>(source.storage());
  auto row = source.get(2);
  auto string = as<RowView<std::string_view>>(row.get(0));
  CHECK_EQUAL((*string).data(), as<String>(physical.value()[0]).data());
  auto it = source.get(1).begin();
  CHECK(equal(*it, row.get(0)));
  auto erased = ErasedArray{source};
  CHECK(equal(erased.get(2), RowView<Data>{row}));
  auto data = Array<Data>{source};
  auto primary = source.to_primary();
  CHECK(equal(data.get(1), RowView<Data>{primary.get(1)}));
  auto builder = ArrayBuilder<Data>{};
  append_row(builder, data.get(0));
  CHECK(equal(builder.finish().get(0), data.get(0)));
  auto mask = storage::BitMap::Mutable{3};
  mask.set(1, true);
  auto nullable = data.null_where(std::move(mask).finish());
  CHECK(equal(nullable.get(0), data.get(0)));
  CHECK(is<RowView<Null>>(nullable.get(1)));
  CHECK(equal(nullable.get(2), data.get(2)));
}

TEST("empty structured constants and zero row field lookup") {
  for (auto length :
       {storage::Index{0}, storage::Index{1}, storage::Index{3}}) {
    auto lists = constant_array(length, List{});
    auto records = constant_array(length, Record{{"null", Null{}}});
    CHECK_EQUAL(lists.to_primary().length(), length);
    CHECK_EQUAL(records.to_primary().length(), length);
    auto field = records.field("null");
    REQUIRE(field);
    CHECK_EQUAL(field->data.length(), length);
    CHECK_EQUAL(field->present.true_count(), length);
    CHECK(not records.field("missing"));
    if (length > 0) {
      CHECK(lists.get(0).begin() == lists.get(0).end());
      CHECK(is<RowView<Null>>(field->data.get(0)));
    }
  }
}

TEST("constant record updates materialize and detach") {
  auto source = constant_array(3, Record{{"z", Int{1}}, {"a", Null{}}});
  auto alias = source;
  auto mask = storage::BitMap::Mutable{3};
  mask.set(1, true);
  auto updated = source.with_fields(
    {{"z", {repeat(Data{Int{7}}, 3), std::move(mask).finish()}}});
  CHECK(is<storage::RecordStorage>(updated.storage()));
  CHECK_EQUAL(*as<RowView<Int>>(updated.field("z")->data.get(0)), 1);
  CHECK_EQUAL(*as<RowView<Int>>(updated.field("z")->data.get(1)), 7);
  CHECK_EQUAL(*as<RowView<Int>>(alias.field("z")->data.get(1)), 1);
  auto extracted = std::move(source).dangerously_extract_field("z");
  REQUIRE(extracted);
  CHECK_EQUAL(*as<RowView<Int>>(extracted->data.get(2)), 1);
  auto names = std::array<std::string_view, 1>{"z"};
  auto removed = alias.without_fields(names, storage::BitMap{3, true});
  CHECK_EQUAL(record_field_names(removed.get(1)),
              std::vector<std::string>{"a"});
  CHECK(alias.field("z"));
}

TEST("structured merges accept every physical representation pair") {
  auto check = []<structured_type T>(T old_value, T new_value) {
    for (auto old_primary : {false, true}) {
      for (auto new_primary : {false, true}) {
        auto old = constant_array(3, old_value);
        auto replacement = constant_array(3, new_value);
        if (old_primary) {
          old = old.to_primary();
        }
        if (new_primary) {
          replacement = replacement.to_primary();
        }
        auto mask = storage::BitMap::Mutable{3};
        mask.set(1, true);
        auto result
          = with_merged({Array<Data>{old}, storage::BitMap{3, true}},
                        {Array<Data>{replacement}, std::move(mask).finish()});
        CHECK(equal(result.get(0), RowView<Data>{old.get(0)}));
        CHECK(equal(result.get(1), RowView<Data>{replacement.get(1)}));
        CHECK(equal(result.get(2), RowView<Data>{old.get(2)}));
      }
    }
  };
  check(List{Int{1}}, List{Record{{"nested", List{}}}, Null{}});
  check(Record{{"z", List{Int{1}}}},
        Record{{"a", Null{}}, {"z", List{Int{2}}}});
}

TEST("constant record removal preserves unselected rows") {
  auto source = constant_array(3, Record{{"z", Int{1}}, {"a", Null{}}});
  auto mask = storage::BitMap::Mutable{3};
  mask.set(1, true);
  auto names = std::array<std::string_view, 1>{"z"};
  auto removed = source.without_fields(names, std::move(mask).finish());
  auto original_names = std::vector<std::string>{"z", "a"};
  CHECK_EQUAL(record_field_names(removed.get(0)), original_names);
  CHECK_EQUAL(record_field_names(removed.get(1)),
              std::vector<std::string>{"a"});
  CHECK_EQUAL(record_field_names(removed.get(2)), original_names);
  REQUIRE(removed.field("z"));
  CHECK(not removed.field("z")->present.get(1));
  CHECK(removed.field("z")->present.get(2));
  CHECK_EQUAL(record_field_names(source.get(1)), original_names);
}

TEST("constant structured materialization and formatting match primary "
     "storage") {
  auto source = constant_array(2, Record{{"z", List{Int{1}, Null{}, Record{}}},
                                         {"a", String{"text"}}});
  auto primary = source.to_primary();
  CHECK(materialize(RowView<Data>{source.get(1)})
        == materialize(RowView<Data>{primary.get(1)}));
  auto constant_strings
    = stringify(Array<Data>{source}, storage::BitMap{2, true});
  auto primary_strings
    = stringify(Array<Data>{primary}, storage::BitMap{2, true});
  CHECK_EQUAL(*constant_strings.get(0), *primary_strings.get(0));
  CHECK_EQUAL(*constant_strings.get(1), *primary_strings.get(1));
}

TEST("constant record overwrite accepts a borrowed field name") {
  auto name = std::string(100, 'x');
  auto source = constant_array(2, Record{{name, Int{1}}});
  auto borrowed_name = (*source.get(0).begin()).first;
  auto updated = std::move(source).with_field_overwrite(
    borrowed_name, {repeat(Data{Int{7}}, 2), storage::BitMap{2, true}});
  auto field = updated.field(name);
  REQUIRE(field);
  CHECK_EQUAL(*as<RowView<Int>>(field->data.get(0)), 7);
  CHECK_EQUAL(*as<RowView<Int>>(field->data.get(1)), 7);
}

static_assert(not std::default_initializable<RowView<List>>);
static_assert(not std::default_initializable<RowView<Record>>);
static_assert(not std::default_initializable<RowView<List>::iterator>);
static_assert(not std::default_initializable<RowView<Record>::iterator>);
static_assert(std::is_nothrow_copy_assignable_v<RowView<List>>);
static_assert(std::is_nothrow_copy_assignable_v<RowView<Record>>);
static_assert(std::is_nothrow_move_assignable_v<RowView<List>>);
static_assert(std::is_nothrow_move_assignable_v<RowView<Record>>);

TEST("structured view variants preserve source identity across assignment") {
  auto check = []<structured_type T>(T value) {
    auto constant = constant_array(2, value);
    auto primary = constant.to_primary();
    auto other = constant_array(2, value);
    auto view = constant.get(0);
    auto saved = view.begin();
    CHECK(saved == view.begin());
    CHECK(saved != other.get(0).begin());
    CHECK(saved != primary.get(0).begin());
    view = primary.get(1);
    CHECK(view.begin() == primary.get(1).begin());
    CHECK(view.begin() != primary.get(0).begin());
    CHECK(saved == constant.get(0).begin());
    CHECK(equal(RowView<Data>{view}, RowView<Data>{constant.get(0)}));
    auto iterator = view.begin();
    iterator = saved;
    CHECK(iterator == constant.get(0).begin());
    view = constant.get(1);
    CHECK(view.begin() == saved);
    ++saved;
    CHECK(saved == constant.get(0).end());
  };
  check(List{Int{1}});
  check(Record{{"field", Int{1}}});
}

static_assert(not ErasedArrayAlternatives::contains<Array<List>>);
static_assert(not ErasedArrayAlternatives::contains<Array<Record>>);
static_assert(ErasedArrayAlternatives::contains<storage::ListStorage>);
static_assert(ErasedArrayAlternatives::contains<storage::RecordStorage>);
static_assert(ErasedArrayAlternatives::contains<
              storage::ConstantStorage<List, RowView<List>>>);
static_assert(ErasedArrayAlternatives::contains<
              storage::ConstantStorage<Record, RowView<Record>>>);

TEST("flattened structured erasure owns physical storage and borrows rows") {
  auto check = []<structured_type T>(T value) {
    for (auto primary : {false, true}) {
      auto source = constant_array(2, value);
      if (primary) {
        source = source.to_primary();
      }
      auto erased = match(std::move(source).storage(), [](auto physical) {
        return ErasedArray{std::move(physical)};
      });
      auto row = tenzir::as<RowView<T>>(erased.get(1));
      auto iterator = row.begin();
      {
        auto logical = tenzir::variant_traits<ErasedArray>::get<
          ErasedDataAlternatives::unique_index_of<Array<T>>>(erased);
        static_assert(
          std::same_as<
            decltype(tenzir::variant_traits<ErasedArray>::get<
                     ErasedDataAlternatives::unique_index_of<Array<T>>>(erased)),
            Array<T>>);
        CHECK(equal(RowView<Data>{logical.get(1)}, RowView<Data>{row}));
      }
      CHECK(iterator == tenzir::as<RowView<T>>(erased.get(1)).begin());
      CHECK(equal(RowView<Data>{row}, RowView<Data>{value}));
      auto data = Array<Data>{std::move(erased)};
      auto data_row = tenzir::as<RowView<T>>(data.get(0));
      auto extracted = data.try_as<T>();
      REQUIRE(extracted);
      CHECK(equal(RowView<Data>{extracted->get(0)}, RowView<Data>{data_row}));
      // Logical extraction returns an independent wrapper. Replacing it must
      // leave views borrowing the erased representation intact.
      extracted = constant_array(2, T{});
      CHECK(equal(RowView<Data>{data_row}, RowView<Data>{value}));
      CHECK(data_row.begin() == tenzir::as<RowView<T>>(data.get(0)).begin());
    }
  };
  check(List{String{std::string(100, 'x')}, Record{{"nested", Null{}}}});
  check(
    Record{{"list", List{String{std::string(100, 'y')}}}, {"null", Null{}}});
}

static_assert([]<class... Ts>(TypeList<Ts...>) {
  return (
    std::is_rvalue_reference_v<decltype(std::declval<Array<Ts>&&>().storage())>
    and ...);
}(data_type_list{}));

TEST("rvalue array storage exposes its own variant") {
  auto check = []<structured_type T>(T value) {
    auto source = constant_array(2, value);
    auto copy = source;
    auto&& physical = std::move(copy).storage();
    CHECK(&physical == &copy.storage());
    CHECK(&physical != &source.storage());
    CHECK(equal(RowView<Data>{source.get(0)}, RowView<Data>{copy.get(0)}));
    auto extracted = Array<T>{
      tenzir::as<storage::ConstantStorage<T, RowView<T>>>(std::move(physical))};
    CHECK(equal(RowView<Data>{source.get(0)}, RowView<Data>{extracted.get(0)}));
  };
  check(List{Int{1}});
  check(Record{{"value", Int{1}}});
}

TEST("physical structured storage shares through erasure and logical "
     "dispatch") {
  auto check = []<structured_type T>(T value) {
    using Physical = typename Type<T>::PrimaryPhysicalStorage;
    auto source = constant_array(2, value).to_primary();
    auto const* backing
      = std::addressof(tenzir::as<Physical>(source.storage()).data());
    auto erased = ErasedArray{source};
    auto data = Array<Data>{source};
    auto copy = data;
    auto extracted = copy.try_as<T>();
    REQUIRE(extracted);
    CHECK(std::addressof(tenzir::as<Physical>(extracted->storage()).data())
          == backing);
    auto view = tenzir::as<RowView<T>>(data.get(0));
    CHECK(view.begin() == source.get(0).begin());
    CHECK(tenzir::as<RowView<T>>(erased.get(0)).begin() == view.begin());
    auto unique = std::move(*extracted).as_unique();
    CHECK(std::addressof(tenzir::as<Physical>(unique.storage()).data())
          != backing);
    CHECK(equal(RowView<Data>{unique.get(0)}, RowView<Data>{view}));
    // Moving the array and erasing it must preserve the backing a row borrows.
    auto moved = Array<Data>{std::move(source)};
    CHECK(view.begin() == tenzir::as<RowView<T>>(moved.get(0)).begin());
    CHECK(equal(RowView<Data>{view}, RowView<Data>{value}));
  };
  check(List{Record{{"nested", List{Int{42}}}}});
  check(Record{{"nested", Record{{"value", Int{42}}}}, {"list", List{}}});
}

TEST("physical record mutation detaches and retains nested column sharing") {
  auto source = constant_array(2, Record{{"nested", Record{{"value", Int{42}}}},
                                         {"old", Int{1}}})
                  .to_primary();
  auto nested = source.field("nested")->data.try_as<Record>();
  REQUIRE(nested);
  auto const* nested_backing
    = &tenzir::as<storage::RecordStorage>(nested->storage()).data();
  auto erased = Array<Data>{source};
  auto copy = erased.try_as<Record>();
  REQUIRE(copy);
  auto changed = std::move(*copy).with_field_overwrite(
    "old", {repeat(Data{Int{2}}, 2), storage::BitMap{2, true}});
  CHECK_EQUAL(*tenzir::as<RowView<Int>>(source.field("old")->data.get(0)), 1);
  CHECK_EQUAL(*tenzir::as<RowView<Int>>(changed.field("old")->data.get(0)), 2);
  auto changed_nested = changed.field("nested")->data.try_as<Record>();
  REQUIRE(changed_nested);
  CHECK(&tenzir::as<storage::RecordStorage>(changed_nested->storage()).data()
        == nested_backing);
  auto physical = tenzir::as<storage::RecordStorage>(source.storage());
  auto&& detached = std::move(physical).data();
  CHECK(&detached
        != &tenzir::as<storage::RecordStorage>(source.storage()).data());
  CHECK(&detached == &physical.data());
}

TEST("consuming list values detaches physical backing") {
  auto source
    = constant_array(2, List{Record{{"value", Int{42}}}}).to_primary();
  auto physical = tenzir::as<storage::ListStorage>(source.storage());
  auto const* original = &physical.values();
  auto values = std::move(physical).values();
  CHECK(&tenzir::as<storage::ListStorage>(source.storage()).values()
        == original);
  CHECK_EQUAL(values.length(), 2);
  CHECK(equal(values.get(0), source.get(0).get(0)));
}

namespace {

/// A three-row record whose field `nested` is a union: `{a: 1}`, `7`, `{a: 2}`.
auto make_union_nested_record() -> Array<Record> {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("nested").record().field("a").data(std::int64_t{1});
  builder.record().field("nested").data(std::int64_t{7});
  builder.record().field("nested").record().field("a").data(std::int64_t{2});
  return builder.finish();
}

/// Materializes `outer.nested` on `row`, requiring the field to be present.
auto nested_value(Array<Record> const& outer, storage::Index row)
  -> tenzir::data {
  auto field = outer.field("nested");
  REQUIRE(field.is_some());
  REQUIRE(field->present.get(row));
  return materialize(field->data.get(row));
}

/// Values for a new field `b` on three rows, present where `mask` says so.
auto make_b_values(storage::BitMap mask) -> MaskedArray<Array<Data>> {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("b").data(std::int64_t{10});
  builder.record().field("b").data(std::int64_t{20});
  builder.record().field("b").data(std::int64_t{30});
  auto values = *builder.finish().field("b");
  values.present = std::move(mask);
  return values;
}

} // namespace

TEST("nested field assignment through a union parent replaces non-record "
     "rows with an implicit record") {
  // The `Int` row has no record to descend into and becomes `{b: 20}`.
  auto outer = make_union_nested_record();
  auto path = make_field_path({"nested", "b"});

  auto updated = assign_nested_field(std::move(outer), path.path(),
                                     make_b_values(storage::BitMap{3, true}));

  CHECK_EQUAL(nested_value(updated, 0),
              (tenzir::data{tenzir::record{{"a", std::int64_t{1}},
                                           {"b", std::int64_t{10}}}}));
  CHECK_EQUAL(nested_value(updated, 1),
              (tenzir::data{tenzir::record{{"b", std::int64_t{20}}}}));
  CHECK_EQUAL(nested_value(updated, 2),
              (tenzir::data{tenzir::record{{"a", std::int64_t{2}},
                                           {"b", std::int64_t{30}}}}));
}

TEST("nested field assignment through a union parent keeps rows outside the "
     "mask intact") {
  auto outer = make_union_nested_record();
  auto path = make_field_path({"nested", "b"});

  auto updated = assign_nested_field(
    std::move(outer), path.path(), make_b_values(bitmap({true, false, false})));

  CHECK_EQUAL(nested_value(updated, 0),
              (tenzir::data{tenzir::record{{"a", std::int64_t{1}},
                                           {"b", std::int64_t{10}}}}));
  CHECK_EQUAL(nested_value(updated, 1), (tenzir::data{std::int64_t{7}}));
  CHECK_EQUAL(nested_value(updated, 2),
              (tenzir::data{tenzir::record{{"a", std::int64_t{2}}}}));
}

TEST("nested field assignment keeps a plain record parent present outside "
     "the mask") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("nested").record().field("a").data(std::int64_t{1});
  builder.record().field("nested").record().field("a").data(std::int64_t{2});
  builder.record().field("nested").record().field("a").data(std::int64_t{3});
  auto outer = builder.finish();
  auto path = make_field_path({"nested", "b"});

  auto updated = assign_nested_field(
    std::move(outer), path.path(), make_b_values(bitmap({false, true, false})));

  CHECK_EQUAL(nested_value(updated, 0),
              (tenzir::data{tenzir::record{{"a", std::int64_t{1}}}}));
  CHECK_EQUAL(nested_value(updated, 1),
              (tenzir::data{tenzir::record{{"a", std::int64_t{2}},
                                           {"b", std::int64_t{20}}}}));
  CHECK_EQUAL(nested_value(updated, 2),
              (tenzir::data{tenzir::record{{"a", std::int64_t{3}}}}));
}

TEST("nested drop through a union parent leaves non-record rows untouched") {
  auto outer = make_union_nested_record();
  auto paths = std::vector<tenzir::ast::field_path>{};
  paths.push_back(make_field_path({"nested", "a"}));
  auto tree = DropTree::make(paths);

  auto updated = tree.apply(std::move(outer), storage::BitMap{3, true});

  CHECK_EQUAL(nested_value(updated, 0), (tenzir::data{tenzir::record{}}));
  CHECK_EQUAL(nested_value(updated, 1), (tenzir::data{std::int64_t{7}}));
  CHECK_EQUAL(nested_value(updated, 2), (tenzir::data{tenzir::record{}}));
}

TEST("nested drop through a union parent keeps rows outside the mask intact") {
  auto outer = make_union_nested_record();
  auto paths = std::vector<tenzir::ast::field_path>{};
  paths.push_back(make_field_path({"nested", "a"}));
  auto tree = DropTree::make(paths);

  auto updated = tree.apply(std::move(outer), bitmap({true, false, false}));

  CHECK_EQUAL(nested_value(updated, 0), (tenzir::data{tenzir::record{}}));
  CHECK_EQUAL(nested_value(updated, 1), (tenzir::data{std::int64_t{7}}));
  CHECK_EQUAL(nested_value(updated, 2),
              (tenzir::data{tenzir::record{{"a", std::int64_t{2}}}}));
}

namespace {

/// Materializes field `name` of `row`, requiring it to be present.
auto field_value(Array<Record> const& array, std::string_view name,
                 storage::Index row) -> tenzir::data {
  auto field = array.field(name);
  REQUIRE(field.is_some());
  REQUIRE(field->present.get(row));
  return materialize(field->data.get(row));
}

} // namespace

TEST("take_last pops fundamental builders and keeps earlier rows") {
  auto ints = ArrayBuilder<Int>{};
  ints.data(1);
  ints.data(2);
  CHECK(ints.take_last() == Data{Int{2}});
  ints.data(3);
  auto int_array = ints.finish();
  REQUIRE_EQUAL(int_array.length(), 2);
  CHECK_EQUAL(*int_array.get(1), 3);

  auto strings = ArrayBuilder<String>{};
  strings.data("keep");
  strings.data("drop");
  CHECK(strings.take_last() == Data{String{"drop"}});
  strings.data("new");
  auto string_array = strings.finish();
  REQUIRE_EQUAL(string_array.length(), 2);
  CHECK_EQUAL(*string_array.get(0), "keep");
  CHECK_EQUAL(*string_array.get(1), "new");

  auto bools = ArrayBuilder<Bool>{};
  bools.data(true);
  bools.data(true);
  CHECK(bools.take_last() == Data{true});
  bools.data(false);
  bools.data(true);
  CHECK(bools.take_last() == Data{true});
  CHECK(bools.take_last() == Data{false});
  auto bool_array = bools.finish();
  REQUIRE_EQUAL(bool_array.length(), 1);
  CHECK_EQUAL(*bool_array.get(0), true);

  auto nulls = ArrayBuilder<Null>{};
  nulls.null();
  nulls.null();
  CHECK(nulls.take_last() == Data{Null{}});
  CHECK_EQUAL(nulls.finish().length(), 1);

  auto durations = ArrayBuilder<Duration>{};
  durations.data(std::chrono::seconds{1});
  durations.data(std::chrono::seconds{2});
  CHECK(durations.take_last() == Data{Duration{std::chrono::seconds{2}}});
  CHECK_EQUAL(durations.finish().length(), 1);
}

TEST("take_last pops a union, a list, and a nested record") {
  auto data_builder = ArrayBuilder<Data>{};
  data_builder.data(std::int64_t{1});
  data_builder.data(std::string_view{"two"});
  CHECK(data_builder.take_last() == Data{String{"two"}});
  data_builder.data(2.5);
  auto data_array = data_builder.finish();
  REQUIRE_EQUAL(data_array.length(), 2);
  CHECK_EQUAL(materialize(data_array.get(0)), (tenzir::data{std::int64_t{1}}));
  CHECK_EQUAL(materialize(data_array.get(1)), (tenzir::data{2.5}));

  auto lists = ArrayBuilder<List>{};
  auto first = lists.list();
  first.data(std::int64_t{1});
  auto second = lists.list();
  second.data(std::int64_t{2});
  second.null();
  second.record().field("x").data(std::string_view{"y"});
  auto expected_list = List{};
  expected_list.emplace_back(Int{2});
  expected_list.emplace_back(Null{});
  auto nested = Record{};
  nested.emplace("x", String{"y"});
  expected_list.emplace_back(std::move(nested));
  CHECK(lists.take_last() == Data{std::move(expected_list)});
  auto third = lists.list();
  third.data(std::int64_t{3});
  auto list_array = lists.finish();
  REQUIRE_EQUAL(list_array.length(), 2);
  CHECK_EQUAL(materialize(RowView<Data>{list_array.get(1)}),
              (tenzir::data{tenzir::list{std::int64_t{3}}}));

  auto records = ArrayBuilder<Record>{};
  records.record().field("a").data(std::int64_t{1});
  auto row = records.record();
  row.field("a").data(std::int64_t{2});
  row.field("b").record().field("c").data(true);
  auto expected_record = Record{};
  expected_record.emplace("a", Int{2});
  auto inner = Record{};
  inner.emplace("c", true);
  expected_record.emplace("b", std::move(inner));
  CHECK(records.take_last() == Data{std::move(expected_record)});
  records.record().field("b").data(std::int64_t{3});
  auto record_array = records.finish();
  REQUIRE_EQUAL(record_array.length(), 2);
  CHECK_EQUAL(field_value(record_array, "a", 0),
              (tenzir::data{std::int64_t{1}}));
  CHECK_EQUAL(field_value(record_array, "b", 1),
              (tenzir::data{std::int64_t{3}}));
  auto a = record_array.field("a");
  REQUIRE(a.is_some());
  CHECK(not a->present.get(1));
}

TEST("record builder joins repeated scalar keys into a list") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").data(std::int64_t{1});
  row.field("a").data(std::int64_t{2});
  row.field("a").data(std::string_view{"three"});
  builder.record().field("a").data(std::int64_t{3});
  auto array = builder.finish();
  REQUIRE_EQUAL(array.length(), 2);
  CHECK_EQUAL(field_value(array, "a", 0),
              (tenzir::data{tenzir::list{std::int64_t{1}, std::int64_t{2},
                                         std::string{"three"}}}));
  CHECK_EQUAL(field_value(array, "a", 1), (tenzir::data{std::int64_t{3}}));
  CHECK_EQUAL(record_field_names(array.get(0)).size(), 1u);
}

TEST("record builder merges repeated record keys") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").record().field("x").data(std::int64_t{1});
  auto again = row.field("a").record();
  again.field("x").data(std::int64_t{2});
  again.field("y").data(std::int64_t{3});
  auto array = builder.finish();
  CHECK_EQUAL(field_value(array, "a", 0),
              (tenzir::data{tenzir::record{
                {"x", tenzir::list{std::int64_t{1}, std::int64_t{2}}},
                {"y", std::int64_t{3}},
              }}));
}

TEST("record builder moves a scalar under the empty key when a record "
     "follows") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").data(std::int64_t{1});
  row.field("a").record().field("b").data(std::int64_t{2});
  auto array = builder.finish();
  CHECK_EQUAL(field_value(array, "a", 0), (tenzir::data{tenzir::record{
                                            {"", std::int64_t{1}},
                                            {"b", std::int64_t{2}},
                                          }}));
}

TEST("record builder adds a scalar under the empty key of an existing record") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").record().field("b").data(std::int64_t{2});
  row.field("a").data(std::int64_t{1});
  auto array = builder.finish();
  CHECK_EQUAL(field_value(array, "a", 0), (tenzir::data{tenzir::record{
                                            {"b", std::int64_t{2}},
                                            {"", std::int64_t{1}},
                                          }}));
}

TEST("record builder concatenates repeated lists and nests others") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").list().data(std::int64_t{1});
  auto more = row.field("a").list();
  more.data(std::int64_t{2});
  more.data(std::int64_t{3});
  row.field("b").data(std::int64_t{1});
  row.field("b").list().data(std::int64_t{2});
  row.field("c").list().data(std::int64_t{1});
  row.field("c").null();
  auto array = builder.finish();
  CHECK_EQUAL(field_value(array, "a", 0),
              (tenzir::data{tenzir::list{std::int64_t{1}, std::int64_t{2},
                                         std::int64_t{3}}}));
  CHECK_EQUAL(field_value(array, "b", 0),
              (tenzir::data{
                tenzir::list{std::int64_t{1}, tenzir::list{std::int64_t{2}}}}));
  CHECK_EQUAL(field_value(array, "c", 0),
              (tenzir::data{tenzir::list{std::int64_t{1}, caf::none}}));
}

TEST("record builder replaces a null on a repeated key") {
  auto builder = ArrayBuilder<Record>{};
  auto row = builder.record();
  row.field("a").null();
  row.field("a").data(std::int64_t{1});
  row.field("b").data(std::int64_t{1});
  row.field("b").null();
  auto array = builder.finish();
  CHECK_EQUAL(field_value(array, "a", 0), (tenzir::data{std::int64_t{1}}));
  CHECK_EQUAL(field_value(array, "b", 0),
              (tenzir::data{tenzir::list{std::int64_t{1}, caf::none}}));
}
