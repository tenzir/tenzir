//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/data.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

using namespace tenzir;

namespace {

auto make_input_slice(data row) -> table_slice {
  auto row_series = data_to_series(row, int64_t{1});
  auto schema = type{"input", as<record_type>(row_series.type)};
  return table_slice{
    record_batch_from_struct_array(schema.to_arrow_schema(),
                                   as<arrow::StructArray>(*row_series.array)),
    std::move(schema),
  };
}

auto make_sliced_list_input_slice() -> table_slice {
  auto schema = type{record_type{
    {"xs", list_type{record_type{{"b", int64_type{}}, {"a", int64_type{}}}}},
  }};
  auto builder = series_builder{schema};
  {
    auto row = builder.record();
    auto xs = row.field("xs").list();
    auto item = xs.record();
    item.field("b").data(int64_t{0});
    item.field("a").data(int64_t{0});
  }
  {
    auto row = builder.record();
    auto xs = row.field("xs").list();
    auto item = xs.record();
    item.field("b").data(int64_t{1});
    item.field("a").data(int64_t{2});
  }
  builder.record().field("xs").data(caf::none);
  auto slices = builder.finish_as_table_slice("input");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto result = subslice(slices[0], 1, 3);
  auto batch = to_record_batch(result);
  auto& xs = as<arrow::ListArray>(*batch->column(0));
  REQUIRE_NOT_EQUAL(xs.offset(), int64_t{0});
  REQUIRE_NOT_EQUAL(xs.value_offset(0), int64_t{0});
  REQUIRE(xs.null_bitmap());
  return result;
}

} // namespace

TEST("sort cmp deduplicates diagnostics from comparator evaluation") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr = parse_expression_with_bad_diagnostics(
    "sort(xs, cmp=(l, r) => missing)", ctx);
  REQUIRE(expr);
  auto resolved = resolve_entities(*expr, ctx);
  REQUIRE(resolved);
  auto row = data{
    record{
      {"xs", list{int64_t{3}, int64_t{1}, int64_t{2}}},
    },
  };
  auto result = eval(*expr, make_input_slice(std::move(row)), dh);
  REQUIRE_EQUAL(result.length(), int64_t{1});
  auto value = materialize(result.view3_at(0));
  auto* sorted = try_as<list>(&value);
  REQUIRE(sorted);
  CHECK_EQUAL(*sorted, (list{int64_t{3}, int64_t{1}, int64_t{2}}));
  CHECK_EQUAL(std::move(dh).collect().size(), size_t{2});
}

TEST("sort list records from sliced input") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr = parse_expression_with_bad_diagnostics("sort(xs)", ctx);
  REQUIRE(expr);
  auto resolved = resolve_entities(*expr, ctx);
  REQUIRE(resolved);
  auto result = eval(*expr, make_sliced_list_input_slice(), dh);
  REQUIRE_EQUAL(result.length(), int64_t{2});
  auto value = materialize(result.view3_at(0));
  auto* sorted = try_as<list>(&value);
  REQUIRE(sorted);
  CHECK_EQUAL(*sorted, (list{record{{"a", int64_t{2}}, {"b", int64_t{1}}}}));
  CHECK(is<caf::none_t>(materialize(result.view3_at(1))));
}
