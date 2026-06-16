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

} // namespace

TEST("sort cmp deduplicates diagnostics from comparator evaluation") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr = parse_expression_with_location_override(
    "sort(xs, cmp=(l, r) => missing)", location::unknown, ctx);
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

TEST("sort preserves nested list value type metadata") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr = parse_expression_with_location_override("sort(r)",
                                                      location::unknown, ctx);
  REQUIRE(expr);
  auto resolved = resolve_entities(*expr, ctx);
  REQUIRE(resolved);
  auto row = data{
    record{
      {"r",
       record{
         {"xs", list{record{{"b", int64_t{2}}, {"a", int64_t{1}}}}},
       }},
    },
  };
  auto input_type = type{
    "input",
    record_type{{
      {"r", record_type{{
              {"xs", list_type{type{
                       "element",
                       record_type{{"b", int64_type{}}, {"a", int64_type{}}},
                       {{"custom", "metadata"}},
                     }}},
            }}},
    }},
  };
  auto row_series = data_to_series(std::move(row), int64_t{1});
  auto row_array
    = std::static_pointer_cast<arrow::StructArray>(row_series.array);
  auto r_array
    = std::static_pointer_cast<arrow::StructArray>(row_array->field(0));
  auto xs_array = std::static_pointer_cast<arrow::ListArray>(r_array->field(0));
  auto value_field = type{
    "element",
    record_type{{"b", int64_type{}}, {"a", int64_type{}}},
    {{"custom", "metadata"}},
  }.to_arrow_field("item");
  auto typed_xs_array = std::make_shared<arrow::ListArray>(
    arrow::list(std::move(value_field)), xs_array->length(),
    xs_array->value_offsets(), xs_array->values(), xs_array->null_bitmap(),
    xs_array->null_count(), xs_array->offset());
  auto typed_r_array = std::make_shared<arrow::StructArray>(
    arrow::struct_({as<record_type>(as<record_type>(input_type).field(0).type)
                      .field(0)
                      .type.to_arrow_field("xs")}),
    r_array->length(), arrow::ArrayVector{std::move(typed_xs_array)},
    r_array->null_bitmap(), r_array->null_count(), r_array->offset());
  auto typed_row_array = std::make_shared<arrow::StructArray>(
    input_type.to_arrow_type(), row_array->length(),
    arrow::ArrayVector{std::move(typed_r_array)}, row_array->null_bitmap(),
    row_array->null_count(), row_array->offset());
  auto input_slice = table_slice{
    record_batch_from_struct_array(input_type.to_arrow_schema(),
                                   *typed_row_array),
    std::move(input_type),
  };
  auto result = eval(*expr, input_slice, dh);
  auto expected_type = type{
    record_type{{
      {"xs", list_type{type{
               "element",
               record_type{{"a", int64_type{}}, {"b", int64_type{}}},
               {{"custom", "metadata"}},
             }}},
    }},
  };
  REQUIRE_EQUAL(result.length(), int64_t{1});
  REQUIRE_EQUAL(result.parts().size(), size_t{1});
  CHECK_EQUAL(result.part(0).type, expected_type);
}
