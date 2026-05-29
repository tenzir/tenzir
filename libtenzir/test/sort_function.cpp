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
