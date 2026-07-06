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

// Builds a table slice with `rows` rows and the schema inferred from `row`.
auto make_slice(data row, int64_t rows) -> table_slice {
  auto row_series = data_to_series(row, int64_t{1});
  auto schema = type{"input", as<record_type>(row_series.type)};
  auto slice = table_slice{
    record_batch_from_struct_array(schema.to_arrow_schema(),
                                   as<arrow::StructArray>(*row_series.array)),
    std::move(schema),
  };
  return subslice(slice, 0, rows);
}

auto eval_expr(std::string_view src, const table_slice& slice) -> multi_series {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(src, location::unknown, ctx);
  REQUIRE(expr);
  auto resolved = resolve_entities(*expr, ctx);
  REQUIRE(resolved);
  return eval(*expr, slice, dh);
}

} // namespace

// Regression for a SIGSEGV reported from a production packetbeat-to-OCSF
// pipeline (crash in `basic_series::slice` <- `split_multi_series` <- binary
// operator eval <- `where`, fault address 0x8).
//
// Evaluating an empty list literal `[]` (as in `ocsf.answers = []`) on a
// zero-row slice used to build an empty `series_builder` whose
// `finish_assert_one_array()` returned a default-constructed, null-`array`
// series. `series::length()` reports such a series as length 0, so it passed
// every length invariant and then segfaulted the instant `split_multi_series`
// sliced it while evaluating a binary comparison. The fix makes
// `finish_assert_one_array()` return a valid zero-length array instead.
TEST("empty list literal indexed and compared on a zero-row slice") {
  auto slice = make_slice(data{record{{"code", "A"}}}, 0);
  REQUIRE_EQUAL(slice.rows(), size_t{0});
  auto result = eval_expr("[][0] == \"A\"", slice);
  CHECK_EQUAL(result.length(), int64_t{0});
}

// The same shape must also work for the non-degenerate case.
TEST("empty list literal indexed and compared on a non-empty slice") {
  auto slice = make_slice(data{record{{"code", "A"}}}, 1);
  REQUIRE_EQUAL(slice.rows(), size_t{1});
  auto result = eval_expr("[][0] == \"A\"", slice);
  CHECK_EQUAL(result.length(), int64_t{1});
}
