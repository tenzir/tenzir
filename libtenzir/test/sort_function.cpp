//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/data.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <arrow/record_batch.h>

using namespace tenzir;

namespace {

auto make_input_slice() -> table_slice {
  auto row = data{record{
    {"pivot", int64_t{10}},
    {"xs", list{int64_t{17}, int64_t{8}, int64_t{12}, int64_t{3}}},
  }};
  auto row_series = data_to_series(row, int64_t{1});
  auto schema = type{"input", as<record_type>(row_series.type)};
  const auto& row_array = as<arrow::StructArray>(*row_series.array);
  return table_slice{
    arrow::RecordBatch::Make(schema.to_arrow_schema(), int64_t{1},
                             row_array.fields()),
    std::move(schema),
  };
}

} // namespace

TEST("sort cmp keeps captures from surrounding row context") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr = parse_expression_with_bad_diagnostics(
    "sort(xs, cmp=(l, r) => (l - pivot) * (l - pivot) < (r - pivot) * (r - "
    "pivot))",
    ctx);
  REQUIRE(expr);
  auto resolved = resolve_entities(*expr, ctx);
  REQUIRE(resolved);
  auto result = eval(*expr, make_input_slice(), dh);
  REQUIRE_EQUAL(result.length(), int64_t{1});
  auto value = materialize(result.value_at(0));
  auto* sorted = try_as<list>(&value);
  REQUIRE(sorted);
  CHECK_EQUAL(*sorted,
              (list{int64_t{8}, int64_t{12}, int64_t{17}, int64_t{3}}));
  CHECK_EQUAL(std::move(dh).collect().size(), size_t{0});
}
