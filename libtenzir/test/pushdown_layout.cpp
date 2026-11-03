//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/test/test.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <string>
#include <vector>

using namespace tenzir;

namespace {

/// Two rows of result columns, the second one masked out.
auto results() -> nova::Events {
  auto builder = nova::ArrayBuilder<nova::Record>{};
  for (auto i = int64_t{0}; i < 2; ++i) {
    auto row = builder.record();
    row.field("__tenzir_column_0").data(i);
    row.field("__tenzir_column_1").data(fmt::format("s{}", i));
    row.field("__tenzir_order_2").data(i * 10);
  }
  auto mask = nova::storage::BitMap::Mutable{2};
  mask.set(0, true);
  return nova::Events{builder.finish(), std::move(mask).finish(),
                      nova::Events::Meta::make_empty(2, "test.results")};
}

auto rows(nova::Events const& events) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  auto data = nova::Array<nova::Data>{events.data};
  for (auto row : nova::storage::true_bits(events.mask)) {
    result.push_back(
      fmt::format("{}", nova::materialize_legacy(data.get(row))));
  }
  return result;
}

} // namespace

TEST("layouts nest output fields and drop hidden columns") {
  auto dh = collecting_diagnostic_handler{};
  auto nested = pushdown::apply_layout(
    results(),
    pushdown::Layout{
      .fields = pushdown::LayoutFields{{{"a", "b"}, "__tenzir_column_0"},
                                       {{"c"}, "__tenzir_column_1"},
                                       {{"d"}, "__tenzir_missing"}},
      .hidden = {"__tenzir_order_2"},
      .schema_name = "tenzir.summarize",
    },
    dh);
  CHECK_EQUAL(rows(nested), std::vector<std::string>{"<a: <b: 0>, c: \"s0\">"});
  CHECK_EQUAL(std::string{*nested.meta.name.get(0)}, "tenzir.summarize");
  // Without output fields, the result columns stay, except the hidden ones.
  auto passthrough
    = pushdown::apply_layout(results(),
                             pushdown::Layout{.fields = None{},
                                              .hidden = {"__tenzir_order_2"},
                                              .schema_name = None{}},
                             dh);
  CHECK_EQUAL(rows(passthrough),
              std::vector<std::string>{"<__tenzir_column_0: 0, "
                                       "__tenzir_column_1: \"s0\">"});
  // Without a schema name, the events keep theirs.
  CHECK_EQUAL(std::string{*passthrough.meta.name.get(0)}, "test.results");
  // Without fields, the events remain, as for an empty selection.
  auto empty = pushdown::apply_layout(
    results(),
    pushdown::Layout{
      .fields = pushdown::LayoutFields{}, .hidden = {}, .schema_name = None{}},
    dh);
  CHECK_EQUAL(rows(empty), std::vector<std::string>{"<>"});
  CHECK(std::move(dh).collect().empty());
}
