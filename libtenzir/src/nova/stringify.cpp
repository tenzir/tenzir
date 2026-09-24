//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/stringify.hpp"

#include "tenzir/concept/printable/tenzir/json_printer_options.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova_json_printer.hpp"

#include <string_view>

namespace tenzir::nova {

namespace {

auto make_printer() -> json_printer {
  return json_printer{json_printer_options{
    .tql = true,
    .oneline = true,
    .trailing_commas = false,
  }};
}

auto stringify_row(RowView<Data> const& row, json_printer& printer)
  -> std::string_view {
  return match(
    row,
    [](RowView<String> value) -> std::string_view {
      return *value;
    },
    [&](auto const&) -> std::string_view {
      printer.print(row);
      auto const bytes = printer.bytes();
      return {reinterpret_cast<char const*>(bytes.data()), bytes.size()};
    });
}

} // namespace

auto stringify(RowView<Data> const& row) -> std::string {
  auto printer = make_printer();
  return std::string{stringify_row(row, printer)};
}

auto stringify(Array<Data> const& array, storage::BitMap const& mask)
  -> Array<String> {
  TENZIR_ASSERT_EQ(array.length(), mask.length());
  auto builder = ArrayBuilder<String>{};
  auto printer = make_printer();
  for (auto index : storage::bitmap_iteration(mask)) {
    if (not index) {
      builder.skip();
      continue;
    }
    builder.data(stringify_row(array.get(*index), printer));
  }
  return builder.finish();
}

} // namespace tenzir::nova
