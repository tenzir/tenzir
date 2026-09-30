//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/stringify.hpp"

#include "tenzir/concept/printable/tenzir/json_printer_options.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova_json_printer.hpp"

#include <arrow/util/utf8.h>

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

auto stringify_row(RowView<Data> const& row, json_printer& printer,
                   bool& invalid_blob) -> std::string_view {
  return match(
    row,
    [](RowView<String> value) -> std::string_view {
      return *value;
    },
    [&](RowView<Blob> value) -> std::string_view {
      auto bytes = *value;
      if (not arrow::util::ValidateUTF8(
            reinterpret_cast<uint8_t const*>(bytes.data()), bytes.size())) {
        invalid_blob = true;
        return "null";
      }
      return {reinterpret_cast<char const*>(bytes.data()), bytes.size()};
    },
    [&](auto const&) -> std::string_view {
      printer.print(row);
      auto const bytes = printer.bytes();
      return {reinterpret_cast<char const*>(bytes.data()), bytes.size()};
    });
}

} // namespace

auto stringify(RowView<Data> const& row) -> Option<std::string> {
  auto printer = make_printer();
  auto invalid_blob = false;
  auto value = stringify_row(row, printer, invalid_blob);
  if (invalid_blob) {
    return None{};
  }
  return std::string{value};
}

auto stringify(Array<Data> const& array, storage::BitMap const& mask)
  -> Array<String> {
  auto dh = null_diagnostic_handler{};
  return stringify(array, mask, location::unknown, dh).data;
}

auto stringify(Array<Data> const& array, storage::BitMap const& mask,
               location source, diagnostic_handler& dh)
  -> MaskedArray<Array<String>> {
  TENZIR_ASSERT_EQ(array.length(), mask.length());
  auto builder = ArrayBuilder<String>{};
  auto present = storage::BitMap::Builder{};
  auto printer = make_printer();
  auto invalid_blob = false;
  for (auto index : storage::bitmap_iteration(mask)) {
    if (not index) {
      builder.skip();
      present.emplace_back(false);
      continue;
    }
    auto row = array.get(*index);
    auto invalid = false;
    builder.data(stringify_row(row, printer, invalid));
    present.emplace_back(not invalid and not is<RowView<Null>>(row));
    invalid_blob = invalid_blob or invalid;
  }
  if (invalid_blob) {
    diagnostic::warning("expected `blob` to contain valid UTF-8 data")
      .primary(source)
      .emit(dh);
  }
  return {builder.finish(), std::move(present).finish()};
}

} // namespace tenzir::nova
