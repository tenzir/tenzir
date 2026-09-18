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

auto stringify(Array<Data> const& array, storage::BitMap const& mask)
  -> Array<String> {
  TENZIR_ASSERT_EQ(array.length(), mask.length());
  auto builder = ArrayBuilder<String>{};
  auto printer = json_printer{json_printer_options{
    .tql = true,
    .oneline = true,
    .trailing_commas = false,
  }};
  for (auto index : storage::bitmap_iteration(mask)) {
    if (not index) {
      builder.skip();
      continue;
    }
    auto row = array.get(*index);
    match(
      row,
      [&](RowView<std::string_view> value) {
        builder.data(*value);
      },
      [&](auto) {
        printer.print(row);
        auto const bytes = printer.bytes();
        builder.data(std::string_view{
          reinterpret_cast<char const*>(bytes.data()), bytes.size()});
      });
  }
  return builder.finish();
}

} // namespace tenzir::nova
