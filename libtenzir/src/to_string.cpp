//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/to_string.hpp"

#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/concept/printable/tenzir/json_printer_options.hpp"

#include <arrow/util/utf8.h>

namespace tenzir {

auto to_string(data_view v, location loc, diagnostic_handler& dh)
  -> std::optional<std::string> {
  using ret_t = std::optional<std::string>;
  const auto f = detail::overload{
    [](view<caf::none_t>) -> ret_t {
      return std::nullopt;
    },
    [loc, &dh](view<blob> b) -> ret_t {
      const auto* begin = reinterpret_cast<const uint8_t*>(b.data());
      const auto size = detail::narrow<int>(b.size());
      if (arrow::util::ValidateUTF8(begin, size)) {
        return std::string{
          reinterpret_cast<const char*>(b.data()),
          reinterpret_cast<const char*>(b.data()) + b.size(),
        };
      }
      diagnostic::warning("expected `blob` to contain valid UTF-8 data")
        .primary(loc)
        .emit(dh);
      return std::nullopt;
    },
    [](const view<std::string>& s) -> ret_t {
      return std::string{s};
    },
    [](const auto& v) -> ret_t {
      static const auto opts = json_printer_options{
        .tql = true,
        .oneline = true,
        .trailing_commas = false,
      };
      static const auto printer = json_printer{opts};
      auto res = std::string{};
      auto it = std::back_inserter(res);
      printer.print(it, v);
      return res;
    },
  };
  return match(v, f);
}

auto to_string(multi_series ms, location loc, diagnostic_handler& dh)
  -> basic_series<string_type> {
  auto b = arrow::StringBuilder{};
  check(b.Reserve(ms.length()));
  auto invalid_blob = false;
  const auto opts = json_printer_options{
    .tql = true,
    .oneline = true,
    .trailing_commas = false,
  };
  const auto printer = json_printer{opts};
  auto buffer = std::string{};
  for (const auto& s : ms) {
    const auto resolved = resolve_enumerations(s);
    match(
      resolved.type,
      [&](const string_type&) {
        check(append_array(b, string_type{},
                           as<arrow::StringArray>(*resolved.array)));
      },
      [&](const blob_type&) {
        for (const auto& x : resolved.values<blob_type>()) {
          if (not x) {
            check(b.AppendNull());
            continue;
          }
          const auto* begin = reinterpret_cast<const uint8_t*>(x->data());
          const auto size = detail::narrow<int>(x->size());
          if (arrow::util::ValidateUTF8(begin, size)) {
            check(b.Append(begin, size));
          } else {
            invalid_blob = true;
            check(b.AppendNull());
          }
        }
      },
      [&](const auto&) {
        for (const auto& x : resolved.values()) {
          if (is<caf::none_t>(x)) {
            check(b.AppendNull());
            continue;
          }
          auto it = std::back_inserter(buffer);
          printer.print(it, x);
          check(b.Append(buffer));
          buffer.clear();
        }
      });
  }
  if (invalid_blob) {
    diagnostic::warning("expected `blob` to contain valid UTF-8 data")
      .primary(loc)
      .emit(dh);
  }
  return {string_type{}, finish(b)};
}

} // namespace tenzir
