//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution.hpp"

#include <arrow/util/utf8.h>

namespace tenzir {

auto resolved_secret_value::utf8_view() const
  -> std::optional<std::string_view> {
  const auto valid_utf8 = arrow::util::ValidateUTF8(
    reinterpret_cast<const uint8_t*>(value_.data()), value_.size());
  if (not valid_utf8) {
    return std::nullopt;
  }
  return std::string_view{
    reinterpret_cast<const char*>(value_.data()),
    value_.size(),
  };
}

auto resolved_secret_value::utf8_view(std::string_view name, location loc,
                                      diagnostic_handler& dh) const
  -> std::string_view {
  auto r = utf8_view();
  if (not r) {
    diagnostic::error("expected secret `{}` to be a UTF-8 string", name)
      .primary(loc)
      .emit(dh);
    TENZIR_UNREACHABLE();
  }
  return *r;
}

} // namespace tenzir
