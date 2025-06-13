//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/series.hpp>

namespace tenzir {

/// Flattens a `series` if it is a record, returning it as-is otherwise
auto flatten(series s, std::string_view flatten_separator)
  -> flatten_series_result {
  if (not s.type.kind().is<record_type>()) {
    return {std::move(s), {}};
  }
  auto [t, arr, renames]
    = flatten(s.type, std::dynamic_pointer_cast<arrow::StructArray>(s.array),
              flatten_separator);
  return {series{std::move(t), std::move(arr)}, std::move(renames)};
}

} // namespace tenzir
