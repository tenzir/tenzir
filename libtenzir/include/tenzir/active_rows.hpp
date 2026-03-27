//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <arrow/array/array_primitive.h>

#include <cstdint>

namespace tenzir {

/// Tracks which rows are active during expression evaluation.
///
/// An active row is one that must be evaluated; inactive rows may be skipped
/// and their output values are unspecified. This enables short-circuit
/// evaluation of `and`, `or`, and `if/else` without physical slicing.
///
/// When `array_` is null, all rows are active (common case). Otherwise,
/// a row `i` is inactive if and only if the array has a valid (non-null)
/// value equal to `skip_value_`. Null entries in the array are always active.
///
/// For `and`: `ActiveRows{left, false}` — skip rows where left is false.
/// For `or`:  `ActiveRows{left, true}`  — skip rows where left is true.
/// For `if`:  then-branch uses `skip_value_=false`, else uses `skip_value_=true`.
class ActiveRows {
public:
  /// All rows active.
  ActiveRows() = default;

  /// Rows where `array[i]` is valid and equals `skip_value` are inactive.
  /// Null rows are always active.
  explicit ActiveRows(arrow::BooleanArray const& array, bool skip_value)
    : array_{&array}, skip_value_{skip_value} {
  }

  auto is_active(int64_t i) const -> bool {
    if (not array_) {
      return true;
    }
    if (array_->IsNull(i)) {
      return true;
    }
    return array_->GetView(i) != skip_value_;
  }

  auto array() const -> arrow::BooleanArray const* {
    return array_;
  }

private:
  arrow::BooleanArray const* array_ = nullptr;
  bool skip_value_ = false;
};

} // namespace tenzir
