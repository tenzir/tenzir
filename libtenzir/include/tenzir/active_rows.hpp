//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"

#include <arrow/array/array_primitive.h>

#include <cstdint>
#include <memory>

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
  explicit ActiveRows(std::shared_ptr<arrow::BooleanArray> array,
                      bool skip_value)
    : array_{std::move(array)}, skip_value_{skip_value} {
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

  auto slice(int64_t begin, int64_t length) const -> ActiveRows {
    TENZIR_ASSERT_GEQ(begin, 0);
    TENZIR_ASSERT_GEQ(length, 0);
    if (not array_) {
      return *this;
    }
    TENZIR_ASSERT_LEQ(begin + length, array_->length());
    return ActiveRows{
      std::static_pointer_cast<arrow::BooleanArray>(
        array_->Slice(begin, length)),
      skip_value_,
    };
  }

  auto as_constant() const -> Option<bool> {
    if (not array_) {
      return true;
    }
    auto saw_active = false;
    auto saw_inactive = false;
    for (auto row = int64_t{0}; row < array_->length(); ++row) {
      if (is_active(row)) {
        saw_active = true;
      } else {
        saw_inactive = true;
      }
      if (saw_active and saw_inactive) {
        return None{};
      }
    }
    return saw_active;
  }

private:
  std::shared_ptr<arrow::BooleanArray> array_;
  bool skip_value_ = false;
};

} // namespace tenzir
