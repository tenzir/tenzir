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
/// When `array_` is null, `inactive_` is the constant answer: false means all
/// rows are active, true means all rows are inactive. When `array_` is
/// non-null, the array is guaranteed to be mixed (not all-active or
/// all-inactive); a row `i` is inactive iff the array has a valid (non-null)
/// value equal to `inactive_`. Null entries in the array are always active.
///
/// For `and`: `ActiveRows{left, false}` — inactive where left is false.
/// For `or`:  `ActiveRows{left, true}`  — inactive where left is true.
/// For `if`:  then-branch uses `inactive_=false`, else uses `inactive_=true`.
class ActiveRows {
public:
  /// All rows active.
  ActiveRows() = default;

  /// Construct from a boolean array. Normalizes: if the array turns out to be
  /// constant (all-active or all-inactive), stores null + inactive_ instead of
  /// keeping the array, so that as_constant() is always O(1).
  explicit ActiveRows(std::shared_ptr<arrow::BooleanArray> array, bool inactive)
    : inactive_{inactive} {
    auto saw_active = false;
    auto saw_inactive = false;
    for (auto i = int64_t{0}; i < array->length(); ++i) {
      if (array->IsNull(i) or array->GetView(i) != inactive) {
        saw_active = true;
      } else {
        saw_inactive = true;
      }
      if (saw_active and saw_inactive) {
        array_ = std::move(array);
        return;
      }
    }
    // Constant: keep array_ null, normalize inactive_ to match.
    inactive_ = not saw_active;
  }

  auto is_active(int64_t i) const -> bool {
    if (not array_) {
      return not inactive_;
    }
    if (array_->IsNull(i)) {
      return true;
    }
    return array_->GetView(i) != inactive_;
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
      inactive_,
    };
  }

  auto as_constant() const -> Option<bool> {
    if (not array_) {
      // We consider all entries to be `true`. So if `inactive_` is false, then
      // all are active – hence we return `not inactive_`.
      return not inactive_;
    }
    return None{};
  }

private:
  std::shared_ptr<arrow::BooleanArray> array_;
  bool inactive_ = false;
};

} // namespace tenzir
