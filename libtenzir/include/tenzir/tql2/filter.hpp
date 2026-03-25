//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_utils.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"

#include <arrow/array.h>

namespace tenzir {

inline auto filter2(const table_slice& slice, const ast::expression& expr,
                    diagnostic_handler& dh, bool warn) -> table_slice {
  auto mask_builder = arrow::BooleanBuilder{arrow_memory_pool()};
  check(mask_builder.Reserve(detail::narrow_cast<int64_t>(slice.rows())));
  auto warned = false;
  for (auto& part : eval(expr, slice, dh)) {
    auto array = try_as<arrow::BooleanArray>(&*part.array);
    if (not array) {
      diagnostic::warning("expected `bool`, got `{}`", part.type.kind())
        .primary(expr)
        .emit(dh);
      for (auto i = int64_t{0}; i < part.array->length(); ++i) {
        check(mask_builder.Append(false));
      }
      continue;
    }
    if (warn and not warned and array->true_count() != array->length()) {
      diagnostic::warning("assertion failure").primary(expr).emit(dh);
      warned = true;
    }
    for (auto i = int64_t{0}; i < array->length(); ++i) {
      check(mask_builder.Append(array->IsValid(i) and array->Value(i)));
    }
  }
  return filter(slice, *finish(mask_builder));
}

} // namespace tenzir
