//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/projection.hpp"

#include <tenzir/detail/assert.hpp>

namespace tenzir::plugins::iceberg::projection {

auto count_visible_conversion_failures(
  std::shared_ptr<arrow::BooleanArray> const& convertible,
  std::shared_ptr<arrow::Array> const& parent) -> int64_t {
  TENZIR_ASSERT(not parent or parent->length() == convertible->length());
  if (not parent or parent->null_count() == 0) {
    return convertible->false_count();
  }
  auto failures = int64_t{0};
  for (auto index = int64_t{0}; index < convertible->length(); ++index) {
    if (parent->IsValid(index) and convertible->IsValid(index)
        and not convertible->Value(index)) {
      ++failures;
    }
  }
  return failures;
}

} // namespace tenzir::plugins::iceberg::projection
