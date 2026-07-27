//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/projection.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/assert.hpp>

#include <arrow/compute/api_scalar.h>

namespace tenzir::plugins::iceberg::projection {

auto count_visible_conversion_failures(
  std::shared_ptr<arrow::BooleanArray> const& convertible,
  std::shared_ptr<arrow::Array> const& parent) -> int64_t {
  TENZIR_ASSERT(not parent or parent->length() == convertible->length());
  auto failures = check(arrow::compute::Invert(convertible));
  if (parent and parent->null_count() > 0) {
    auto valid = check(arrow::compute::IsValid(parent));
    failures = check(arrow::compute::And(failures, valid));
  }
  return std::static_pointer_cast<arrow::BooleanArray>(failures.make_array())
    ->true_count();
}

} // namespace tenzir::plugins::iceberg::projection
