//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/type.hpp"

#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/type_traits.h>

#include <source_location>

namespace tenzir {

inline void
check(const arrow::Status& status, std::source_location location
                                   = std::source_location::current()) {
  if (not status.ok()) [[unlikely]] {
    detail::panic_impl(status.ToString(), location);
  }
}

template <class T>
[[nodiscard]] auto
check(arrow::Result<T> result, std::source_location location
                               = std::source_location::current()) -> T {
  check(result.status(), location);
  return result.MoveValueUnsafe();
}

template <std::derived_from<arrow::ArrayBuilder> T>
[[nodiscard]] auto
finish(T& x, std::source_location location = std::source_location::current()) {
  auto array = check(x.Finish(), location);
  auto cast
    = std::dynamic_pointer_cast<type_to_arrow_array_t<type_from_arrow_t<T>>>(
      std::move(array));
  TENZIR_ASSERT(cast);
  return cast;
}

} // namespace tenzir
