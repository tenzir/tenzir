//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"

#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/type_traits.h>

namespace tenzir {

inline void check(const arrow::Status& status) {
  TENZIR_ASSERT(status.ok(), status.ToString());
}

template <class T>
[[nodiscard]] auto check(arrow::Result<T> result) -> T {
  check(result.status());
  return result.MoveValueUnsafe();
}

template <std::derived_from<arrow::ArrayBuilder> T>
[[nodiscard]] auto finish(T& x) {
  using Type = std::conditional_t<std::same_as<arrow::StringBuilder, T>,
                                  arrow::StringType, typename T::TypeClass>;
  auto result = std::shared_ptr<typename arrow::TypeTraits<Type>::ArrayType>{};
  check(x.Finish(&result));
  TENZIR_ASSERT(result);
  return result;
}

} // namespace tenzir
