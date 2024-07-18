//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/pp.hpp"
#include "tenzir/variant.hpp"

#include <arrow/result.h>
#include <arrow/status.h>
#include <caf/result.hpp>

#include <optional>

namespace tenzir {

/// Trait to customize the behavior of `TENZIR_TRY(...)`.
template <class T>
struct tryable;

} // namespace tenzir

template <class T>
struct tenzir::tryable<arrow::Result<T>> {
  static auto is_success(const arrow::Result<T>& x) -> bool {
    return x.ok();
  }

  static auto get_success(arrow::Result<T>&& x) -> T {
    return std::move(x);
  }

  static auto get_error(arrow::Result<T>&& x) -> arrow::Status {
    return std::move(x);
  }
};

template <>
struct tenzir::tryable<arrow::Status> {
  static auto is_success(const arrow::Status& x) -> bool {
    return x.ok();
  }

  static void get_success(arrow::Status&& x) {
    TENZIR_UNUSED(x);
  }

  static auto get_error(arrow::Status&& x) -> arrow::Status {
    return std::move(x);
  }
};

template <class T>
struct tenzir::tryable<std::optional<T>> {
  static auto is_success(const std::optional<T>& x) -> bool {
    return x.has_value();
  }

  static auto get_success(std::optional<T>&& x) -> T {
    return std::move(*x);
  }

  static auto get_error(std::optional<T>&&) -> std::nullopt_t {
    return std::nullopt;
  }
};

template <class T>
struct tenzir::tryable<caf::expected<T>> {
  static auto is_success(const caf::expected<T>& x) -> bool {
    return x.engaged();
  }

  static auto get_success(caf::expected<T>&& x) -> T {
    return std::move(*x);
  }

  static auto get_error(caf::expected<T>&& x) -> caf::error {
    return std::move(x.error());
  }
};

template <class V, class E>
struct tenzir::tryable<tenzir::variant<V, E>> {
  static auto is_success(const tenzir::variant<V, E>& x) -> bool {
    return x.index() == 0;
  }

  static auto get_success(tenzir::variant<V, E>&& x) -> V {
    return std::move(*std::get_if<0>(&x));
  }

  static auto get_error(tenzir::variant<V, E>&& x) -> E {
    return std::move(*std::get_if<1>(&x));
  }
};

#define TENZIR_TRY_COMMON(var, expr)                                           \
  auto var = (expr);                                                           \
  if (not tenzir::tryable<decltype(var)>::is_success(var)) [[unlikely]] {      \
    return tenzir::tryable<decltype(var)>::get_error(std::move(var));          \
  }

#define TENZIR_TRY_EXTRACT(decl, var, expr)                                    \
  TENZIR_TRY_COMMON(var, expr);                                                \
  decl = tenzir::tryable<decltype(var)>::get_success(std::move(var))

#define TENZIR_TRY_DISCARD(var, expr)                                          \
  TENZIR_TRY_COMMON(var, expr)                                                 \
  if (false) {                                                                 \
    /* trigger [[nodiscard]] */                                                \
    tenzir::tryable<decltype(var)>::get_success(std::move(var));               \
  }

#define TENZIR_TRY_1(expr)                                                     \
  TENZIR_TRY_DISCARD(TENZIR_PP_PASTE2(_try, __COUNTER__), expr)

#define TENZIR_TRY_2(decl, expr)                                               \
  TENZIR_TRY_EXTRACT(decl, TENZIR_PP_PASTE2(_try, __COUNTER__), expr)

#define TENZIR_TRY(...)                                                        \
  TENZIR_PP_OVERLOAD(TENZIR_TRY_, __VA_ARGS__)(__VA_ARGS__)

#define TRY TENZIR_TRY
