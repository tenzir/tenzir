//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/try.hpp"
#include "tenzir/unit.hpp"
#include "tenzir/variant.hpp"

namespace tenzir {

template <class T>
class Err {
public:
  explicit Err(T value) : value_{std::move(value)} {
  }

  auto unwrap() & -> T& {
    return value_;
  }

  auto unwrap() const& -> T const& {
    return value_;
  }

  auto unwrap() && -> T {
    return std::move(value_);
  }

private:
  T value_;
};

template <class Value, class Error>
class [[nodiscard]] Result {
public:
  using ValueRef = std::add_lvalue_reference_t<Value>;
  using ConstValueRef = std::add_lvalue_reference_t<std::add_const_t<Value>>;
  using ErrorRef = std::add_lvalue_reference_t<Error>;
  using ConstErrorRef = std::add_lvalue_reference_t<std::add_const_t<Error>>;

  Result() = default;

  template <class T>
    requires std::constructible_from<Value, T>
  explicit(false) Result(T value) : value_{std::move(value)} {
  }

  explicit(false) Result(Err<Error> err) : value_{std::move(err)} {
  }

  auto expect(std::string_view msg) && -> Value {
    if (auto value = try_as<VoidToUnit<Value>>(value_)) [[likely]] {
      return unit_to_void(std::move(*value));
    }
    panic("unexpected result error: {}", msg);
  };

  explicit operator bool() const {
    return not is_err();
  }

  auto is_err() const -> bool {
    return is<Err<Error>>(value_);
  }

  auto is_ok() const -> bool {
    return not is_err();
  }

  template <class F>
  auto map_err(F&& f) && -> Result<Value, std::invoke_result_t<F, Error>> {
    if (is_ok()) [[likely]] {
      return std::move(*this).unwrap_unchecked();
    } else {
      return Err{std::invoke(std::forward<F>(f),
                             std::move(*this).unwrap_err_unchecked())};
    }
  }

  auto unwrap() & -> ValueRef {
    return as<VoidToUnit<Value>>(value_);
  }

  auto unwrap() const& -> ConstValueRef {
    return as<VoidToUnit<Value>>(value_);
  }

  auto unwrap() && -> Value {
    return unit_to_void(as<VoidToUnit<Value>>(std::move(value_)));
  }

  auto unwrap_unchecked() && -> Value {
    return unit_to_void(std::move(*try_as<VoidToUnit<Value>>(value_)));
  }

  auto unwrap_err_unchecked() && -> Error {
    return unit_to_void(std::move(*try_as<Err<Error>>(value_)).unwrap());
  }

  auto unwrap_err() & -> ErrorRef {
    return as<Err<Error>>(value_).unwrap();
  }

  auto unwrap_err() const& -> ConstErrorRef {
    return as<Err<Error>>(value_).unwrap();
  }

  auto unwrap_err() && -> Error {
    return as<Err<Error>>(std::move(value_)).unwrap();
  }

  auto ignore() const -> void {
    // Does nothing.
  }

private:
  variant<VoidToUnit<Value>, Err<Error>> value_;
};

} // namespace tenzir

template <class V, class E>
struct tenzir::tryable<tenzir::Result<V, E>> {
  static auto is_success(tenzir::Result<V, E> const& x) -> bool {
    return not x.is_err();
  }

  static auto get_success(tenzir::Result<V, E>&& x) -> tenzir::VoidToUnit<V> {
    if constexpr (std::is_void_v<V>) {
      return {};
    } else {
      return std::move(x).unwrap();
    }
  }

  static auto get_error(tenzir::Result<V, E>&& x) -> tenzir::Err<E> {
    return tenzir::Err{std::move(x).unwrap_err()};
  }
};
