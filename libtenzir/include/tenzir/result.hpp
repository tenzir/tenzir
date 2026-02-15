//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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

  Result() = default;

  template <class T>
    requires std::constructible_from<Value, T>
  explicit(false) Result(T value) : value_{std::move(value)} {
  }

  explicit(false) Result(Err<Error> err) : value_{std::move(err)} {
  }

  auto expect(std::string_view msg) -> ValueRef {
    TENZIR_UNUSED(msg);
    if (is_err()) {
      TENZIR_TODO();
    }
  };

  auto is_err() const -> bool {
    return is<Err<Error>>(value_);
  }

  auto unwrap() & -> ValueRef {
    return as<VoidToUnit<Value>>(value_);
  }

  auto unwrap() && -> Value {
    return unit_to_void(as<VoidToUnit<Value>>(std::move(value_)));
  }

  auto unwrap_err() && -> Error {
    return as<Err<Error>>(std::move(value_)).unwrap();
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
