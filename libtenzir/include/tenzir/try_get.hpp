//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/concept/convertible/to.hpp"

namespace tenzir {

/// Tries to find the entry with the dot-sperated `path` with the given type.
/// Attempts to convert the entry, if possible.
/// @pre `!path.empty()`
template <class T>
auto try_get(const record& r, std::string_view path)
  -> caf::expected<std::optional<T>> {
  auto result = descend(&r, path);
  if (! result) {
    // Error.
    return std::move(result.error());
  }
  if (! *result) {
    // Entry not found.
    return std::nullopt;
  }
  // Attempt conversion.
  return match(**result, [&](auto& x) -> caf::expected<std::optional<T>> {
    using U = std::remove_cvref_t<decltype(x)>;
    if constexpr (std::is_same_v<U, T>) {
      return x;
    } else if constexpr (convertible<U, T, data_to_type_t<T>>) {
      return tenzir::to<T>(x);
    } else {
      return caf::make_error(
        ec::convert_error,
        fmt::format("'{}' has type {}, which cannot be converted to {}", path,
                    typeid(U).name(), typeid(T).name()));
    }
  });
}

/// Tries to find the entry with the dot-sperated `path` with the given type.
/// Does not attempt to perform any conversions.
/// @pre `!path.empty()`
template <class T>
auto try_get_only(const record& r, std::string_view path)
  -> caf::expected<T const*> {
  auto result = descend(&r, path);
  if (! result) {
    return std::move(result.error());
  }
  if (! *result) {
    return nullptr;
  }
  return match(**result, [&](auto& x) -> caf::expected<T const*> {
    using U = std::remove_cvref_t<decltype(x)>;
    if constexpr (std::is_same_v<U, T>) {
      return &x;
    } else {
      return caf::make_error(
        ec::type_clash, fmt::format("'{}' has type {} but expected {}", path,
                                    typeid(U).name(), typeid(T).name()));
    }
  });
}

template <class T>
auto try_get_or(const record& r, std::string_view path, const T& fallback)
  -> caf::expected<T> {
  auto result = try_get<T>(r, path);
  if (! result.has_value()) {
    return std::move(result.error());
  }
  if (! result->has_value()) {
    return fallback;
  }
  return std::move(**result);
}

} // namespace tenzir
