// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/type_system.hpp"

namespace tenzir::nova {

/// Converts an evaluated constant without materializing it as legacy data.
template <class T>
auto convert_argument(Data value, location source, diagnostic_handler& dh)
  -> failure_or<T> {
  if constexpr (std::same_as<T, Data>) {
    return value;
  } else {
    if (auto* result = try_as<T>(&value)) {
      return std::move(*result);
    }
    if constexpr (std::same_as<T, UInt>) {
      if (auto const* result = try_as<Int>(&value)) {
        if (*result < 0) {
          diagnostic::error("expected positive integer, got `{}`", *result)
            .primary(source)
            .emit(dh);
          return failure::promise();
        }
        return static_cast<UInt>(*result);
      }
    }
    if constexpr (std::same_as<T, Secret>) {
      auto promote = [](auto const& bytes) -> Secret {
        auto view = std::as_bytes(std::span{bytes});
        return Secret{ecc::cleansing_blob{view.begin(), view.end()}};
      };
      if (auto const* result = try_as<String>(&value)) {
        return promote(*result);
      }
      if (auto const* result = try_as<Blob>(&value)) {
        return promote(*result);
      }
    }
    auto actual = match(value, []<class V>(V const&) {
      return Type<V>::static_name;
    });
    constexpr auto expected = std::same_as<T, Secret>
                                ? Type<String>::static_name
                                : Type<T>::static_name;
    diagnostic::error("expected argument of type `{}`, but got `{}`", expected,
                      actual)
      .primary(source)
      .emit(dh);
    return failure::promise();
  }
}

} // namespace tenzir::nova
