//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/nova/storage_fwd.hpp"
#include "tenzir/nova/type_list.hpp"
#include "tenzir/variant.hpp"

namespace tenzir::nova {

template <typename List>
class ImplementErasure;

template <typename... Ts>
class ImplementErasure<TypeList<Ts...>> {
public:
  ImplementErasure() = default;
  template <typename U>
    requires concepts::one_of<std::remove_cvref_t<U>, Ts...>
  ImplementErasure(U&& u)
    : data_{std::in_place_type<std::remove_cvref_t<U>>, std::forward<U>(u)} {
  }
  [[nodiscard]] auto length() const -> storage::Index {
    return match(data_, [](const auto& arr) {
      return arr.length();
    });
  }

protected:
  using storage = TypeList<Ts...>::template apply<variant>;
  storage data_;
};

} // namespace tenzir::nova
