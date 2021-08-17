//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/atoms.hpp"
#include "vast/factory.hpp"
#include "vast/legacy_type.hpp"
#include "vast/synopsis.hpp"

#include <caf/settings.hpp>

#include <typeindex>

namespace vast {

template <>
struct factory_traits<synopsis> {
  using result_type = synopsis_ptr;
  using key_type = std::type_index;
  using signature = result_type (*)(type, const caf::settings&);

  static void initialize();

  template <class T>
  static key_type key() {
    return std::type_index{typeid(T)};
  }

  static key_type key(const type& t) {
    auto f = [](const auto& x) {
      using concrete_type = std::decay_t<decltype(x)>;
      if constexpr (std::is_same_v<concrete_type, legacy_alias_type>)
        return key(x.value_type);
      else
        return key<concrete_type>();
    };
    return caf::visit(f, t);
  }

  /// Constructs a synopsis for a given type.
  /// @param x The type to construct a synopsis for.
  /// @param opts Auxiliary context for constructing a synopsis.
  /// @relates synopsis synopsis_factory add_synopsis_factory
  /// @note The passed options may change between invocations for a given type.
  ///       Therefore, the type *x* should be sufficient to fully create a
  ///       valid synopsis instance.
  template <class T>
  static result_type make(type x, const caf::settings& opts) {
    if constexpr (std::is_constructible_v<T, type, const caf::settings&>)
      return std::make_unique<T>(std::move(x), opts);
    else
      return std::make_unique<T>(std::move(x));
  }
};

} // namespace vast
