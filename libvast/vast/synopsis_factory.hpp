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
#include "vast/synopsis.hpp"
#include "vast/type.hpp"

#include <caf/settings.hpp>

#include <typeindex>

namespace vast {

template <>
struct factory_traits<synopsis> {
  using result_type = synopsis_ptr;
  using key_type = uint8_t;
  using signature = result_type (*)(type, const caf::settings&);

  static void initialize();

  template <concrete_type T>
  static key_type key() {
    return T::type_index();
  }

  static key_type key(const type& t) {
    return t.type_index();
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
