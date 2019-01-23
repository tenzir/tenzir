/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <typeindex>

#include <caf/atom.hpp>

#include "vast/factory.hpp"
#include "vast/synopsis.hpp"
#include "vast/type.hpp"

namespace vast {

template <>
struct factory_traits<synopsis> {
  using result_type = synopsis_ptr;
  using key_type = std::type_index;
  using signature = result_type (*)(type, const synopsis_options&);

  static void initialize();

  template <class T>
  static key_type key() {
    return std::type_index{typeid(T)};
  }

  static key_type key(const type& t) {
    auto f = [](const auto& x) {
      using concrete_type = std::decay_t<decltype(x)>;
      if constexpr (std::is_same_v<concrete_type, alias_type>)
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
  static result_type make(type x, const synopsis_options& opts) {
    if constexpr (std::is_constructible_v<T, type, const synopsis_options&>)
      return caf::make_counted<T>(std::move(x), opts);
    else
      return caf::make_counted<T>(std::move(x));
  }
};

} // namespace vast
