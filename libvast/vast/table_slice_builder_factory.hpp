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

// -- v1 includes --------------------------------------------------------------

#include "vast/factory.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_encoding.hpp"

#include <caf/make_counted.hpp>

// -- v0 includes --------------------------------------------------------------

#include "vast/factory.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <caf/atom.hpp>

namespace vast {

template <>
struct factory_traits<v1::table_slice_builder> {
  using result_type = v1::table_slice_builder_ptr;
  using key_type = v1::table_slice_encoding;
  using signature = result_type (*)(record_type);

  static void initialize();

  template <class T>
  static key_type key() {
    return T::implementation_id;
  }

  template <class T>
  static result_type make(record_type layout) {
    static_assert(T::implementation_id != v1::table_slice_encoding::invalid
                    || std::is_same_v<T, v1::table_slice_builder>,
                  "non-default table_slice_builder implementations must set a "
                  "valid implementation_id");
    return caf::make_counted<T>(std::move(layout));
  }
};

template <>
struct factory_traits<v0::table_slice_builder> {
  using result_type = v0::table_slice_builder_ptr;
  using key_type = caf::atom_value;
  using signature = result_type (*)(record_type);

  static void initialize();

  template <class T>
  static key_type key() {
    return T::get_implementation_id();
  }

  template <class T>
  static result_type make(record_type layout) {
    return T::make(std::move(layout));
  }
};

} // namespace vast
