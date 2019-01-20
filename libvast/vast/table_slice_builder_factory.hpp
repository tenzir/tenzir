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

#include <caf/atom.hpp>

#include "vast/factory.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

namespace vast {

template <>
struct factory_traits<table_slice_builder> {
  using result_type = table_slice_builder_ptr;
  using key_type = caf::atom_value;
  using signature = result_type (*)(record_type);

  static void initialize();

  template <class T>
  static result_type make(record_type layout) {
    return T::make(std::move(layout));
  }
};

} // namespace vast
