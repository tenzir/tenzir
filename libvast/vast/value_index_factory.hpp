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

#include "vast/factory.hpp"
#include "vast/fwd.hpp"

namespace vast {

template <>
struct factory_traits<value_index> {
  using result_type = value_index_ptr;
  using key_type = type;
  using signature = result_type (*)(type);

  static void initialize();

  static key_type key(const type& x);
};

} // namespace vast
