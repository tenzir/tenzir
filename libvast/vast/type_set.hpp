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

#include "vast/detail/stable_set.hpp"
#include "vast/fwd.hpp"
#include "vast/type.hpp"

namespace vast {

struct type_set : detail::stable_set<type> {
  using super = detail::stable_set<type>;
  using super::super;

  template <class Inspector>
  friend auto inspect(Inspector& f, type_set& x) {
    return f(caf::meta::type_name("vast.type_set"), static_cast<super&>(x));
  }
};

} // namespace vast
