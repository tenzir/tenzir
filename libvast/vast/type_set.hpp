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

#include "vast/type.hpp"

#include <unordered_set>

namespace vast {

struct type_set {
  std::unordered_set<vast::type> value;

  template <class Inspector>
  friend auto inspect(Inspector& f, type_set& x) {
    return f(caf::meta::type_name("type_set"), x.value);
  }
};

} // namespace vast
