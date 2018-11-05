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

#include <cstddef>
#include <vector>

#include "vast/offset.hpp"
#include "vast/type.hpp"

#include "test.hpp"

/// Returns the type at `offset{xs...}`.
template <class... Offsets>
const vast::type& at(const vast::record_type& rec, Offsets... xs) {
  auto ptr = rec.at(vast::offset{static_cast<size_t>(xs)...});
  if (!ptr)
    FAIL("offset lookup failed at " << std::vector<int>{xs...});
  return *ptr;
};

/// Returns the record type at `offset{xs...}`.
template <class... Offsets>
const vast::record_type& rec_at(const vast::record_type& rec, Offsets... xs) {
  auto& t = at(rec, xs...);
  if (!caf::holds_alternative<vast::record_type>(t))
    FAIL("expected a record type at offset " << std::vector<int>{xs...});
  return caf::get<vast::record_type>(t);
};
