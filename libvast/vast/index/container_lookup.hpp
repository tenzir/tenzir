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

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/ids.hpp"
#include "vast/operator.hpp"
#include "vast/view.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <algorithm>
#include <memory>
#include <type_traits>

namespace vast::detail {

template <class Index, class Sequence>
caf::expected<ids>
container_lookup_impl(const Index& idx, relational_operator op,
                      const Sequence& xs) {
  ids result;
  if (op == relational_operator::in) {
    result = ids{idx.offset(), false};
    for (auto x : xs) {
      auto r = idx.lookup(relational_operator::equal, x);
      if (r)
        result |= *r;
      else
        return r;
      if (all<1>(result)) // short-circuit
        return result;
    }
  } else if (op == relational_operator::not_in) {
    result = ids{idx.offset(), true};
    for (auto x : xs) {
      auto r = idx.lookup(relational_operator::equal, x);
      if (r)
        result -= *r;
      else
        return r;
      if (all<0>(result)) // short-circuit
        return result;
    }
  } else {
    return caf::make_error(ec::unsupported_operator, op);
  }
  return result;
}

template <class Index>
caf::expected<ids>
container_lookup(const Index& idx, relational_operator op, view<list> xs) {
  VAST_ASSERT(xs);
  return container_lookup_impl(idx, op, *xs);
}

} // namespace vast::detail
