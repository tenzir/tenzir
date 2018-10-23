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

#include "vast/table_slice_builder.hpp"

#include <algorithm>

#include "vast/data.hpp"
#include "vast/detail/overload.hpp"

namespace vast {

table_slice_builder::~table_slice_builder() {
  // nop
}

bool table_slice_builder::recursive_add(const data& x, const type& t) {
  return caf::visit(detail::overload(
                      [&](const vector& xs, const record_type& rt) {
                        for (size_t i = 0; i < xs.size(); ++i) {
                          if (!recursive_add(xs[i], rt.fields[i].type))
                            return false;
                        }
                        return true;
                      },
                      [&](const auto&, const auto&) {
                        return add(make_view(x));
                      }),
                    x, t);
}

void table_slice_builder::reserve(size_t) {
  // nop
}

} // namespace vast
