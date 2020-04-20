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

#include "vast/qualified_record_field.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"

namespace vast {

vast::record_field qualified_record_field::to_record_field() const {
  return {fqn, type};
}

bool operator==(const qualified_record_field& x,
                const qualified_record_field& y) {
  return x.fqn == y.fqn && x.type == y.type;
}

bool operator<(const qualified_record_field& x,
               const qualified_record_field& y) {
  return std::tie(x.fqn, x.type) < std::tie(y.fqn, y.type);
}

} // namespace vast

namespace std {

size_t hash<vast::qualified_record_field>::operator()(
  const vast::qualified_record_field& x) const {
  return vast::uhash<vast::xxhash64>{}(x);
}

} // namespace std
