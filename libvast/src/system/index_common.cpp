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

#include "vast/system/index_common.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"

namespace vast {

vast::record_field fully_qualified_leaf_field::to_record_field() const {
  return {name, type};
}

fully_qualified_leaf_field
to_fully_qualified(const std::string& tn, const record_field& field) {
  return {tn + "." + field.name, field.type};
}

bool operator==(const fully_qualified_leaf_field& x,
                const fully_qualified_leaf_field& y) {
  return x.name == y.name && x.type == y.type;
}

bool operator<(const fully_qualified_leaf_field& x,
               const fully_qualified_leaf_field& y) {
  return std::tie(x.name, x.type) < std::tie(y.name, y.type);
}

} // namespace vast

namespace std {

size_t hash<vast::fully_qualified_leaf_field>::operator()(
  const vast::fully_qualified_leaf_field& x) const {
  return vast::uhash<vast::xxhash64>{}(x);
}

} // namespace std
