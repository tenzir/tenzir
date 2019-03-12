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

#include "vast/ids.hpp"

#include "vast/table_slice.hpp"

namespace vast {

ids make_ids(std::initializer_list<id_range> ranges, size_t min_size,
             bool default_bit) {
  ids result;
  for (auto [first, last] : ranges) {
    VAST_ASSERT(first < last);
    if (first >= result.size()) {
      result.append_bits(false, first - result.size());
      result.append_bits(true, (last - first));
    } else {
      ids tmp;
      tmp.append_bits(false, first);
      tmp.append_bits(true, (last - first));
      result |= tmp;
    }
  }
  if (result.size() < min_size)
    result.append_bits(default_bit, min_size - result.size());
  return result;
}

ids make_ids(const table_slice& slice) {
  return make_ids({{slice.offset(), slice.offset() + slice.rows()}});
}

} // namespace vast
