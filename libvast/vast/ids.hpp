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

#include "vast/aliases.hpp"
#include "vast/bitmap.hpp"
#include "vast/fwd.hpp"

namespace vast {

/// A set of IDs.
using ids = bitmap;

/// An open interval of IDs.
struct id_range {
  id_range(id from, id to) : first(from), last(to) {
    // nop
  }
  id_range(id id) : id_range(id, id + 1) {
    // nop
  }
  id first;
  id last;
};

/// Generates an ID set for the given ranges. For example,
/// `make_ids({{10, 12}, {20, 22}})` will return an ID set containing the
/// ranges [10, 12) and [20, 22), i.e., 10, 11, 20, and 21. The bitmap is
/// at least of size `min_size. If the size is less than `min_size`, additional
/// bits of value `default_bit` are appended.
ids make_ids(std::initializer_list<id_range> ranges, size_t min_size = 0,
             bool default_bit = false);

/// Generates an ID set for the given table slice.
ids make_ids(const table_slice& slice);

} // namespace vast
