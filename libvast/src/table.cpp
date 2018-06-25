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

#include "vast/table.hpp"

namespace vast {

table_slice::~table_slice() {
  // nop
}

const record_type& table_slice::layout() const {
  return layout_;
}

table_slice::size_type table_slice::rows() const {
  return rows_;
}

table_slice::size_type table_slice::columns() const {
  return columns_;
}

table_slice::table_slice(record_type layout) 
  : layout_{std::move(layout)},
    rows_{0},
    columns_{flatten(layout_).fields.size()} {
  // nop
}

} // namespace vast
