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

#include "vast/format/reader.hpp"

namespace vast::format {

reader::consumer::~consumer() {
  // nop
}
reader::reader(caf::atom_value table_slice_type)
  : table_slice_type_(table_slice_type) {
  // nop
}

reader::~reader() {
  // nop
}

/// @returns A report for the accountant.
vast::system::report reader::status() const {
  return {};
}

} // namespace vast::format
