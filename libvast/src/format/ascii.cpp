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

#include "vast/format/ascii.hpp"

#include "vast/concept/printable/vast/view.hpp"
#include "vast/policy/omit_field_names.hpp"
#include "vast/table_slice.hpp"

namespace vast::format::ascii {

caf::error writer::write(const table_slice_ptr& x) {
  data_view_printer printer;
  return print<policy::omit_field_names>(printer, x, "<", ", ", ">");
}

const char* writer::name() const {
  return "ascii-writer";
}

} // namespace vast::format::ascii
