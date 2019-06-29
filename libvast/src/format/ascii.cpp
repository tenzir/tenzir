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
#include "vast/table_slice.hpp"

namespace vast::format::ascii {

caf::error writer::write(const table_slice& x) {
  auto iter = std::back_inserter(buf_);
  data_view_printer printer;
  for (size_t row = 0; row < x.rows(); ++row) {
    append('<');
    printer.print(iter, x.at(row, 0));
    for (size_t column = 1; column < x.columns(); ++column) {
      append(", ");
      printer.print(iter, x.at(row, column));
    }
    append(">\n");
    write_buf();
  }
  return caf::none;
}

const char* writer::name() const {
  return "ascii-writer";
}

} // namespace vast::format::ascii
