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

#include "vast/format/writer.hpp"

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"

namespace vast::format {

writer::~writer() {
  // nop
}

caf::expected<void> writer::write(const event&) {
  return ec::unimplemented;
}

caf::error writer::write(const std::vector<event>& xs) {
  for (auto& x : xs)
    if (auto res = write(x); !res)
      return res.error();
  return caf::none;
}

caf::error writer::write(const table_slice& x) {
  return write(to_events(x));
}

caf::expected<void> writer::flush() {
  return caf::no_error;
}

} // namespace vast::format
