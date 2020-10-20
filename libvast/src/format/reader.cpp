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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/logger.hpp"

#include <caf/settings.hpp>

namespace vast::format {

reader::consumer::~consumer() {
  // nop
}
reader::reader(table_slice_encoding table_slice_type,
               const caf::settings& options)
  : table_slice_type_(table_slice_type) {
  if (auto batch_timeout_arg
      = caf::get_if<std::string>(&options, "vast.import.batch-timeout")) {
    if (auto batch_timeout = to<decltype(batch_timeout_)>(*batch_timeout_arg))
      batch_timeout_ = *batch_timeout;
    else
      VAST_WARNING(this, "cannot set vast.import.batch-timeout to",
                   *batch_timeout_arg, "as it is not a valid duration");
  }
  if (auto read_timeout_arg
      = caf::get_if<std::string>(&options, "vast.import.read-timeout")) {
    if (auto read_timeout = to<decltype(batch_timeout_)>(*read_timeout_arg))
      read_timeout_ = *read_timeout;
    else
      VAST_WARNING(this, "cannot set vast.import.read-timeout to",
                   *read_timeout_arg, "as it is not a valid duration");
  }
  last_batch_sent_ = reader_clock::now();
}

reader::~reader() {
  // nop
}

vast::system::report reader::status() const {
  return {};
}

} // namespace vast::format
