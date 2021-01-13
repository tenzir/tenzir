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

#include "vast/format/null.hpp"

#include "vast/fwd.hpp"

namespace vast::format::null {

writer::writer(ostream_ptr out, const caf::settings&) : super{std::move(out)} {
}

caf::error writer::write(const table_slice&) {
  return caf::none;
}

const char* writer::name() const {
  return "null-writer";
}

} // namespace vast::format::null
