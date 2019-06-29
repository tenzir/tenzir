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

#include "vast/format/ostream_writer.hpp"

#include "vast/error.hpp"

namespace vast::format {

ostream_writer::ostream_writer(ostream_ptr out) : out_(std::move(out)) {
  // nop
}

ostream_writer::~ostream_writer() {
  // nop
}

caf::expected<void> ostream_writer::flush() {
  if (out_ == nullptr)
    return make_error(ec::format_error, "no output stream available");
  out_->flush();
  if (!*out_)
    return make_error(ec::format_error, "failed to flush");
  return caf::unit;
}

void ostream_writer::write_buf() {
  out_->write(buf_.data(), buf_.size());
  buf_.clear();
}

} // namespace vast::format
