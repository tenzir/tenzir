//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
    return caf::make_error(ec::format_error, "no output stream available");
  out_->flush();
  if (!*out_)
    return caf::make_error(ec::format_error, "failed to flush");
  return caf::unit;
}

std::ostream& ostream_writer::out() {
  VAST_ASSERT(out_ != nullptr);
  return *out_;
}

void ostream_writer::write_buf() {
  VAST_ASSERT(out_ != nullptr);
  out_->write(buf_.data(), buf_.size());
  buf_.clear();
}

} // namespace vast::format
