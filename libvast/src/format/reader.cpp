//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/reader.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/table_slice_encoding.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/logger.hpp"

#include <caf/settings.hpp>

namespace vast::format {

reader::consumer::~consumer() {
  // nop
}
reader::reader(const caf::settings& options) {
  if (auto batch_encoding_arg
      = caf::get_if<std::string>(&options, "vast.import.batch-encoding")) {
    if (auto batch_encoding = to<table_slice_encoding>(*batch_encoding_arg))
      table_slice_type_ = *batch_encoding;
    else
      VAST_WARN("{} cannot set vast.import.batch-encoding to {} as it "
                "is not a valid encoding",
                detail::pretty_type_name(this), *batch_encoding_arg);
  }
  if (auto batch_timeout_arg
      = caf::get_if<std::string>(&options, "vast.import.batch-timeout")) {
    if (auto batch_timeout = to<decltype(batch_timeout_)>(*batch_timeout_arg))
      batch_timeout_ = *batch_timeout;
    else
      VAST_WARN("{} cannot set vast.import.batch-timeout to {} as it "
                "is not a valid duration",
                detail::pretty_type_name(this), *batch_timeout_arg);
  }
  if (auto read_timeout_arg
      = caf::get_if<std::string>(&options, "vast.import.read-timeout")) {
    if (auto read_timeout = to<decltype(batch_timeout_)>(*read_timeout_arg))
      read_timeout_ = *read_timeout;
    else
      VAST_WARN("{} cannot set vast.import.read-timeout to {} as it is "
                "not a valid duration",
                detail::pretty_type_name(this), *read_timeout_arg);
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
