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
#include "vast/format/reader_factory.hpp"
#include "vast/logger.hpp"
#include "vast/system/configuration.hpp"

#include <caf/settings.hpp>

namespace vast::format {

caf::expected<std::unique_ptr<format::reader>>
reader::make(std::string input_format, const caf::settings& options) {
  return factory<format::reader>::make(input_format, options);
}

reader::consumer::~consumer() {
  // nop
}

reader::reader(const caf::settings& options) {
  auto parse_timeout = [&](std::string_view key, duration fallback) {
    auto timeout_value = system::get_or_duration(options, key, fallback);
    if (!timeout_value) {
      VAST_WARN("client failed to read '{}': {}", key, timeout_value.error());
      return fallback;
    }
    return *timeout_value;
  };
  batch_timeout_ = parse_timeout("vast.import.batch-timeout",
                                 vast::defaults::import::batch_timeout);
  read_timeout_ = parse_timeout("vast.import.read-timeout",
                                vast::defaults::import::read_timeout);
  last_batch_sent_ = reader_clock::now();
}

reader::~reader() {
  // nop
}

vast::system::report reader::status() const {
  return {};
}

} // namespace vast::format
