//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/format/reader.hpp"

#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/format/reader_factory.hpp"
#include "tenzir/logger.hpp"

#include <caf/settings.hpp>

namespace tenzir::format {

caf::expected<std::unique_ptr<format::reader>>
reader::make(std::string input_format, const caf::settings& options) {
  return factory<format::reader>::make(input_format, options);
}

reader::consumer::~consumer() {
  // nop
}

reader::reader(const caf::settings& options) {
  auto parse_timeout = [&](std::string_view key, duration fallback) {
    auto timeout_value = get_or_duration(options, key, fallback);
    if (!timeout_value) {
      TENZIR_WARN("client failed to read '{}': {}", key, timeout_value.error());
      return fallback;
    }
    return *timeout_value;
  };
  batch_timeout_ = parse_timeout("tenzir.import.batch-timeout",
                                 tenzir::defaults::import::batch_timeout);
  read_timeout_ = parse_timeout("tenzir.import.read-timeout",
                                tenzir::defaults::import::read_timeout);
  last_batch_sent_ = reader_clock::now();
}

reader::~reader() {
  // nop
}

tenzir::report reader::status() const {
  return {};
}

} // namespace tenzir::format
