//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/writer.hpp"

#include "vast/error.hpp"
#include "vast/format/writer_factory.hpp"
#include "vast/table_slice.hpp"

namespace vast::format {

caf::expected<std::unique_ptr<format::writer>>
writer::make(std::string output_format, const caf::settings& options) {
  return factory<format::writer>::make(output_format, options);
}

writer::~writer() {
  // nop
}

caf::expected<void> writer::flush() {
  return {};
}

} // namespace vast::format
