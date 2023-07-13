//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/format/writer.hpp"

#include "tenzir/error.hpp"
#include "tenzir/format/writer_factory.hpp"
#include "tenzir/table_slice.hpp"

namespace tenzir::format {

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

} // namespace tenzir::format
