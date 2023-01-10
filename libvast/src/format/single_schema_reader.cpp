//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/single_schema_reader.hpp"

#include "vast/error.hpp"
#include "vast/factory.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <chrono>

namespace vast::format {

single_schema_reader::single_schema_reader(const caf::settings& options)
  : reader(options) {
  // nop
}

single_schema_reader::~single_schema_reader() {
  // nop
}

caf::error single_schema_reader::finish(consumer& f, caf::error result) {
  last_batch_sent_ = reader_clock::now();
  batch_events_ = 0;
  if (builder_ != nullptr && builder_->rows() > 0) {
    auto slice = builder_->finish();
    // Override error in case we encounter an error in the builder.
    if (slice.encoding() == table_slice_encoding::none)
      return caf::make_error(ec::parse_error, "unable to finish current slice");
    f(std::move(slice));
  }
  return result;
}

bool single_schema_reader::reset_builder(type schema) {
  builder_ = std::make_shared<table_slice_builder>(std::move(schema));
  last_batch_sent_ = reader_clock::now();
  batch_events_ = 0;
  return builder_ != nullptr;
}

} // namespace vast::format
