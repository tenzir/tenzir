//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/format/single_schema_reader.hpp"

#include "tenzir/cast.hpp"
#include "tenzir/error.hpp"
#include "tenzir/factory.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/type.hpp"

#include <chrono>

namespace tenzir::format {

single_schema_reader::single_schema_reader(const caf::settings& options)
  : reader(options) {
  // nop
}

single_schema_reader::~single_schema_reader() {
  // nop
}

caf::error single_schema_reader::finish(consumer& f, caf::error result,
                                        type cast_to_schema) {
  last_batch_sent_ = reader_clock::now();
  batch_events_ = 0;
  if (builder_ != nullptr && builder_->rows() > 0) {
    auto slice = builder_->finish();
    // Override error in case we encounter an error in the builder.
    if (slice.encoding() == table_slice_encoding::none)
      return caf::make_error(ec::parse_error, "unable to finish current slice");
    if (cast_to_schema)
      slice = cast(std::move(slice), cast_to_schema);
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

} // namespace tenzir::format
