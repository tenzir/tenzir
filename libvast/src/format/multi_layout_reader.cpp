//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/multi_layout_reader.hpp"

#include "vast/error.hpp"
#include "vast/factory.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast::format {

multi_layout_reader::multi_layout_reader(const caf::settings& options)
  : reader(options) {
  // nop
}

multi_layout_reader::~multi_layout_reader() {
  // nop
}

caf::error
multi_layout_reader::finish(consumer& f, table_slice_builder_ptr& builder_ptr,
                            caf::error result) {
  auto rows = builder_ptr->rows();
  if (builder_ptr != nullptr && rows > 0) {
    if (batch_events_ >= rows) {
      batch_events_ -= rows;
    } else {
      // This is a defensive mechanism to prevent wrap-around. If we run into
      // this case we probably have a counting logic bug in the reader
      // implementation (which is required to bump batch_events_ for every
      // successfully added event), but it is not an error, so there is no
      // reason to treat it as one.
      VAST_WARN("{} detected event counting mismatch: expected {}, got {}",
                name(), rows, batch_events_);
      batch_events_ = 0;
    }
    auto slice = builder_ptr->finish();
    // Override error in case we encounter an error in the builder.
    if (slice.encoding() == table_slice_encoding::none)
      return caf::make_error(ec::parse_error, "unable to finish current slice");
    f(std::move(slice));
  }
  return result;
}

caf::error multi_layout_reader::finish(consumer& f, caf::error result) {
  last_batch_sent_ = reader_clock::now();
  for (auto& kvp : builders_)
    if (auto err = finish(f, kvp.second))
      return err;
  return result;
}

table_slice_builder_ptr multi_layout_reader::builder(const type& t) {
  auto i = builders_.find(t);
  if (i != builders_.end())
    return i->second;
  if (!caf::holds_alternative<record_type>(t)) {
    VAST_ERROR("{} cannot create slice builder for non-record type: {}", name(),
               t);
    // Insert a nullptr into the map and return it to make sure the error gets
    // printed only once.
    return builders_[t];
  }
  auto builder = std::make_shared<table_slice_builder>(t);
  builders_.emplace(t, builder);
  return builder;
}

} // namespace vast::format
