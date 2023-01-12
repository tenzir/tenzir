//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/format/reader.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast::format {

/// Base class for readers that only have a single schema at any point in time.
class single_schema_reader : public reader {
public:
  single_schema_reader(const caf::settings& options);

  ~single_schema_reader() override;

protected:
  /// Convenience function for finishing our current table slice in `builder_`
  /// before reporting an error. Usually simply returns `result` after
  /// finishing the slice, however, an error in finishing the slice "overrides"
  /// `result`.
  /// @param f Consumer for the finished slice.
  /// @param result Current status of the parent context, usually returned
  ///               unmodified.
  /// @returns `result`, unless any `finish()` call fails.
  caf::error finish(consumer& f, caf::error result = caf::none);

  /// Tries to create a new table slice builder from given schema.
  bool reset_builder(type schema);

  /// Stores the current builder instance.
  table_slice_builder_ptr builder_;
};

} // namespace vast::format
