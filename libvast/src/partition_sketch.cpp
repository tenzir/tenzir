//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/partition_sketch.hpp"

#include "vast/data.hpp"
#include "vast/error.hpp"
#include "vast/table_slice.hpp"

namespace vast {

partition_sketch_builder::partition_sketch_builder(data config) {
  // TODO: parse config
}

caf::error partition_sketch_builder::add(const table_slice& slice) {
  auto& layout = slice.layout();
  // TODO: iterate over all columns and append each column to the respective
  // sketch.
  return ec::unimplemented;
}

} // namespace vast
