//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/partition_sketch.hpp"

#include "vast/detail/assert.hpp"
#include "vast/table_slice.hpp"

namespace vast {

// partition_sketch::partition_sketch(chunk_ptr flatbuffer) noexcept
//   : flatbuffer_{std::move(flatbuffer)} {
//   VAST_ASSERT(flatbuffer_);
// }

// double partition_sketch::lookup(const predicate&) {
//   // TODO: implement
//   return 0.0;
// }

// size_t mem_usage(const partition_sketch& x) noexcept {
//   return x.flatbuffer_->size();
// }

} // namespace vast
