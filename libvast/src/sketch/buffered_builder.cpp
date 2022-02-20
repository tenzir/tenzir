//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/buffered_builder.hpp"

#include "vast/detail/array_hasher.hpp"
#include "vast/error.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/table_slice.hpp"

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

namespace vast::sketch {

caf::error buffered_builder::add(table_slice x, offset off) {
  auto record_batch = to_record_batch(x);
  const auto& layout = caf::get<record_type>(x.layout());
  auto idx = layout.flat_index(off);
  auto array = record_batch->column(idx);
  auto add = [this](uint64_t digest) {
    digests_.insert(digest);
  };
  auto nulls = caf::visit(detail::array_hasher{add}, *array);
  if (!nulls)
    return caf::make_error(ec::unspecified, //
                           fmt::format("failed to hash table slice column {}: "
                                       "{}",
                                       idx, nulls.error()));
  // Treat null value as additional value in every domain.
  if (*nulls > 0)
    ; // FIXME: do something meaningful
  return caf::none;
}

caf::expected<sketch> buffered_builder::finish() {
  auto result = build(digests_);
  if (result)
    digests_.clear();
  return result;
}

} // namespace vast::sketch
