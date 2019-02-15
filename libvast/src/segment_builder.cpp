/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/segment_builder.hpp"

#include <caf/binary_serializer.hpp>

#include "vast/error.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/segment.hpp"
#include "vast/table_slice.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"

namespace vast {

segment_builder::segment_builder() {
  reset();
}

caf::error segment_builder::add(table_slice_ptr x) {
  if (x->offset() < min_table_slice_offset_)
    return make_error(ec::unspecified, "slice offsets not increasing");
  auto before = table_slice_buffer_.size();
  caf::binary_serializer sink{nullptr, table_slice_buffer_};
  if (auto error = sink(x)) {
    table_slice_buffer_.resize(before);
    return error;
  }
  auto after = table_slice_buffer_.size();
  VAST_ASSERT(before < after);
  meta_.slices.push_back({
    detail::narrow_cast<int64_t>(before),
    detail::narrow_cast<int64_t>(after),
    x->offset(), x->rows()});
  min_table_slice_offset_ = x->offset() + x->rows();
  slices_.push_back(x);
  return caf::none;
}

segment_ptr segment_builder::finish() {
  if (meta_.slices.empty())
    return nullptr;
  auto result = segment_ptr{new segment, false};
  result->meta_ = std::move(meta_);
  result->chunk_ = chunk::make(std::move(table_slice_buffer_));
  result->header_.magic = segment::magic;
  result->header_.version = segment::version;
  result->header_.id = id_;
  reset();
  return result;
}

caf::expected<std::vector<table_slice_ptr>>
segment_builder::lookup(const ids& xs) const {
  std::vector<table_slice_ptr> result;
  auto f = [](auto& slice) {
    return std::pair{slice->offset(), slice->offset() + slice->rows()};
  };
  auto g = [&](auto& slice) {
    result.push_back(slice);
    return caf::none;
  };
  if (auto error = select_with(xs, slices_.begin(), slices_.end(), f, g))
    return error;
  return result;
}

const uuid& segment_builder::id() const {
  return id_;
}

size_t segment_builder::table_slice_bytes() const {
  return table_slice_buffer_.size();
}

void segment_builder::reset() {
  min_table_slice_offset_ = 0;
  meta_ = {};
  id_ = uuid::random();
  table_slice_buffer_ = {};
  slices_.clear();
}

} // namespace vast
