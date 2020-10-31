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

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/segment.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/binary_serializer.hpp>

namespace vast {

/// Explictly instantiate table_builder template for segment.
template class fbs::table_builder<segment>;

segment_builder::segment_builder() noexcept {
  // nop
}

caf::error segment_builder::add(table_slice_ptr slice) {
  if (slice->offset() < min_table_slice_offset_)
    return make_error(ec::unspecified, "slice offsets not increasing");
  // Flatten the slice.
  auto flat_slice = pack(builder_, slice);
  if (!flat_slice)
    return flat_slice.error();
  flat_slices_.push_back(*flat_slice);
  // For slices with monotonically increasing IDs, adjust the end of the
  // intervals range map.
  if (!intervals_.empty() && intervals_.back().end() == slice->offset())
    intervals_.back()
      = {intervals_.back().begin(), intervals_.back().end() + slice->rows()};
  // Otherwise, create a new entry in the range map indicating a jump in the
  // ID space.
  else
    intervals_.emplace_back(slice->offset(), slice->offset() + slice->rows());
  min_table_slice_offset_ = slice->offset();
  num_events_ += slice->rows();
  slices_.push_back(std::move(slice));
  return caf::none;
}

caf::expected<std::vector<table_slice_ptr>>
segment_builder::lookup(const vast::ids& xs) const {
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

vast::ids segment_builder::ids() const {
  vast::ids result;
  for (const auto& slice : slices_) {
    result.append_bits(false, slice->offset() - result.size());
    result.append_bits(true, slice->rows());
  }
  return result;
}

const std::vector<table_slice_ptr>& segment_builder::slices() const {
  return slices_;
}

void segment_builder::do_reset() {
  id_ = uuid::random();
  min_table_slice_offset_ = 0;
  num_events_ = 0;
  slices_.clear();
  flat_slices_.clear();
  intervals_.clear();
}

segment_builder::offset_type segment_builder::create() {
  auto table_slices = builder_.CreateVector(flat_slices_);
  auto uuid = pack(builder_, id_);
  auto ids = builder_.CreateVectorOfStructs(intervals_);
  auto segment_v0
    = fbs::segment::Createv0(builder_, table_slices, *uuid, ids, num_events_);
  return fbs::CreateSegment(builder_, fbs::segment::Segment::v0,
                            segment_v0.Union());
}

} // namespace vast
