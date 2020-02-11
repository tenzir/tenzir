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

#include <caf/binary_serializer.hpp>

namespace vast {

segment_builder::segment_builder() {
  reset();
}

caf::error segment_builder::add(table_slice_ptr x) {
  if (x->offset() < min_table_slice_offset_)
    return make_error(ec::unspecified, "slice offsets not increasing");
  auto slice = fbs::create_table_slice(builder_, x);
  if (!slice)
    return slice.error();
  table_slices_.push_back(*slice);
  slices_.push_back(x);
  return caf::none;
}

segment segment_builder::finish() {
  auto table_slices_offset = builder_.CreateVector(table_slices_);
  auto uuid_offset = fbs::create_bytes(builder_, id_);
  fbs::SegmentBuilder segment_builder{builder_};
  segment_builder.add_data(table_slices_offset);
  segment_builder.add_uuid(uuid_offset);
  segment_builder.add_version(fbs::SegmentVersion::v1);
  auto segment_offset = segment_builder.Finish();
  fbs::FinishSegmentBuffer(builder_, segment_offset);
  size_t offset;
  size_t size;
  auto ptr = builder_.ReleaseRaw(size, offset);
  auto deleter = [=]() { flatbuffers::DefaultAllocator::dealloc(ptr, size); };
  auto chk = chunk::make(size - offset, ptr + offset, deleter);
#if 0
  VAST_ASSERT(size > offset);
  auto verifier = flatbuffers::Verifier{ptr + offset, size - offset};
  VAST_ASSERT(fbs::VerifySegmentBuffer(verifier));
#endif
  reset();
  return segment{std::move(chk)};
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
  for (auto x : slices_) {
    result.append_bits(false, x->offset() - result.size());
    result.append_bits(true, x->rows());
  }
  return result;
}

size_t segment_builder::table_slice_bytes() const {
  return builder_.GetSize();
}

const std::vector<table_slice_ptr>& segment_builder::table_slices() const {
  return slices_;
}

void segment_builder::reset() {
  id_ = uuid::random();
  min_table_slice_offset_ = 0;
  builder_.Clear();
  table_slices_.clear();
  slices_.clear();
}

} // namespace vast
