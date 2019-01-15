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

#include <caf/detail/scope_guard.hpp>

#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/segment.hpp"
#include "vast/table_slice.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coded_serializer.hpp"
#include "vast/detail/narrow.hpp"

namespace vast {

namespace {

template <class T>
T to_little_endian(T x) {
  return detail::swap<detail::host_endian, detail::little_endian>(x);
}

} // namespace <anonymous>

segment_builder::segment_builder()
  : table_slice_streambuf_{table_slice_buffer_},
    table_slice_serializer_{table_slice_streambuf_} {
  reset();
}

caf::error segment_builder::add(table_slice_ptr x) {
  if (x->offset() < min_table_slice_offset_)
    return make_error(ec::unspecified, "slice offsets not non-decreasing");
  auto before = table_slice_buffer_.size();
  if (auto error = table_slice_serializer_(x)) {
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
  auto guard = caf::detail::make_scope_guard([&] { reset(); });
  // Write header.
  segment_buffer_.resize(sizeof(segment_header));
  auto header = reinterpret_cast<segment_header*>(segment_buffer_.data());
  header->magic = to_little_endian(segment::magic);
  header->version = to_little_endian(segment::version);
  header->id = id_;
  // Serialize meta data into buffer.
  caf::vectorbuf segment_streambuf{segment_buffer_};
  detail::coded_serializer<caf::vectorbuf&> meta_serializer{segment_streambuf};
  if (auto error = meta_serializer(meta_))
    return nullptr;
  // Add the payload offset to the header.
  header = reinterpret_cast<segment_header*>(segment_buffer_.data());
  uint64_t payload_offset = segment_buffer_.size();
  header->payload_offset = to_little_endian(payload_offset);
  // Write the table slices.
  segment_buffer_.resize(segment_buffer_.size() + table_slice_buffer_.size());
  std::memcpy(segment_buffer_.data() + payload_offset,
              table_slice_buffer_.data(),
              table_slice_buffer_.size());
  // Move the complete segment buffer into a chunk.
  auto chk = chunk::make(std::move(segment_buffer_));
  auto result = caf::make_counted<segment>(chk);
  result->header_ = *chk->as<segment_header>();
  result->meta_ = std::move(meta_);
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
  segment_buffer_ = {};
  table_slice_buffer_.clear();
  slices_.clear();
}

} // namespace vast
