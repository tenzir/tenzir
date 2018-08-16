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

#include <caf/stream_deserializer.hpp>

#include <caf/detail/scope_guard.hpp>

#include "vast/bitmap_algorithms.hpp"
#include "vast/const_table_slice_handle.hpp"
#include "vast/ids.hpp"
#include "vast/segment.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coded_deserializer.hpp"
#include "vast/detail/coded_serializer.hpp"
#include "vast/detail/narrow.hpp"

namespace vast {

using namespace binary_byte_literals;

namespace {

template <class T>
T to_little_endian(T x) {
  return detail::swap<detail::host_endian, detail::little_endian>(x);
}

template <class T>
T from_little_endian(T x) {
  return detail::swap<detail::little_endian, detail::host_endian>(x);
}

segment::header make_header(chunk_ptr chunk) {
  VAST_ASSERT(chunk->size() >= sizeof(segment::header));
  auto hdr = reinterpret_cast<const segment::header*>(chunk->data());
  segment::header result;
  result.magic = from_little_endian(hdr->magic);
  result.version = from_little_endian(hdr->version);
  result.id = hdr->id;
  result.payload_offset = from_little_endian(hdr->payload_offset);
  return result;
}

} // namespace <anonymous>

caf::expected<segment_ptr> segment::make(caf::actor_system& sys, chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  if (chunk->size() < sizeof(header))
    return make_error(ec::unspecified, "segment too small", chunk->size());
  auto hdr = make_header(chunk);
  if (hdr.magic != magic)
    return make_error(ec::unspecified, "invalid segment magic", hdr.magic);
  if (hdr.version > version)
    return make_error(ec::unspecified, "segment version too big", hdr.version);
  // Create a segment and copy the header.
  auto result = caf::make_counted<segment>(sys, chunk);
  result->header_ = hdr;
  // Deserialize meta data.
  caf::charbuf buf{chunk->data(), chunk->size()};
  detail::coded_deserializer<caf::charbuf&> meta_deserializer{buf};
  if (auto error = meta_deserializer(result->meta_))
    return error;
  return result;
}

const uuid& segment::id() const {
  return header_.id;
}

chunk_ptr segment::chunk() const {
  return chunk_;
}

size_t segment::slices() const {
  return meta_.slices.size();
}

caf::expected<std::vector<const_table_slice_handle>>
segment::lookup(const ids& xs) const {
  std::vector<const_table_slice_handle> result;
  auto rng = select(xs);
  auto payload = chunk_->data() + header_.payload_offset;
  // Walk in lock-step through the slices and the ID sequence.
  for (auto& slice : meta_.slices) {
    if (!rng)
      return result;
    // Make the ID range catch up if it's behind.
    if (rng.get() < slice.offset) {
      rng.skip(slice.offset);
      if (!rng)
        return result;
    }
    // If the next ID falls in the current slice, we add to the result.
    if (rng.get() >= slice.offset && rng.get() <= slice.offset + slice.size) {
      auto slice_size = detail::narrow_cast<size_t>(slice.end - slice.start);
      caf::charbuf buf{payload + slice.start, slice_size};
      caf::stream_deserializer<caf::charbuf&> deserializer{actor_system_, buf};
      const_table_slice_handle x;
      if (auto error = deserializer(x))
        return error;
      result.push_back(x);
      // Fast forward to the ID one past this slice.
      rng.skip(slice.offset + slice.size);
    }
  }
  return result;
}

segment::segment(caf::actor_system& sys, chunk_ptr chunk) 
  : actor_system_{sys},
    chunk_{std::move(chunk)} {
  // Only the builder and make() call this constructor. In the former case the
  // chunk is properly constructed and in the latter case the factory ensures
  // that it meets the requirements.
  VAST_ASSERT(chunk_ != nullptr);
}

segment_builder::segment_builder(caf::actor_system& sys)
  : actor_system_{sys},
    table_slice_streambuf_{table_slice_buffer_},
    table_slice_serializer_{actor_system_, table_slice_streambuf_} {
  reset();
}

caf::error segment_builder::add(const_table_slice_handle x) {
  if (x->offset() < min_table_slice_offset_)
    return make_error(ec::unspecified, "table slice offsets not increasing");
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
  return caf::none;
}

caf::expected<segment_ptr> segment_builder::finish() {
  auto guard = caf::detail::make_scope_guard([&] { reset(); });
  // Write header.
  segment_buffer_.resize(sizeof(segment::header));
  auto header = reinterpret_cast<segment::header*>(segment_buffer_.data());
  header->magic = to_little_endian(segment::magic);
  header->version = to_little_endian(segment::version);
  header->id = uuid::random();
  // Serialize meta data into buffer.
  caf::vectorbuf segment_streambuf{segment_buffer_};
  detail::coded_serializer<caf::vectorbuf&> meta_serializer{segment_streambuf};
  if (auto error = meta_serializer(meta_))
    return error;
  // Add the payload offset to the header.
  header = reinterpret_cast<segment::header*>(segment_buffer_.data());
  uint64_t payload_offset = segment_buffer_.size();
  header->payload_offset = to_little_endian(payload_offset);
  // Write the table slices.
  segment_buffer_.resize(segment_buffer_.size() + table_slice_buffer_.size());
  std::memcpy(segment_buffer_.data() + payload_offset,
              table_slice_buffer_.data(),
              table_slice_buffer_.size());
  // Move the complete segment buffer into a chunk.
  auto buffer = std::make_shared<std::vector<char>>(std::move(segment_buffer_));
  auto deleter = [buf=buffer](char*, size_t) mutable { buf.reset(); };
  auto chk = chunk::make(buffer->size(), buffer->data(), deleter);
  auto result = caf::make_counted<segment>(actor_system_, std::move(chk));
  header = reinterpret_cast<segment::header*>(buffer->data());
  result->header_ = *header;
  result->meta_ = std::move(meta_);
  return result;
}

size_t segment_builder::table_slice_bytes() const {
  return table_slice_buffer_.size();
}

void segment_builder::reset() {
  min_table_slice_offset_ = 0;
  meta_ = {};
  segment_buffer_ = {};
  table_slice_buffer_.clear();
}

} // namespace vast
