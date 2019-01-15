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

#include "vast/bitmap.hpp"
#include "vast/bitmap_algorithms.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/segment.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/coded_deserializer.hpp"
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

segment_header make_header(chunk_ptr chunk) {
  VAST_ASSERT(chunk->size() >= sizeof(segment_header));
  auto hdr = reinterpret_cast<const segment_header*>(chunk->data());
  segment_header result;
  result.magic = from_little_endian(hdr->magic);
  result.version = from_little_endian(hdr->version);
  result.id = hdr->id;
  result.payload_offset = from_little_endian(hdr->payload_offset);
  return result;
}

} // namespace <anonymous>

segment_ptr segment::make(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  if (chunk->size() < sizeof(segment_header)) {
    VAST_ERROR_ANON(__func__, "got a chunk smaller than the segment header");
    return nullptr;
  }
  auto hdr = make_header(chunk);
  if (hdr.magic != magic) {
    VAST_ERROR_ANON(__func__, "got invalid segment magic", hdr.magic);
    return nullptr;
  }
  if (hdr.version > version) {
    VAST_ERROR_ANON(__func__, "got newer segment version", hdr.version);
    return nullptr;
  }
  // Create a segment and copy the header.
  auto result = caf::make_counted<segment>(chunk);
  result->header_ = hdr;
  // Deserialize meta data.
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::charbuf buf{data + sizeof(segment_header),
                   chunk->size() - sizeof(segment_header)};
  detail::coded_deserializer<caf::charbuf&> meta_deserializer{buf};
  if (auto error = meta_deserializer(result->meta_)) {
    VAST_ERROR_ANON(__func__, "failed to deserialize segment meta data");
    return nullptr;
  }
  return result;
}

const uuid& segment::id() const {
  return header_.id;
}

chunk_ptr segment::chunk() const {
  return chunk_;
}

size_t segment::num_slices() const {
  return meta_.slices.size();
}

caf::expected<std::vector<table_slice_ptr>>
segment::lookup(const ids& xs) const {
  std::vector<table_slice_ptr> result;
  auto f = [](auto& slice) {
    return std::pair{slice.offset, slice.offset + slice.size};
  };
  auto g = [&](auto& slice) -> caf::error {
    auto x = make_slice(slice);
    if (!x)
      return x.error();
    result.push_back(*x);
    return caf::none;
  };
  auto begin = meta_.slices.begin();
  auto end = meta_.slices.end();
  if (auto error = select_with(xs, begin, end, f, g))
    return error;
  return result;
}

caf::expected<table_slice_ptr>
segment::make_slice(const table_slice_synopsis& slice) const {
  auto payload = chunk_->data() + header_.payload_offset;
  auto slice_size = detail::narrow_cast<size_t>(slice.end - slice.start);
  // CAF won't touch the pointer during deserialization.
  caf::charbuf buf{const_cast<char*>(payload) + slice.start, slice_size};
  caf::stream_deserializer<caf::charbuf&> deserializer{buf};
  table_slice_ptr result;
  if (auto error = deserializer(result))
    return error;
  return result;
}

segment::segment(chunk_ptr chunk) : chunk_{std::move(chunk)} {
  // Only the builder and make() call this constructor. In the former case the
  // chunk is properly constructed and in the latter case the factory ensures
  // that it meets the requirements.
  VAST_ASSERT(chunk_ != nullptr);
}

caf::error inspect(caf::serializer& sink, const segment_ptr& x) {
  VAST_ASSERT(x != nullptr);
  return sink(x->chunk());
}

caf::error inspect(caf::deserializer& source, segment_ptr& x) {
  chunk_ptr chunk;
  if (auto error = source(chunk))
    return error;
  x = segment::make(std::move(chunk));
  if (x == nullptr)
    return make_error(ec::unspecified, "failed to make segment from chunk");
  return caf::none;
}

} // namespace vast
