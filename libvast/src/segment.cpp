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
#include "vast/const_table_slice_handle.hpp"
#include "vast/ids.hpp"
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

caf::expected<segment_ptr> segment::make(caf::actor_system& sys,
                                         chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  if (chunk->size() < sizeof(header))
    return make_error(caf::sec::invalid_argument, "segment too small",
                      chunk->size());
  auto hdr = make_header(chunk);
  if (hdr.magic != magic)
    return make_error(ec::version_error, "invalid segment magic", hdr.magic);
  if (hdr.version > version)
    return make_error(ec::version_error, "segment version too big",
                      hdr.version);
  // Create a segment and copy the header.
  auto result = caf::make_counted<segment>(sys, chunk);
  result->header_ = hdr;
  // Deserialize meta data.
  caf::charbuf buf{chunk->data() + sizeof(header),
                   chunk->size() - sizeof(header)};
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

size_t segment::num_slices() const {
  return meta_.slices.size();
}

caf::expected<std::vector<const_table_slice_handle>>
segment::lookup(const ids& xs) const {
  std::vector<const_table_slice_handle> result;
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

caf::expected<const_table_slice_handle>
segment::make_slice(const table_slice_synopsis& slice) const {
  auto payload = chunk_->data() + header_.payload_offset;
  auto slice_size = detail::narrow_cast<size_t>(slice.end - slice.start);
  caf::charbuf buf{payload + slice.start, slice_size};
  caf::stream_deserializer<caf::charbuf&> deserializer{actor_system_, buf};
  const_table_slice_handle result;
  if (auto error = deserializer(result))
    return error;
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

caf::error inspect(caf::serializer& sink, const segment_ptr& x) {
  VAST_ASSERT(x != nullptr);
  return sink(x->chunk());
}

caf::error inspect(caf::deserializer& source, segment_ptr& x) {
  chunk_ptr chunk;
  if (auto error = source(chunk))
    return error;
  if (source.context() == nullptr)
    return make_error(caf::sec::no_context);
  auto result = segment::make(source.context()->system(), std::move(chunk));
  if (!result)
    return result.error();
  x = std::move(*result);
  return caf::none;
}

} // namespace vast
