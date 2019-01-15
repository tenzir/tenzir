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
#include <caf/streambuf.hpp>

#include "vast/bitmap.hpp"
#include "vast/bitmap_algorithms.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/segment.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"

namespace vast {

using namespace binary_byte_literals;

segment_ptr segment::make(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  // Setup a CAF deserializer
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::charbuf buf{data, chunk->size()};
  caf::stream_deserializer<caf::charbuf&> source{buf};
  auto result = segment_ptr{new segment, false};
  if (auto error = source(result->header_, result->meta_)) {
    VAST_ERROR_ANON(__func__, "failed to deserialize segment meta data");
    return nullptr;
  }
  if (result->magic != magic) {
    VAST_ERROR_ANON(__func__, "got invalid segment magic", result->magic);
    return nullptr;
  }
  if (result->version > version) {
    VAST_ERROR_ANON(__func__, "got newer segment version", result->version);
    return nullptr;
  }
  // Skip meta data. Since the buffer following the chunk meta data was
  // previously serialized as chunk pointer (uint32_t size + data), we have
  // to add add sizeof(uint32_t) bytes to directly jump to the table slice
  // data.
  auto bytes_read = detail::narrow_cast<size_t>(buf.in_avail());
  VAST_ASSERT(chunk->size() > bytes_read);
  auto meta_size = chunk->size() - bytes_read;
  result->chunk_ = chunk->slice(meta_size + sizeof(uint32_t));
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
  auto slice_size = detail::narrow_cast<size_t>(slice.end - slice.start);
  // CAF won't touch the pointer during deserialization.
  caf::charbuf buf{const_cast<char*>(chunk_->data()) + slice.start, slice_size};
  caf::stream_deserializer<caf::charbuf&> deserializer{buf};
  table_slice_ptr result;
  if (auto error = deserializer(result))
    return error;
  return result;
}

caf::error inspect(caf::serializer& sink, const segment_ptr& x) {
  VAST_ASSERT(x != nullptr);
  return sink(x->header_, x->meta_, x->chunk_);
}

caf::error inspect(caf::deserializer& source, segment_ptr& x) {
  x.reset(new segment);
  return source(x->header_, x->meta_, x->chunk_);
}

} // namespace vast
