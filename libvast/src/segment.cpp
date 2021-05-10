//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/segment.hpp"

#include "vast/bitmap.hpp"
#include "vast/bitmap_algorithms.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/table_slice.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/error.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <flatbuffers/base.h> // FLATBUFFERS_MAX_BUFFER_SIZE

namespace vast {

using namespace binary_byte_literals;

caf::expected<segment> segment::make(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  // FlatBuffers <= 1.11 does not correctly use '::flatbuffers::soffset_t' over
  // 'soffset_t' in FLATBUFFERS_MAX_BUFFER_SIZE.
  using ::flatbuffers::soffset_t;
  if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE)
    return caf::make_error(ec::format_error,
                           "cannot read segment because its size",
                           chunk->size(), "exceeds the maximum allowed size of",
                           FLATBUFFERS_MAX_BUFFER_SIZE);
  auto s = fbs::GetSegment(chunk->data());
  VAST_ASSERT(s); // `GetSegment` is just a cast, so this cant become null.
  if (s->segment_type() != fbs::segment::Segment::v0)
    return caf::make_error(ec::format_error, "unsupported segment version");
  return segment{std::move(chunk)};
}

uuid segment::id() const {
  auto segment = fbs::GetSegment(chunk_->data());
  auto segment_v0 = segment->segment_as_v0();
  uuid result;
  if (auto error = unpack(*segment_v0->uuid(), result))
    VAST_ERROR("couldnt get uuid from segment: {}", error);
  return result;
}

vast::ids segment::ids() const {
  vast::ids result;
  auto segment = fbs::GetSegment(chunk_->data());
  auto segment_v0 = segment->segment_as_v0();
  for (auto interval : *segment_v0->ids()) {
    result.append_bits(false, interval->begin() - result.size());
    result.append_bits(true, interval->end() - interval->begin());
  }
  return result;
}

size_t segment::num_slices() const {
  auto segment = fbs::GetSegment(chunk_->data());
  auto segment_v0 = segment->segment_as_v0();
  return segment_v0->slices()->size();
}

chunk_ptr segment::chunk() const {
  return chunk_;
}

caf::expected<std::vector<table_slice>>
segment::lookup(const vast::ids& xs) const {
  std::vector<table_slice> result;
  auto segment = fbs::GetSegment(chunk_->data())->segment_as_v0();
  if (!segment)
    return caf::make_error(ec::format_error, "invalid segment version");
  VAST_ASSERT(segment->ids()->size() == segment->slices()->size());
  auto f = [&](const auto& zip) noexcept {
    auto&& interval = std::get<0>(zip);
    return std::pair{interval->begin(), interval->end()};
  };
  auto g = [&](const auto& zip) {
    auto&& [interval, flat_slice] = zip;
    auto slice = table_slice{*flat_slice, chunk_, table_slice::verify::yes};
    slice.offset(interval->begin());
    VAST_ASSERT(slice.offset() == interval->begin());
    VAST_ASSERT(slice.offset() + slice.rows() == interval->end());
    VAST_DEBUG("{} returns slice from lookup: {}",
               detail::pretty_type_name(this), to_string(slice));
    result.push_back(std::move(slice));
    return caf::none;
  };
  // TODO: We cannot iterate over `*segment->ids()` and `*segment->slices()`
  // directly here, because the `flatbuffers::Vector<Offset<T>>` iterator
  // dereferences to a temporary pointer. This works for normal iteration, but
  // the `detail::zip` adapter tries to take the address of the pointer, which
  // cannot work. We could improve this by adding a `select_with` overload that
  // iterates over multiple ranges in lockstep.
  auto intervals = std::vector(segment->ids()->begin(), segment->ids()->end());
  auto flat_slices
    = std::vector(segment->slices()->begin(), segment->slices()->end());
  auto zipped = detail::zip(intervals, flat_slices);
  if (auto error = select_with(xs, zipped.begin(), zipped.end(), f, g))
    return error;
  return result;
}

segment::segment(chunk_ptr chk) : chunk_{std::move(chk)} {
  // nop
}

} // namespace vast
