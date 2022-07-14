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
#include "vast/segment_builder.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/binary_serializer.hpp>
#include <flatbuffers/base.h> // FLATBUFFERS_MAX_BUFFER_SIZE

namespace vast {

using namespace binary_byte_literals;

caf::expected<segment> segment::make(chunk_ptr&& chunk) {
  auto s = flatbuffer<fbs::Segment>::make(std::move(chunk));
  if (!s)
    return s.error();
  if ((*s)->segment_type() != fbs::segment::Segment::v0)
    return caf::make_error(ec::format_error, "unsupported segment version");
  return segment{std::move(*s)};
}

uuid segment::id() const {
  auto segment_v0 = flatbuffer_->segment_as_v0();
  uuid result;
  if (auto error = unpack(*segment_v0->uuid(), result))
    VAST_ERROR("couldnt get uuid from segment: {}", error);
  return result;
}

vast::ids segment::ids() const {
  vast::ids result;
  auto segment_v0 = flatbuffer_->segment_as_v0();
  for (auto interval : *segment_v0->ids()) {
    result.append_bits(false, interval->begin() - result.size());
    result.append_bits(true, interval->end() - interval->begin());
  }
  return result;
}

size_t segment::num_slices() const {
  auto segment_v0 = flatbuffer_->segment_as_v0();
  return segment_v0->slices()->size();
}

chunk_ptr segment::chunk() const {
  return flatbuffer_.chunk();
}

std::vector<table_slice> segment::lookup(const vast::ids& xs) const {
  std::vector<table_slice> result;
  auto const* segment = flatbuffer_->segment_as_v0();
  VAST_ASSERT(segment, "invalid segment version");
  VAST_ASSERT(segment->ids()->size() == segment->slices()->size());
  auto f = [&](const auto& zip) noexcept {
    auto&& interval = std::get<0>(zip);
    return std::pair{interval->begin(), interval->end()};
  };
  auto g = [&](const auto& zip) {
    auto&& [interval, flat_slice] = zip;
    // TODO: rework lifetime sharing API of table slice.
    auto slice = table_slice{*flat_slice, chunk(), table_slice::verify::yes};
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
  auto error = select_with(xs, zipped.begin(), zipped.end(), f, g);
  // `select_with()` only returns an error when `g` does.
  VAST_ASSERT(!error);
  return result;
}

caf::expected<std::vector<table_slice>>
segment::erase(const vast::ids& xs) const {
  const auto* segment = flatbuffer_->segment_as_v0();
  auto intervals = std::vector(segment->ids()->begin(), segment->ids()->end());
  auto flat_slices
    = std::vector(segment->slices()->begin(), segment->slices()->end());
  VAST_ASSERT(segment->ids()->size() == segment->slices()->size(),
              "inconsistent number of ids and slices");
  // We have IDs we wish to delete in `xs`, but we need a bitmap of what to
  // keep for `select` in order to fill `new_slices` with the table slices
  // that remain after dropping all deleted IDs from the segment.
  auto keep_mask = ~xs;
  std::vector<table_slice> result;
  for (unsigned int i = 0; i < segment->slices()->size(); ++i) {
    const auto& flat_slice = segment->slices()->Get(i);
    auto slice = table_slice{*flat_slice, chunk(), table_slice::verify::yes};
    slice.offset(intervals.at(i)->begin());
    // Expand keep_mask on-the-fly if needed.
    auto max_id = slice.offset() + slice.rows();
    if (keep_mask.size() < max_id)
      keep_mask.append_bits(true, max_id - keep_mask.size());
    select(result, slice, keep_mask);
  }
  return result;
}

segment::segment(flatbuffer<fbs::Segment> flatbuffer)
  : flatbuffer_{std::move(flatbuffer)} {
  // nop
}

caf::expected<segment>
segment::copy_without(const vast::segment& segment, const vast::ids& xs) {
  // TODO: Add a `segment::size` field so we can get a better upper bound on
  // the segment size from the old segment.
  segment_builder builder(defaults::system::max_segment_size, segment.id());
  if (is_subset(segment.ids(), xs)) {
    auto segments = builder.finish();
    VAST_ASSERT(segments.size() == 1);
    return std::move(segments[0]);
  }
  auto slices = segment.erase(xs);
  if (!slices)
    return slices.error();
  for (auto&& slice : std::exchange(*slices, {}))
    builder.add(slice);
  auto segments = builder.finish();
  // All slices did fit into a single segment before and we copied a subset
  // so they should again fit afterwards.
  VAST_ASSERT(segments.size() == 1);
  return std::move(segments[0]);
}

} // namespace vast
