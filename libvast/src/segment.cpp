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

segment::iterator::iterator(size_t slice_idx, interval_iterator intervals,
                            const segment* parent)
  : slice_idx_{slice_idx}, intervals_{intervals}, parent_{parent} {
  // nop
}

[[nodiscard]] table_slice segment::iterator::dereference() const {
  auto slice = parent_->get_slice_(slice_idx_);
  slice.offset((*intervals_)->begin());
  VAST_ASSERT(slice.offset() + slice.rows() == (*intervals_)->end());
  return slice;
}

void segment::iterator::increment() {
  ++slice_idx_;
  ++intervals_;
}

void segment::iterator::decrement() {
  --slice_idx_;
  --intervals_;
}

void segment::iterator::advance(size_t n) {
  slice_idx_ += n;
  intervals_ += n;
}

[[nodiscard]] bool segment::iterator::equals(segment::iterator other) const {
  return intervals_ == other.intervals_;
}

[[nodiscard]] segment::iterator::difference_type
segment::iterator::distance_to(segment::iterator other) const {
  return other.intervals_ - intervals_;
}

caf::expected<segment> segment::make(chunk_ptr&& chunk) {
  if (fbs::SegmentBufferHasIdentifier(chunk->data())) {
    auto s = flatbuffer<fbs::Segment>::make(std::move(chunk));
    if (!s)
      return s.error();
    if ((*s)->segment_type() != fbs::segment::Segment::v0)
      return caf::make_error(ec::format_error, "unsupported segment version");
    return segment{std::move(*s)};
  } else if (fbs::SegmentedFileHeaderBufferHasIdentifier(chunk->data())) {
    auto container = fbs::flatbuffer_container{std::move(chunk)};
    if (!container)
      return caf::make_error(ec::format_error, "unable to create container "
                                               "from chunk");
    return segment{std::move(container)};
  } else {
    return caf::make_error(ec::format_error, "unknown segment identifier");
  }
}

uuid segment::id() const {
  auto const* segment_v0 = flatbuffer_->segment_as_v0();
  auto result = vast::uuid{};
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
  auto const* segment_v0 = flatbuffer_->segment_as_v0();
  return segment_v0->slices()->size() + segment_v0->overflow_slices();
}

segment::iterator segment::begin() const {
  auto const* v0 = flatbuffer_->segment_as_v0();
  return segment::iterator{0, v0->ids()->begin(), this};
}

segment::iterator segment::end() const {
  auto const* v0 = flatbuffer_->segment_as_v0();
  return segment::iterator{num_slices(), v0->ids()->end(), nullptr};
}

chunk_ptr segment::chunk() const {
  if (container_)
    return container_->chunk();
  else
    return flatbuffer_.chunk();
}

caf::expected<std::vector<table_slice>>
segment::lookup(const vast::ids& xs) const {
  std::vector<table_slice> result;
  auto const* segment = flatbuffer_->segment_as_v0();
  if (!segment)
    return caf::make_error(ec::format_error, "invalid segment version");
  VAST_ASSERT(segment->ids()->size() == segment->slices()->size());
  auto f = [&](const auto& idx) noexcept {
    auto const* interval = segment->ids()->Get(idx);
    return std::pair{interval->begin(), interval->end()};
  };
  auto g = [&](const auto& idx) {
    auto const* interval = segment->ids()->Get(idx);
    auto slice = get_slice_(idx);
    slice.offset(interval->begin());
    VAST_ASSERT(slice.offset() == interval->begin());
    VAST_ASSERT(slice.offset() + slice.rows() == interval->end());
    VAST_DEBUG("{} returns slice from lookup: {}",
               detail::pretty_type_name(this), to_string(slice));
    result.push_back(std::move(slice));
    return caf::none;
  };
  auto indices = std::vector<size_t>(num_slices());
  std::iota(indices.begin(), indices.end(), 0ull);
  if (auto error = select_with(xs, indices.begin(), indices.end(), f, g))
    return error;
  return result;
}

caf::expected<std::vector<table_slice>>
segment::erase(const vast::ids& xs) const {
  const auto* segment = flatbuffer_->segment_as_v0();
  auto intervals = std::vector(segment->ids()->begin(), segment->ids()->end());
  VAST_ASSERT(segment->ids()->size()
                == segment->slices()->size() + segment->overflow_slices(),
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

vast::table_slice segment::get_slice_(size_t idx) const {
  VAST_ASSERT_CHEAP(idx < num_slices());
  const auto* segment = flatbuffer_->segment_as_v0();
  if (idx < segment->slices()->size())
    return table_slice{*segment->slices()->Get(idx), chunk(),
                       table_slice::verify::yes};
  return table_slice{container_->get_raw(1 + (idx - segment->slices()->size())),
                     table_slice::verify::yes};
}

std::vector<const vast::fbs::FlatTableSlice*> segment::flat_slices_() const {
  auto result = std::vector<const vast::fbs::FlatTableSlice*>{};
  const auto* segment = flatbuffer_->segment_as_v0();
  result.reserve(num_slices());
  result.insert(result.end(), segment->slices()->begin(),
                segment->slices()->end());
  VAST_ASSERT_CHEAP(!container_
                    || container_->size() == segment->overflow_slices() + 1);
  for (size_t i = 0; i < segment->overflow_slices(); ++i)
    // Chunk 0 is the segment itself, so we apply an offset of 1
    result.push_back(container_->as_flatbuffer<fbs::FlatTableSlice>(1 + i));
  return result;
}

segment::segment(flatbuffer<fbs::Segment> flatbuffer)
  : flatbuffer_{std::move(flatbuffer)} {
  // nop
}

segment::segment(fbs::flatbuffer_container container)
  : container_(std::move(container)) {
  // For a segment stored in a flatbuffer container, the format is that
  // the actual segment is contained in chunk 0 and all table slices
  // that could not be fit into that segment are stored sequentially in
  // chunks 1..n afterwards.
  auto segment_chunk = container_->get_raw(0);
  // FIXME: Error handling
  flatbuffer_ = *flatbuffer<fbs::Segment>::make(std::move(segment_chunk));
}

caf::expected<segment>
segment::copy_without(const vast::segment& segment, const vast::ids& xs) {
  // TODO: Add a `segment::size` field so we can get a better upper bound on
  // the segment size from the old segment.
  segment_builder builder(defaults::system::max_segment_size, segment.id());
  if (is_subset(segment.ids(), xs))
    return builder.finish();
  auto slices = segment.erase(xs);
  if (!slices)
    return slices.error();
  for (auto&& slice : std::exchange(*slices, {})) [[maybe_unused]]
    auto err = builder.add(std::move(slice));
  return builder.finish();
}

} // namespace vast
