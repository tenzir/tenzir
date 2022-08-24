//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/segment_builder.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/fbs/flatbuffer_container.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/segment.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/binary_serializer.hpp>

namespace vast {

segment_builder::segment_builder(size_t initial_buffer_size,
                                 const std::optional<uuid>& id)
  : builder_{initial_buffer_size} {
  reset(id);
}

caf::error segment_builder::add(table_slice x) {
  // The index already sets the correct offset for this slice, but in some unit
  // tests we test this component separately, causing incoming table slices not
  // to have an offset at all. We should fix the unit tests properly, but that
  // takes time we did not want to spend when migrating to partition-local ids.
  // -- DL
  if (x.offset() == invalid_id)
    x.offset(num_events_);
  VAST_ASSERT(x.offset() == num_events_);
  if (x.offset() < min_table_slice_offset_)
    return caf::make_error(ec::unspecified, "slice offsets not increasing");
  if (!x.is_serialized()) {
    auto serialized_x = table_slice{to_record_batch(x), x.layout(),
                                    table_slice::serialize::yes};
    serialized_x.import_time(x.import_time());
    serialized_x.offset(x.offset());
    x = std::move(serialized_x);
  }
  VAST_ASSERT(x.is_serialized());
  auto last_fb_offset = flat_slices_.empty() ? 0ull : flat_slices_.back().o;
  // Allow ca. 500MiB of extra space for the non-table data.
  constexpr auto REASONABLE_SIZE
    = detail::narrow_cast<size_t>(0.75 * FLATBUFFERS_MAX_BUFFER_SIZE);
  if (overflow_idx_ < slices_.size()
      || last_fb_offset + as_bytes(x).size() >= REASONABLE_SIZE) [[unlikely]] {
    /* nop */;
  } else {
    // This is the happy path where everything fits into a single
    // segment flatbuffer
    ++overflow_idx_;
    auto bytes = fbs::pack_bytes(builder_, x);
    auto slice = fbs::CreateFlatTableSlice(builder_, bytes);
    flat_slices_.push_back(slice);
  }
  intervals_.emplace_back(x.offset(), x.offset() + x.rows());
  num_events_ += x.rows();
  slices_.emplace_back(std::move(x));
  return caf::none;
}

segment segment_builder::finish() {
  auto table_slices_offset = builder_.CreateVector(flat_slices_);
  auto uuid_offset = pack(builder_, id_);
  auto ids_offset = builder_.CreateVectorOfStructs(intervals_);
  fbs::segment::v0Builder segment_v0_builder{builder_};
  segment_v0_builder.add_slices(table_slices_offset);
  segment_v0_builder.add_uuid(*uuid_offset);
  segment_v0_builder.add_ids(ids_offset);
  segment_v0_builder.add_events(num_events_);
  segment_v0_builder.add_overflow_slices(slices_.size() - overflow_idx_);
  auto segment_v0_offset = segment_v0_builder.Finish();
  fbs::SegmentBuilder segment_builder{builder_};
  segment_builder.add_segment_type(vast::fbs::segment::Segment::v0);
  segment_builder.add_segment(segment_v0_offset.Union());
  auto segment_offset = segment_builder.Finish();
  // The happy path.
  if (overflow_idx_ == slices_.size()) [[likely]] {
    auto segment_flatbuffer = flatbuffer<fbs::Segment>{
      builder_, segment_offset, fbs::SegmentIdentifier()};
    reset();
    return segment{std::move(segment_flatbuffer)};
  }
  // The slightly less happy path.
  FinishSegmentBuffer(builder_, segment_offset);
  auto chunk = chunk::make(builder_.Release());
  fbs::flatbuffer_container_builder cbuilder;
  cbuilder.add(as_bytes(chunk));
  for (size_t i = overflow_idx_; i < slices_.size(); ++i)
    cbuilder.add(as_bytes(slices_.at(i)));
  auto container = std::move(cbuilder).finish(fbs::SegmentIdentifier());
  return segment{std::move(container)};
}

caf::expected<std::vector<table_slice>>
segment_builder::lookup(const vast::ids& xs) const {
  std::vector<table_slice> result;
  auto f = [](auto& slice) {
    return std::pair{slice.offset(), slice.offset() + slice.rows()};
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
    result.append_bits(false, x.offset() - result.size());
    result.append_bits(true, x.rows());
  }
  return result;
}

size_t segment_builder::table_slice_bytes() const {
  return builder_.GetSize();
}

const std::vector<table_slice>& segment_builder::table_slices() const {
  return slices_;
}

void segment_builder::reset(const std::optional<uuid>& id) {
  if (id)
    id_ = *id;
  else
    id_ = uuid::random();
  min_table_slice_offset_ = 0;
  overflow_idx_ = 0;
  num_events_ = 0;
  builder_.Clear();
  flat_slices_.clear();
  intervals_.clear();
  slices_.clear();
}

} // namespace vast
