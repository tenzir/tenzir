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

#include "vast/segment.hpp"

#include "vast/bitmap.hpp"
#include "vast/bitmap_algorithms.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/table_slice.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/version.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace vast {

using namespace binary_byte_literals;

caf::expected<segment> segment::make(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  auto s = fbs::GetSegment(chunk->data());
  VAST_ASSERT(s); // `GetSegment` is just a cast, so this cant become null.
  if (s->segment_type() != fbs::segment::Segment::v0)
    return make_error(ec::format_error, "unsupported segment version");
  auto vs = s->segment_as_v0();
  // This check is an artifact from an earlier flatbuffer versioning
  // scheme, where the version was stored as an inline field.
  if (vs->version() != fbs::Version::v0)
    return make_error(ec::format_error, "invalid v0 segment layout");
  return segment{std::move(chunk)};
}

uuid segment::id() const {
  auto segment = fbs::GetSegment(chunk_->data());
  auto segment_v0 = segment->segment_as_v0();
  uuid result;
  if (auto error = unpack(*segment_v0->uuid(), result))
    VAST_ERROR_ANON("couldnt get uuid from segment:", error);
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
  // TODO: Store a separate offset table in the segment header to avoid lots of
  // small disk reads, and generate the resulting id range for each table.
  std::vector<table_slice> slices;
  std::vector<table_slice> result;
  auto f = [&](const table_slice& slice) noexcept {
    VAST_ASSERT(slice.offset() != invalid_id);
    return std::pair{slice.offset(), slice.offset() + slice.rows()};
  };
  auto g = [&](const table_slice& slice) {
    VAST_DEBUG(this, "returns slice from lookup:", to_string(slice));
    result.push_back(slice);
    return caf::none;
  };
  auto segment = fbs::GetSegment(chunk_->data())->segment_as_v0();
  if (!segment)
    return make_error(ec::format_error, "invalid segment version");
  // Assign offsets to all table slices.
  if (segment->ids()->size() == 0)
    return make_error(ec::lookup_error, "encountered empty segment ids range");
  slices.reserve(segment->slices()->size());
  auto current_interval = segment->ids()->begin();
  auto current_offset = current_interval->begin();
  for (auto&& flat_slice : *segment->slices()) {
    auto slice = table_slice{*flat_slice, chunk_, table_slice::verify::no};
    slice.offset(current_offset);
    VAST_DEBUG(this, "assigns offset", current_offset, "to slice with",
               slice.rows(), "rows");
    current_offset += slice.rows();
    if (current_offset >= current_interval->end()
        && current_interval != segment->ids()->end()) {
      VAST_ASSERT(current_offset == current_interval->end());
      current_offset = (++current_interval)->begin();
    }
    slices.push_back(std::move(slice));
  }
  // Select the subset of `slices` for the given ids `xs`.
  if (auto error = select_with(xs, slices.begin(), slices.end(), f, g))
    return error;
  return result;
}

segment::segment(chunk_ptr chk) : chunk_{std::move(chk)} {
  // nop
}

} // namespace vast
