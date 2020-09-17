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
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/version.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace vast {

using namespace binary_byte_literals;

caf::expected<segment> segment::make(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  auto fb = fbs::as_versioned_flatbuffer<fbs::Segment>(as_bytes(chunk),
                                                       fbs::Version::v0);
  if (!fb)
    return fb.error();
  // Perform version check.
  if (auto err = fbs::check_version((*fb)->version(), fbs::Version::v0))
    return err;
  return segment{std::move(chunk)};
}

uuid segment::id() const {
  auto ptr = fbs::GetSegment(chunk_->data());
  auto data = span<const uint8_t, 16>(ptr->uuid()->Data(), 16);
  return uuid{as_bytes(data)};
}

vast::ids segment::ids() const {
  vast::ids result;
  auto ptr = fbs::GetSegment(chunk_->data());
  for (auto buffer : *ptr->slices()) {
    auto slice = buffer->data_nested_root();
    result.append_bits(false, slice->offset() - result.size());
    result.append_bits(true, slice->rows());
  }
  return result;
}

size_t segment::num_slices() const {
  return fbs::GetSegment(chunk_->data())->slices()->size();
}

chunk_ptr segment::chunk() const {
  return chunk_;
}

caf::expected<std::vector<table_slice_ptr>>
segment::lookup(const vast::ids& xs) const {
  std::vector<table_slice_ptr> result;
  auto f = [](auto buffer) {
    auto slice = buffer->data_nested_root();
    return std::pair{slice->offset(), slice->offset() + slice->rows()};
  };
  auto g = [&](auto buffer) -> caf::error {
    // TODO: bind the lifetime of the table slice to the segment chunk. This
    // requires that table slices will be constructable from a chunk. Until
    // then, we stupidly deserialize the data into a new table slice.
    table_slice_ptr slice;
    if (auto err = unpack(*buffer->data_nested_root(), slice))
      return err;
    result.push_back(std::move(slice));
    return caf::none;
  };
  auto ptr = fbs::GetSegment(chunk_->data());
  auto begin = ptr->slices()->begin();
  auto end = ptr->slices()->end();
  if (auto error = select_with(xs, begin, end, f, g))
    return error;
  return result;
}

segment::segment(chunk_ptr chk) : chunk_{std::move(chk)} {
}

} // namespace vast
