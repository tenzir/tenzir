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

#include <numeric>

#include "vast/detail/byte_swap.hpp"
#include "vast/detail/overlay.hpp"
#include "vast/detail/varbyte.hpp"

namespace vast::detail {

namespace {

    // Computes the offset to the beginning of the offset table.
size_t offset_table_start(chunk_ptr chk) {
  auto ptr = chk->end() - sizeof(overlay::entry_type);
  return to_host_order(*reinterpret_cast<const overlay::entry_type*>(ptr));
}

} // namespace <anonymous>

overlay::writer::writer(std::streambuf& streambuf)
  : streambuf_{streambuf},
    serializer_{streambuf_},
    deserializer_{streambuf_} {
}

size_t overlay::writer::finish() {
  if (offsets_.empty())
    return 0;
  // Delta-encode offsets and serialize them.
  std::adjacent_difference(offsets_.begin(), offsets_.end(), offsets_.begin());
  entry_type offsets_position = streambuf_.put();
  serializer_ << offsets_;
  // Write offset position as trailing bytes.
  char buf[sizeof(entry_type)];
  auto ptr = reinterpret_cast<entry_type*>(&buf);
  *ptr = to_network_order(offsets_position);
  serializer_.apply_raw(sizeof(buf), buf);
  // Enable re-use of writer by resetting offset table.
  offsets_.clear();
  return streambuf_.put();
}

overlay::reader::reader(std::streambuf& streambuf)
  : streambuf_{streambuf},
    deserializer_{streambuf_} {
  // Locate offset table position.
  auto pos = streambuf.pubseekoff(-4, std::ios::end, std::ios::in);
  if (pos == -1)
    return;
  char buf[4];
  auto got = streambuf.sgetn(buf, 4);
  if (got != 4)
    return;
  auto ptr = reinterpret_cast<entry_type*>(buf);
  auto offset_table = to_host_order(*ptr);
  // Read offsets.
  pos = streambuf.pubseekoff(offset_table, std::ios::beg, std::ios::in);
  if (pos == -1)
    return;
  deserializer_ >> offsets_;
  VAST_ASSERT(!offsets_.empty());
  // Delta-decode offsets.
  std::partial_sum(offsets_.begin(), offsets_.end(), offsets_.begin());
}

overlay::viewer::viewer(chunk_ptr chk) : chunk_{chk} {
  VAST_ASSERT(chunk_ != nullptr);
  offsets_ = offset_table{chunk_};
}

overlay::viewer::viewer(const viewer& other)
  : chunk_{other.chunk_},
    offsets_{chunk_} {
}

span<const byte> overlay::viewer::view(size_t i) const {
  VAST_ASSERT(i < chunk_->size());
  // For an intermediate element, the size is the difference in offsets.
  // For the last element, we compute the difference to the subsequently
  // following offset table.
  auto last = i + 1 == chunk_->size();
  long size = last ? offset_table_start(chunk_) : offsets_[i + 1];
  auto ptr = chunk_->data() + offsets_[i];
  return {reinterpret_cast<const byte*>(ptr), size};
}

/// @returns The number of elements in the viewer.
size_t overlay::viewer::size() const {
  return offsets_.size();
}

/// Retrieves a pointer to the underlying chunk.
chunk_ptr overlay::viewer::chunk() const {
  return chunk_;
}

overlay::viewer::offset_table::offset_table(chunk_ptr chunk)
  : table_{chunk->data() + offset_table_start(chunk)} {
  table_ += varbyte::decode(size_, table_);
  VAST_ASSERT(size_ > 0);
}

size_t overlay::viewer::offset_table::operator[](size_t i) const {
  VAST_ASSERT(i < size());
  // On-the-fly partial sum with delta decoding.
  auto result = size_t{0};
  auto ptr = table_;
  for (auto j = 0u; j <= i; ++j) {
    size_t delta;
    ptr += varbyte::decode(delta, ptr);
    result += delta;
  }
  return result;
}

size_t overlay::viewer::offset_table::size() const {
  return size_;
}

} // namespace vast::detail
