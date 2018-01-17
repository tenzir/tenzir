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

#include "vast/null_bitmap.hpp"

namespace vast {

null_bitmap::null_bitmap(size_type n, bool bit) {
  append_bits(bit, n);
}

bool null_bitmap::empty() const {
  return bitvector_.empty();
}

null_bitmap::size_type null_bitmap::size() const {
  return bitvector_.size();
}

void null_bitmap::append_bit(bool bit) {
  bitvector_.push_back(bit);
}

void null_bitmap::append_bits(bool bit, size_type n) {
  bitvector_.resize(bitvector_.size() + n, bit);
}

void null_bitmap::append_block(block_type value, size_type bits) {
  bitvector_.append_block(value, bits);
}

void null_bitmap::flip() {
  bitvector_.flip();
}

bool operator==(null_bitmap const& x, null_bitmap const& y) {
  return x.bitvector_ == y.bitvector_;
}


null_bitmap_range::null_bitmap_range(null_bitmap const& bm)
  : bitvector_{&bm.bitvector_},
    block_{bm.bitvector_.blocks().begin()},
    end_{bm.bitvector_.blocks().end()} {
  if (block_ != end_)
    scan();
}

void null_bitmap_range::next() {
  if (++block_ != end_)
    scan();
}

bool null_bitmap_range::done() const {
  return block_ == end_;
}

void null_bitmap_range::scan() {
  auto last = end_ - 1;
  if (block_ == last) {
    // Process the last block.
    auto partial = bitvector_->size() % word_type::width;
    bits_ = {*block_, partial == 0 ? word_type::width : partial};
  } else if (!word_type::all_or_none(*block_)) {
    // Process an intermediate inhomogeneous block.
    bits_ = {*block_, word_type::width};
  } else {
    // Scan for consecutive runs of all-0 or all-1 blocks.
    auto n = word_type::width;
    auto data = *block_;
    while (++block_ != last && *block_ == data)
      n += word_type::width;
    if (block_ == last) {
      auto partial = bitvector_->size() % word_type::width;
      if (partial > 0) {
        auto mask = word_type::mask(partial);
        if ((*block_ & mask) == (data & mask)) {
          n += partial;
          ++block_;
        }
      } else if (*block_ == data) {
        n += word_type::width;
        ++block_;
      }
    }
    bits_ = {data, n};
    --block_;
  }
}

null_bitmap_range bit_range(null_bitmap const& bm) {
  return null_bitmap_range{bm};
}

} // namespace vast
