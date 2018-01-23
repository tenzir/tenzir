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

#include <iostream>
#include <caf/detail/scope_guard.hpp>

#include "vast/wah_bitmap.hpp"

namespace vast {

wah_bitmap::wah_bitmap(size_type n, bool bit) {
  append_bits(bit, n);
}

bool wah_bitmap::empty() const {
  return num_bits_ == 0;
}

wah_bitmap::size_type wah_bitmap::size() const {
  return num_bits_;
}

const wah_bitmap::block_vector& wah_bitmap::blocks() const {
  return blocks_;
}

void wah_bitmap::append_bit(bool bit) {
  if (blocks_.empty())
    blocks_.push_back(word_type::none);
  else if (num_last_ == word_type::literal_word_size)
    merge_active_word();
  blocks_.back() |= static_cast<block_type>(bit) << num_last_;
  ++num_last_;
  ++num_bits_;
  return;
}

void wah_bitmap::append_bits(bool bit, size_type n) {
  if (n == 0)
    return;
  if (blocks_.empty())
    blocks_.push_back(word_type::none);
  else if (num_last_ == word_type::literal_word_size)
    merge_active_word();
  // Fill up the active word.
  auto unused = word_type::literal_word_size - num_last_;
  auto inject = std::min(unused, n);
  VAST_ASSERT(inject > 0);
  if (bit)
    blocks_.back() |= word_type::word::lsb_fill(inject) << num_last_;
  num_last_ += inject;
  num_bits_ += inject;
  if (n <= inject)
    return;
  merge_active_word();
  n -= inject;
  blocks_.pop_back();
  // Now that we're at a word boundary, append fill words.
  auto fills = n / word_type::literal_word_size;
  auto partial = n % word_type::literal_word_size;
  // Can we append to a previous fill of the same kind?
  auto& prev = blocks_.back();
  if (word_type::is_fill(prev, bit)) {
    auto prev_fill_words = word_type::fill_words(prev);
    // Update previous fill if there's enough room.
    if (prev_fill_words + fills <= word_type::max_fill_words) {
      prev = word_type::make_fill(bit, prev_fill_words + fills);
      fills = 0;
    }
  }
  // Add maximum fills.
  while (fills > word_type::max_fill_words) {
    blocks_.push_back(word_type::make_fill(bit, word_type::max_fill_words));
    fills -= word_type::max_fill_words;
  }
  // Add incomplete fill.
  if (fills > 0)
    blocks_.push_back(word_type::make_fill(bit, fills));
  // No more fill words, back to the last active word.
  blocks_.push_back(word_type::none);
  if (partial > 0 && bit)
    blocks_.back() = word_type::lsb_mask(partial);
  num_last_ = partial;
  num_bits_ += n;
}

void wah_bitmap::append_block(block_type value, size_type bits) {
  VAST_ASSERT(bits > 0);
  VAST_ASSERT(bits <= word_type::width);
  if (blocks_.empty())
    blocks_.push_back(word_type::none);
  else if (num_last_ == word_type::literal_word_size)
    merge_active_word();
  auto unused = word_type::literal_word_size - num_last_;
  auto inject = std::min(unused, bits);
  VAST_ASSERT(inject > 0);
  blocks_.back() |= (value & word_type::word::lsb_fill(inject)) << num_last_;
  num_last_ += inject;
  num_bits_ += inject;
  if (bits <= inject)
    return;
  merge_active_word();
  value >>= inject;
  auto remaining = bits - inject;
  blocks_.back() = value & word_type::word::lsb_mask(remaining);
  num_last_ = remaining;
  num_bits_ += remaining;
}

void wah_bitmap::flip() {
  if (blocks_.empty())
    return;
  for (auto& block : blocks_)
    block ^= (word_type::is_fill(block) ? word_type::msb1 : word_type::all) >> 1;
  // Undo flipping of unused bits in last block.
  auto mask =
    word_type::lsb_mask(word_type::literal_word_size - num_last_) << num_last_;
  blocks_.back() ^= mask;
}

void wah_bitmap::merge_active_word() {
  VAST_ASSERT(!blocks_.empty());
  VAST_ASSERT(num_last_ == word_type::literal_word_size);
  auto guard = caf::detail::make_scope_guard([&] {
    blocks_.push_back(word_type::none);
    num_last_ = 0;
  });
  auto is_fill = word_type::all_or_none(blocks_.back(), word_type::literal_word_size);
  if (!is_fill)
    return;
  // If there's no other word than the active word, we have nothing to merge.
  if (blocks_.size() == 1) {
    blocks_.back() = word_type::make_fill(blocks_.back(), 1);
    return;
  }
  auto fill_type = blocks_.back() != word_type::none;
  // If the active word is a fill, we try to merge it with the previous word if
  // that's a fill word as well.
  if (fill_type) {
    // All 1s.
    auto& prev = *(blocks_.rbegin() + 1);
    if (word_type::is_fill(prev, true) && word_type::fill_words(prev) < word_type::max_fill_words) {
      prev = word_type::make_fill(true, word_type::fill_words(prev) + 1);
      blocks_.pop_back();
    } else {
      blocks_.back() = word_type::make_fill(true, 1);
    }
  } else {
    // All 0s.
    auto& prev = *(blocks_.rbegin() + 1);
    if (word_type::is_fill(prev, false) && word_type::fill_words(prev) < word_type::max_fill_words) {
      prev = word_type::make_fill(false, word_type::fill_words(prev) + 1);
      blocks_.pop_back();
    } else {
      blocks_.back() = word_type::make_fill(false, 1);
    }
  }
}

bool operator==(const wah_bitmap& x, const wah_bitmap& y) {
  return x.blocks_ == y.blocks_ && x.num_bits_ == y.num_bits_;
}

wah_bitmap_range::wah_bitmap_range(const wah_bitmap& bm)
  : bm_{&bm},
    begin_{bm.blocks_.begin()},
    end_{bm.blocks_.end()} {
  if (begin_ != end_)
    scan();
}

bool wah_bitmap_range::done() const {
  return begin_ == end_;
}

void wah_bitmap_range::next() {
  if (++begin_ != end_)
    scan();
}

void wah_bitmap_range::scan() {
  VAST_ASSERT(begin_ != end_);
  if (word_type::is_fill(*begin_)) {
    auto n = word_type::fill_words(*begin_) * word_type::literal_word_size;
    auto value = word_type::fill_type(*begin_);
    while (++begin_ != end_ && word_type::is_fill(*begin_, value))
      n += word_type::fill_words(*begin_) * word_type::literal_word_size;
    bits_ = {value ? word_type::word::all : word_type::word::none, n};
    --begin_;
  } else if (begin_ + 1 != end_) {
    bits_ = {*begin_, word_type::literal_word_size}; // intermediate literal
  } else {
    bits_ = {*begin_, bm_->num_last_}; // last (literal) word
  }
}

wah_bitmap_range bit_range(const wah_bitmap& bm) {
  return wah_bitmap_range{bm};
}

} // namespace vast
