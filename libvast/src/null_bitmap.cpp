//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/null_bitmap.hpp"

#include "vast/fbs/bitmap.hpp"

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

size_t null_bitmap::memusage() const {
  return bitvector_.capacity() * sizeof(block_type);
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

bool operator==(const null_bitmap& x, const null_bitmap& y) {
  return x.bitvector_ == y.bitvector_;
}

auto pack(flatbuffers::FlatBufferBuilder& builder, const null_bitmap& from)
  -> flatbuffers::Offset<fbs::bitmap::NullBitmap> {
  const auto bitvector_offset = fbs::bitmap::detail::CreateBitVectorDirect(
    builder, &from.bitvector_.blocks_, from.bitvector_.size_);
  return fbs::bitmap::CreateNullBitmap(builder, bitvector_offset);
}

auto unpack(const fbs::bitmap::NullBitmap& from, null_bitmap& to)
  -> caf::error {
  to.bitvector_.blocks_.clear();
  to.bitvector_.blocks_.reserve(from.bit_vector()->blocks()->size());
  to.bitvector_.blocks_.insert(to.bitvector_.blocks_.end(),
                               from.bit_vector()->blocks()->begin(),
                               from.bit_vector()->blocks()->end());
  to.bitvector_.size_ = from.bit_vector()->size();
  return caf::none;
}

null_bitmap_range::null_bitmap_range(const null_bitmap& bm)
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

null_bitmap_range bit_range(const null_bitmap& bm) {
  return null_bitmap_range{bm};
}

} // namespace vast
