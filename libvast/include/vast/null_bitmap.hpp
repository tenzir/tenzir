//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/bitmap_base.hpp"
#include "vast/bitvector.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

class null_bitmap_range;

/// An uncompressed bitmap. Essentially, a null_bitmap lifts an append-only
/// ::bitvector into a bitmap type, enabling efficient block-level operations
/// and making it compatiable with algorithms that operate on bitmaps.
class null_bitmap : public bitmap_base<null_bitmap>,
                    detail::equality_comparable<null_bitmap> {
  friend null_bitmap_range;

public:
  using bitvector_type = bitvector<block_type>;

  null_bitmap() = default;

  explicit null_bitmap(size_type n, bool bit = false);

  // -- inspectors -----------------------------------------------------------

  [[nodiscard]] bool empty() const;

  [[nodiscard]] size_type size() const;

  [[nodiscard]] size_t memusage() const;

  // -- modifiers ------------------------------------------------------------

  void append_bit(bool bit);

  void append_bits(bool bit, size_type n);

  void append_block(block_type bits, size_type n = word_type::width);

  void flip();

  // -- concepts -------------------------------------------------------------

  friend bool operator==(const null_bitmap& x, const null_bitmap& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, null_bitmap& bm) {
    return f.apply(bm.bitvector_);
  }

  friend null_bitmap_range bit_range(const null_bitmap& bm);

  friend auto
  pack(flatbuffers::FlatBufferBuilder& builder, const null_bitmap& from)
    -> flatbuffers::Offset<fbs::bitmap::NullBitmap>;

  friend auto unpack(const fbs::bitmap::NullBitmap& from, null_bitmap& to)
    -> caf::error;

private:
  bitvector_type bitvector_;
};

class null_bitmap_range
  : public bit_range_base<null_bitmap_range, null_bitmap::block_type> {
public:
  using word_type = null_bitmap::word_type;

  explicit null_bitmap_range(const null_bitmap& bm);

  void next();
  [[nodiscard]] bool done() const;

private:
  void scan();

  const null_bitmap::bitvector_type* bitvector_;
  typename null_bitmap::bitvector_type::block_vector::const_iterator block_;
  typename null_bitmap::bitvector_type::block_vector::const_iterator end_;
};


} // namespace vast


