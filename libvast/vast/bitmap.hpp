//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/bitmap_base.hpp"
#include "vast/detail/operators.hpp"
#include "vast/ewah_bitmap.hpp"
#include "vast/null_bitmap.hpp"
#include "vast/wah_bitmap.hpp"

#include <caf/detail/type_list.hpp>
#include <caf/variant.hpp>

namespace vast {

class bitmap_bit_range;

/// A type-erased bitmap. This type wraps a concrete bitmap instance and models
/// the Bitmap concept at the same time.
class bitmap : public bitmap_base<bitmap>,
               detail::equality_comparable<bitmap> {
  friend bitmap_bit_range;

public:
  using types = caf::detail::type_list<
    ewah_bitmap,
    null_bitmap,
    wah_bitmap
  >;

  using variant = caf::detail::tl_apply_t<types, caf::variant>;

  /// The concrete bitmap type to be used for default construction.
  using default_bitmap = ewah_bitmap;

  /// Default-constructs a bitmap of type ::default_bitmap.
  bitmap();

  /// Constructs a bitmap from a concrete bitmap type.
  /// @param bm The bitmap instance to type-erase.
  template <
    class Bitmap,
    class = std::enable_if_t<
      caf::detail::tl_contains<types, std::decay_t<Bitmap>>::value
    >
  >
  bitmap(Bitmap&& bm) : bitmap_(std::forward<Bitmap>(bm)) {
  }

  /// Constructs a bitmap with a given number of bits having given value.
  /// @param n The number of bits.
  /// @param bit The bit value for all *n* bits.
  bitmap(size_type n, bool bit = false);

  // -- inspectors -----------------------------------------------------------

  [[nodiscard]] bool empty() const;

  [[nodiscard]] size_type size() const;

  [[nodiscard]] size_t memusage() const;

  // -- modifiers ------------------------------------------------------------

  void append_bit(bool bit);

  void append_bits(bool bit, size_type n);

  void append_block(block_type value, size_type n = word_type::width);

  void flip();

  // -- concepts -------------------------------------------------------------

  variant& get_data();
  [[nodiscard]] const variant& get_data() const;

  friend bool operator==(const bitmap& x, const bitmap& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, bitmap& bm) {
    return f(bm.bitmap_);
  }

private:
  variant bitmap_;
};

/// @relates bitmap
class bitmap_bit_range
  : public bit_range_base<bitmap_bit_range, bitmap::block_type> {
public:
  explicit bitmap_bit_range(const bitmap& bm);

  void next();
  [[nodiscard]] bool done() const;

private:
  using range_variant = caf::variant<
    ewah_bitmap_range,
    null_bitmap_range,
    wah_bitmap_range
  >;

  range_variant range_;
};

bitmap_bit_range bit_range(const bitmap& bm);

} // namespace vast

namespace caf {

template <>
struct sum_type_access<vast::bitmap> : default_sum_type_access<vast::bitmap> {};

} // namespace caf
