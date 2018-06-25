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

#pragma once

#include <caf/variant.hpp>
#include <caf/detail/type_list.hpp>

#include "vast/bitmap_base.hpp"
#include "vast/ewah_bitmap.hpp"
#include "vast/null_bitmap.hpp"
#include "vast/wah_bitmap.hpp"

#include "vast/detail/operators.hpp"

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

  bool empty() const;

  size_type size() const;

  // -- modifiers ------------------------------------------------------------

  void append_bit(bool bit);

  void append_bits(bool bit, size_type n);

  void append_block(block_type value, size_type n = word_type::width);

  void flip();

  // -- concepts -------------------------------------------------------------

  variant& get_data();
  const variant& get_data() const;

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
  bool done() const;

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
