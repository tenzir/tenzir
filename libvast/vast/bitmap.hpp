#ifndef VAST_BITMAP_HPP
#define VAST_BITMAP_HPP

#include "vast/bitmap_base.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/ewah_bitmap.hpp"
#include "vast/null_bitmap.hpp"
#include "vast/wah_bitmap.hpp"
#include "vast/variant.hpp"

namespace vast {

/// A type-erased bitmap. This type wraps a concrete bitmap instance and models
/// the Bitmap concept at the same time.
class bitmap : public bitmap_base<bitmap>,
               detail::equality_comparable<bitmap> {
public:
  /// The concrete bitmap type to be used for default construction.
  using default_bitmap = ewah_bitmap;

  /// Default-constructs a bitmap of type ::default_bitmap.
  bitmap();

  bitmap(size_type n, bool bit = false);

  /// Constructs a bitmap from a another bitmap.
  /// @param bm The concrete bitmap instance.
  template <class Bitmap, class>
  bitmap(Bitmap&& bm);

  // -- inspectors -----------------------------------------------------------

  bool empty() const;

  size_type size() const;

  // -- modifiers ------------------------------------------------------------

  void append_bit(bool bit);

  void append_bits(bool bit, size_type n);

  void append_block(block_type value, size_type n = word_type::width);

  void flip();

  // -- concepts -------------------------------------------------------------

  friend bool operator==(bitmap const& x, bitmap const& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, bitmap& bm) {
    return f(bm.bitmap_);
  }

  friend auto& expose(bitmap& bm) {
    return bm.bitmap_;
  }

private:
  using bitmap_variant = variant<
    ewah_bitmap,
    null_bitmap,
    wah_bitmap
  >;

  bitmap_variant bitmap_;
};

template <
  class Bitmap,
  class = std::enable_if_t<
    detail::contains<std::decay_t<Bitmap>, bitmap::bitmap_variant::types>{}
  >
>
bitmap::bitmap(Bitmap&& bm) : bitmap_(std::forward<Bitmap>(bm)) {
}

class bitmap_bit_range
  : public bit_range_base<bitmap_bit_range, bitmap::block_type> {
public:
  explicit bitmap_bit_range(bitmap const& bm);

  void next();
  bool done() const;

private:
  using range_variant = variant<
    ewah_bitmap_range,
    null_bitmap_range,
    wah_bitmap_range
  >;

  range_variant range_;
};

bitmap_bit_range bit_range(bitmap const& bm);

} // namespace vast

#endif
