#ifndef VAST_BITMAP_H
#define VAST_BITMAP_H

#include <type_traits>

#include "vast/base.h"
#include "vast/binner.h"
#include "vast/coder.h"
#include "vast/detail/order.h"

namespace vast {

struct access;
class ewah_bitstream;

/// An associative array which maps (arithmetic) values to [bitstreams](@ref
/// bitstream).
/// @tparam T The value type for append and lookup operation.
/// @tparam Base The base determining the value decomposition
/// @tparam Coder The encoding/decoding policy.
/// @tparam Binner The pre-processing policy to perform on values.
template <
  typename T,
  typename Coder =
    multi_level_coder<
      make_uniform_base<2, T>,
      range_coder<ewah_bitstream>
    >,
  typename Binner = identity_binner
>
class bitmap : util::equality_comparable<bitmap<T, Coder, Binner>> {
  static_assert(! std::is_same<T, bool>{} || is_singleton_coder<Coder>{},
                "boolean bitmap requires singleton coder");
  friend access;

public:
  using value_type = T;
  using coder_type = Coder;
  using binner_type = Binner;
  using bitstream_type = typename coder_type::bitstream_type;

  bitmap() = default;

  template <
    typename... Ts,
    typename = std::enable_if_t<std::is_constructible<coder_type, Ts...>{}>
  >
  explicit bitmap(Ts&&... xs)
    : coder_(std::forward<Ts>(xs)...) {
  }

  friend bool operator==(bitmap const& x, bitmap const& y) {
    return x.coder_ == y.coder_;
  }

  /// Adds a value to the bitmap. For example, in the case of equality
  /// coding, this means appending 1 to the single bitstream for the given
  /// value and 0 to all other bitstreams.
  /// @param x The value to append.
  /// @param n The number of times to append *x*.
  /// @returns `true` on success and `false` if the bitmap is full, i.e., has
  ///          `std::numeric_limits<size_t>::max() - 1` elements.
  bool push_back(value_type x, size_t n = 1) {
    return coder_.encode(order(binner_type::bin(x)), n);
  }

  /// Aritifically increases the bitmap size, i.e., the number of rows.
  /// @param n The number of rows to increase the bitmap by.
  /// @returns `true` on success and `false` if there is not enough space.
  bool stretch(size_t n) {
    return coder_.stretch(n);
  }

  /// Appends the contents of another bitmap to this one.
  /// @param other The other bitmap.
  /// @returns `true` on success.
  bool append(bitmap const& other) {
    return coder_.append(other.coder_);
  }

  /// Retrieves a bitstream of a given value with respect to a given operator.
  /// @param op The relational operator to use for looking up *x*.
  /// @param x The value to find the bitstream for.
  /// @returns The bitstream for all values *v* where *op(v,x)* is `true`.
  bitstream_type lookup(relational_operator op, value_type x) const {
    return coder_.decode(op, order(binner_type::bin(x)));
  }

  /// Retrieves the bitmap size.
  /// @returns The number of elements/rows contained in the bitmap.
  uint64_t size() const {
    return coder_.rows();
  }

  /// Checks whether the bitmap is empty.
  /// @returns `true` *iff* the bitmap has 0 entries.
  bool empty() const {
    return size() == 0;
  }

  /// Accesses the underlying coder of the bitmap.
  /// @returns The coder of this bitmap.
  coder_type const& coder() const {
    return coder_;
  }

private:
  template <typename U, typename B>
  using is_shiftable =
    std::integral_constant<
      bool,
      (detail::is_precision_binner<B>{} || detail::is_decimal_binner<B>{})
        && std::is_floating_point<U>{}
    >;

  template <typename U, typename B = binner_type>
  static auto order(U x)
    -> std::enable_if_t<is_shiftable<U, B>{}, detail::ordered_type<U>> {
    return detail::order(x) >> (52 - B::digits2);
  }

  template <typename U, typename B = binner_type>
  static auto order(U x)
    -> std::enable_if_t<! is_shiftable<U, B>{}, detail::ordered_type<T>> {
    return detail::order(x);
  }

  coder_type coder_;
};

} // namespace vast

#endif
