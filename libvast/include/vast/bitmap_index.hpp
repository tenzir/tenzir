//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/base.hpp"
#include "vast/binner.hpp"
#include "vast/coder.hpp"
#include "vast/detail/order.hpp"
#include "vast/fbs/coder.hpp"

#include <type_traits>

namespace vast {

class bitmap;

/// An associative array which maps arithmetic values to [bitmaps](@ref bitmap).
/// @tparam T The value type for append and lookup operation.
/// @tparam Base The base determining the value decomposition
/// @tparam Coder The encoding/decoding policy.
/// @tparam Binner The pre-processing policy to perform on values.
template <class T, class Coder = multi_level_coder<range_coder<bitmap>>,
          class Binner = identity_binner>
class bitmap_index
  : detail::equality_comparable<bitmap_index<T, Coder, Binner>> {
  static_assert(!std::is_same<T, bool>{} || is_singleton_coder<Coder>{},
                "boolean bitmap index requires singleton coder");

public:
  using value_type = T;
  using coder_type = Coder;
  using binner_type = Binner;
  using bitmap_type = typename coder_type::bitmap_type;
  using size_type = typename coder_type::size_type;

  bitmap_index() = default;

  template <class... Ts>
    requires(std::is_constructible_v<coder_type, Ts...>)
  explicit bitmap_index(Ts&&... xs) : coder_(std::forward<Ts>(xs)...) {
    // nop
  }

  /// Appends a value to the bitmap index.
  /// @param x The value to append.
  void append(value_type x) {
    append(x, 1);
  }

  /// Appends one or more instances of value to the bitmap index.
  /// @param x The value to append.
  /// @param n The number of times to append *x*.
  void append(value_type x, size_type n) {
    coder_.encode(transform(binner_type::bin(x)), n);
  }

  /// Appends the contents of another bitmap index to this one.
  /// @param other The other bitmap index.
  void append(const bitmap_index& other) {
    coder_.append(other.coder_);
  }

  /// Instructs the coder to add undefined values for the sake of increasing
  /// the number of elements.
  /// @param n The number of elements to skip.
  void skip(size_type n) {
    coder_.skip(n);
  }

  /// Retrieves a bitmap of a given value with respect to a given operator.
  /// @param op The relational operator to use for looking up *x*.
  /// @param x The value to find the bitmap for.
  /// @returns The bitmap for all values *v* where *op(v,x)* is `true`.
  [[nodiscard]] bitmap_type lookup(relational_operator op, value_type x) const {
    auto binned = binner_type::bin(x);
    // In case the binning causes a loss of precision, the comparison value
    // has to be adjusted by 1. E.g. a query for `dat > 1.1` will be
    // transformed to `dat > 1` by the binner, which would result in a loss
    // of the value range between 1.0 and 2.0. False positives are filtered
    // out in the candidate check at a later stage.
    if constexpr (!std::is_same_v<binner_type, identity_binner>) {
      if (op == relational_operator::greater)
        --binned;
      if (op == relational_operator::less)
        ++binned;
    }
    return coder_.decode(op, transform(binned));
  }

  /// Retrieves the bitmap index size.
  /// @returns The number of elements/rows contained in the bitmap index.
  [[nodiscard]] size_type size() const {
    return coder_.size();
  }

  /// Retrieves the bitmap index memory usage.
  /// @returns The number of bytes occupied by this instance.
  [[nodiscard]] size_type memusage() const {
    return coder_.memusage();
  }

  /// Checks whether the bitmap index is empty.
  /// @returns `true` *iff* the bitmap index has 0 entries.
  [[nodiscard]] bool empty() const {
    return size() == 0;
  }

  /// Accesses the underlying coder of the bitmap index.
  /// @returns The coder of this bitmap index.
  [[nodiscard]] const coder_type& coder() const {
    return coder_;
  }

  friend bool operator==(const bitmap_index& x, const bitmap_index& y) {
    return x.coder_ == y.coder_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, bitmap_index& bmi) {
    return f.apply(bmi.coder_);
  }

  friend flatbuffers::Offset<fbs::BitmapIndex>
  pack(flatbuffers::FlatBufferBuilder& builder, const bitmap_index& value) {
    constexpr auto coder_type
      = is_singleton_coder<Coder>::value ? fbs::coder::Coder::singleton
        : std::disjunction_v<is_equality_coder<Coder>, is_range_coder<Coder>,
                             is_bitslice_coder<Coder>>
          ? fbs::coder::Coder::vector
          : fbs::coder::Coder::multi_level;
    const auto coder_offset = fbs::CreateCoder(
      builder, coder_type, pack(builder, value.coder_).Union());
    return fbs::CreateBitmapIndex(builder, coder_offset);
  }

  friend caf::error unpack(const fbs::BitmapIndex& from, bitmap_index& to) {
    using concrete_coder_type = std::conditional_t<
      is_singleton_coder<Coder>::value, fbs::coder::SingletonCoder,
      std::conditional_t<
        std::disjunction_v<is_equality_coder<Coder>, is_range_coder<Coder>,
                           is_bitslice_coder<Coder>>,
        fbs::coder::VectorCoder, fbs::coder::MultiLevelCoder>>;
    if (const auto* from_concrete
        = from.coder()->coder_as<concrete_coder_type>())
      return unpack(*from_concrete, to.coder_);
    return caf::make_error(ec::logic_error, "invalid vast.fbs.BitmapIndex "
                                            "coder type");
  }

private:
  template <class U, class B>
  static constexpr bool shiftable
    = (detail::is_precision_binner<B>{} || detail::is_decimal_binner<B>{})
      && std::is_floating_point_v<U>;

  template <class U, class B = binner_type>
  static auto transform(U x) -> detail::ordered_type<U> {
    if constexpr (shiftable<U, B>)
      return detail::order(x) >> (52 - B::digits2);
    else
      return detail::order(x);
  }

  coder_type coder_;
};

} // namespace vast
