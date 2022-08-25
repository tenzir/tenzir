//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/base.hpp"
#include "vast/bitmap.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/inspection_common.hpp"
#include "vast/detail/operators.hpp"
#include "vast/error.hpp"
#include "vast/fbs/coder.hpp"
#include "vast/operator.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <type_traits>
#include <vector>

namespace vast {

/// The concept class for bitmap coders. A coder offers two basic primitives:
/// encoding and decoding of (one or more) values into bitmap storage. The
/// decoding step is a function of specific relational operator, as supported
/// by the coder. A coder is an append-only data structure. Users have the
/// ability to control the position/offset where to begin encoding of values.
// template <class Bitmap>
// struct coder {
//   using bitmap_type = Bitmap;
//   using size_type = typename Bitmap::size_type;
//   using value_type = size_t;
//
//   /// Returns the number of bitmaps stored by the coder.
//   size_type bitmap_count() const noexcept;
//
//   /// Accesses individual bitmaps. The implementation may lazily fill a bitmap
//   /// before returning it.
//   /// @returns the bitmap for `x`.
//   Bitmap& bitmap_at(size_t index);
//
//   /// Accesses individual bitmaps. The implementation may lazily fill a bitmap
//   /// before returning it.
//   /// @returns the bitmap for `x`.
//   const Bitmap& bitmap_at(size_t index) const;
//
//   /// Encodes a single values multiple times.
//   /// @tparam An unsigned integral type.
//   /// @param x The value to encode.
//   /// @param n The number of time to add *x*.
//   /// @pre `Bitmap::max_size - size() >= n`
//   void encode(value_type x, size_type n = 1);
//
//   /// Decodes a value under a relational operator.
//   /// @param x The value to decode.
//   /// @param op The relation operator under which to decode *x*.
//   /// @returns The bitmap for lookup *? op x* where *?* represents the value in
//   ///          the coder.
//   Bitmap decode(relational_operator op, value_type x) const;
//
//   /// Instructs the coder to add undefined values for the sake of increasing
//   /// the number of elements.
//   /// @param n The number of elements to skip.
//   void skip(size_type n);
//
//   /// Appends another coder to this instance.
//   /// @param other The coder to append.
//   /// @pre `size() + other.size() < Bitmap::max_size`
//   void append(const coder& other);
//
//   /// Retrieves the number entries in the coder, i.e., the number of rows.
//   /// @returns The size of the coder measured in number of entries.
//   size_type size() const;
//
//   /// Retrieves the amout of memory that is occupied by the coder.
//   /// @returns The size of the coder measured in heap bytes used.
//   [[nodiscard]] size_t memusage() const;
//
//   /// Retrieves the coder-specific bitmap storage.
//   auto& storage() const;
// };

/// A coder that wraps a single bitmap (and can thus only stores 2 values).
template <class Bitmap>
class singleton_coder : detail::equality_comparable<singleton_coder<Bitmap>> {
public:
  using bitmap_type = Bitmap;
  using size_type = typename Bitmap::size_type;
  using value_type = bool;

  [[nodiscard]] size_t bitmap_count() const noexcept {
    return 1;
  }

  bitmap_type& bitmap_at(size_t index) {
    VAST_ASSERT(index == 0);
    return bitmap_;
  }

  [[nodiscard]] const bitmap_type& bitmap_at(size_t index) const {
    VAST_ASSERT(index == 0);
    return bitmap_;
  }

  void encode(value_type x, size_type n = 1) {
    VAST_ASSERT(Bitmap::max_size - size() >= n);
    bitmap_.append_bits(x, n);
  }

  [[nodiscard]] Bitmap decode(relational_operator op, value_type x) const {
    VAST_ASSERT(op == relational_operator::equal
                || op == relational_operator::not_equal);
    auto result = bitmap_;
    if ((x && op == relational_operator::equal)
        || (!x && op == relational_operator::not_equal))
      return result;
    result.flip();
    return result;
  }

  void skip(size_type n) {
    bitmap_.append_bits(0, n);
  }

  void append(const singleton_coder& other) {
    bitmap_.append(other.bitmap_);
  }

  [[nodiscard]] size_type size() const {
    return bitmap_.size();
  }

  [[nodiscard]] size_t memusage() const {
    return bitmap_.memusage();
  }

  [[nodiscard]] const Bitmap& storage() const {
    return bitmap_;
  }

  friend bool operator==(const singleton_coder& x, const singleton_coder& y) {
    return x.bitmap_ == y.bitmap_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, singleton_coder& sc) {
    return f.apply(sc.bitmap_);
  }

  friend flatbuffers::Offset<fbs::coder::SingletonCoder>
  pack(flatbuffers::FlatBufferBuilder& builder, const singleton_coder& value) {
    const auto bitmap_offset = pack(builder, bitmap{value.bitmap_});
    return fbs::coder::CreateSingletonCoder(builder, bitmap_offset);
  }

  friend caf::error
  unpack(const fbs::coder::SingletonCoder& from, singleton_coder& to) {
    if constexpr (std::is_same_v<Bitmap, bitmap>) {
      return unpack(*from.bitmap(), to.bitmap_);
    } else {
      using concrete_bitmap_type = std::conditional_t<
        std::is_same_v<Bitmap, ewah_bitmap>, fbs::bitmap::EWAHBitmap,
        std::conditional_t<std::is_same_v<Bitmap, null_bitmap>,
                           fbs::bitmap::NullBitmap,
                           std::conditional_t<std::is_same_v<Bitmap, wah_bitmap>,
                                              fbs::bitmap::WAHBitmap, void>>>;
      static_assert(!std::is_void_v<concrete_bitmap_type>);
      if (const auto* from_concrete
          = from.bitmap()->bitmap_as<concrete_bitmap_type>())
        return unpack(*from_concrete, to.bitmap_);
      return caf::make_error(
        ec::logic_error, "invalid vast.fbs.coder.SingletonCoder bitmap type");
    }
  }

private:
  Bitmap bitmap_;
};

template <class Bitmap>
class vector_coder : detail::equality_comparable<vector_coder<Bitmap>> {
public:
  using bitmap_type = Bitmap;
  using size_type = typename Bitmap::size_type;
  using value_type = size_t;

  vector_coder() = default;
  vector_coder(const vector_coder&) = delete;
  vector_coder(vector_coder&&) = default;
  vector_coder& operator=(const vector_coder&) = delete;
  vector_coder& operator=(vector_coder&&) = default;
  ~vector_coder() = default;

  explicit vector_coder(size_t n) : bitmaps_(n) {
    // nop
  }

  size_t bitmap_count() const noexcept {
    return bitmaps_.size();
  }

  auto size() const {
    return size_;
  }

  size_t memusage() const {
    size_t acc = 0;
    for (const auto& bitmap : bitmaps_)
      acc += bitmap.memusage();
    return acc;
  }

  auto& storage() const {
    return bitmaps_;
  }

  friend bool operator==(const vector_coder& x, const vector_coder& y) {
    return x.size_ == y.size_ && x.bitmaps_ == y.bitmaps_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, vector_coder& ec) {
    return detail::apply_all(f, ec.size_, ec.bitmaps_);
  }

  friend flatbuffers::Offset<fbs::coder::VectorCoder>
  pack(flatbuffers::FlatBufferBuilder& builder, const vector_coder& value) {
    auto bitmap_offsets = std::vector<flatbuffers::Offset<fbs::Bitmap>>{};
    bitmap_offsets.reserve(value.bitmaps_.size());
    for (const auto& bm : value.bitmaps_)
      bitmap_offsets.emplace_back(pack(builder, bitmap{bm}));
    return fbs::coder::CreateVectorCoderDirect(builder, value.size_,
                                               &bitmap_offsets);
  }

  friend caf::error
  unpack(const fbs::coder::VectorCoder& from, vector_coder& to) {
    to.size_ = from.size();
    to.bitmaps_.clear();
    to.bitmaps_.reserve(from.bitmaps()->size());
    for (const auto* from_bitmap : *from.bitmaps()) {
      if constexpr (std::is_same_v<Bitmap, bitmap>) {
        auto to_bitmap = bitmap{};
        if (auto err = unpack(*from_bitmap, to_bitmap))
          return err;
        to.bitmaps_.emplace_back(std::move(to_bitmap));
      } else {
        using concrete_bitmap_type = std::conditional_t<
          std::is_same_v<Bitmap, ewah_bitmap>, fbs::bitmap::EWAHBitmap,
          std::conditional_t<
            std::is_same_v<Bitmap, null_bitmap>, fbs::bitmap::NullBitmap,
            std::conditional_t<std::is_same_v<Bitmap, wah_bitmap>,
                               fbs::bitmap::WAHBitmap, void>>>;
        static_assert(!std::is_void_v<concrete_bitmap_type>);
        const auto* from_concrete
          = from_bitmap->bitmap_as<concrete_bitmap_type>();
        if (!from_concrete)
          return caf::make_error(
            ec::logic_error, "invalid vast.fbs.coder.VectorCoder bitmap type");
        auto to_concrete = Bitmap{};
        if (auto err = unpack(*from_concrete, to_concrete))
          return err;
        to.bitmaps_.emplace_back(std::move(to_concrete));
      }
    }
    return caf::none;
  }

protected:
  void append(const vector_coder& other, bool bit) {
    VAST_ASSERT(bitmaps_.size() == other.bitmaps_.size());
    for (auto i = 0u; i < bitmaps_.size(); ++i) {
      bitmaps_[i].append_bits(bit, this->size() - bitmaps_[i].size());
      bitmaps_[i].append(other.bitmaps_[i]);
    }
    size_ += other.size_;
  }

  size_type size_ = 0;
  mutable std::vector<Bitmap> bitmaps_ = {};
};

/// Encodes each value in its own bitmap.
template <class Bitmap>
class equality_coder : public vector_coder<Bitmap> {
public:
  using super = vector_coder<Bitmap>;

  using typename super::bitmap_type;
  using typename super::size_type;
  using typename super::value_type;

  using super::super;

  bitmap_type& lazy_bitmap_at(size_t index) const {
    auto& result = this->bitmaps_[index];
    result.append_bits(false, this->size_ - result.size());
    return result;
  }

  bitmap_type& bitmap_at(size_t index) {
    return lazy_bitmap_at(index);
  }

  const bitmap_type& bitmap_at(size_t index) const {
    return lazy_bitmap_at(index);
  }

  void encode(value_type x, size_type n = 1) {
    VAST_ASSERT(Bitmap::max_size - this->size_ >= n);
    VAST_ASSERT(x < this->bitmaps_.size());
    bitmap_at(x).append_bits(true, n);
    this->size_ += n;
  }

  Bitmap decode(relational_operator op, value_type x) const {
    VAST_ASSERT(op == relational_operator::less
                || op == relational_operator::less_equal
                || op == relational_operator::equal
                || op == relational_operator::not_equal
                || op == relational_operator::greater_equal
                || op == relational_operator::greater);
    VAST_ASSERT(x < this->bitmaps_.size());
    switch (op) {
      default:
        return Bitmap{this->size_, false};
      case relational_operator::less: {
        if (x == 0)
          return Bitmap{this->size_, false};
        auto f = this->bitmaps_.begin();
        auto result = nary_or(f, f + x);
        result.append_bits(false, this->size_ - result.size());
        return result;
      }
      case relational_operator::less_equal: {
        auto f = this->bitmaps_.begin();
        auto result = nary_or(f, f + x + 1);
        result.append_bits(false, this->size_ - result.size());
        return result;
      }
      case relational_operator::equal:
      case relational_operator::not_equal: {
        auto result = bitmap_at(x);
        if (op == relational_operator::not_equal)
          result.flip();
        return result;
      }
      case relational_operator::greater_equal: {
        auto result = nary_or(this->bitmaps_.begin() + x, this->bitmaps_.end());
        result.append_bits(false, this->size_ - result.size());
        return result;
      }
      case relational_operator::greater: {
        if (x >= this->bitmaps_.size() - 1)
          return Bitmap{this->size_, false};
        auto f = this->bitmaps_.begin();
        auto l = this->bitmaps_.end();
        auto result = nary_or(f + x + 1, l);
        result.append_bits(false, this->size_ - result.size());
        return result;
      }
    }
  }

  void skip(size_type n) {
    this->size_ += n;
  }

  void append(const equality_coder& other) {
    super::append(other, false);
  }

  friend flatbuffers::Offset<fbs::coder::VectorCoder>
  pack(flatbuffers::FlatBufferBuilder& builder, const equality_coder& value) {
    return pack(builder, static_cast<const vector_coder<Bitmap>&>(value));
  }

  friend caf::error
  unpack(const fbs::coder::VectorCoder& from, equality_coder& to) {
    return unpack(from, static_cast<vector_coder<Bitmap>&>(to));
  }
};

/// Encodes a value according to an inequalty. Given a value *x* and an
/// index *i* in *[0,N)*, all bits are 0 for i < x and 1 for i >= x.
template <class Bitmap>
class range_coder : public vector_coder<Bitmap> {
public:
  using super = vector_coder<Bitmap>;

  using typename super::bitmap_type;
  using typename super::size_type;
  using typename super::value_type;

  using super::super;

  bitmap_type& lazy_bitmap_at(size_t index) const {
    auto& result = this->bitmaps_[index];
    result.append_bits(true, this->size_ - result.size());
    return result;
  }

  bitmap_type& bitmap_at(size_t index) {
    return lazy_bitmap_at(index);
  }

  const bitmap_type& bitmap_at(size_t index) const {
    return lazy_bitmap_at(index);
  }

  void encode(value_type x, size_type n = 1) {
    VAST_ASSERT(Bitmap::max_size - this->size_ >= n);
    VAST_ASSERT(x < this->bitmaps_.size() + 1);
    // Lazy append: we only add 0s until we hit index i of value x. The
    // remaining bitmaps are always 1, by definition of the range coding
    // property i >= x for all i in [0,N).
    for (auto i = 0u; i < x; ++i)
      bitmap_at(i).append_bits(false, n);
    this->size_ += n;
  }

  Bitmap decode(relational_operator op, value_type x) const {
    VAST_ASSERT(op == relational_operator::less
                || op == relational_operator::less_equal
                || op == relational_operator::equal
                || op == relational_operator::not_equal
                || op == relational_operator::greater_equal
                || op == relational_operator::greater);
    VAST_ASSERT(x < this->bitmaps_.size() + 1);
    switch (op) {
      default:
        return Bitmap{this->size_, false};
      case relational_operator::less: {
        if (x == 0)
          return Bitmap{this->size_, false};
        return bitmap_at(x - 1);
      }
      case relational_operator::less_equal: {
        return bitmap_at(x);
      }
      case relational_operator::equal: {
        auto result = bitmap_at(x);
        if (x > 0)
          result &= ~bitmap_at(x - 1);
        return result;
      }
      case relational_operator::not_equal: {
        auto result = ~bitmap_at(x);
        if (x > 0)
          result |= bitmap_at(x - 1);
        return result;
      }
      case relational_operator::greater: {
        return ~bitmap_at(x);
      }
      case relational_operator::greater_equal: {
        if (x == 0)
          return Bitmap{this->size_, true};
        return ~bitmap_at(x - 1);
      }
    }
  }

  void skip(size_type n) {
    this->size_ += n;
  }

  void append(const range_coder& other) {
    super::append(other, true);
  }

  friend flatbuffers::Offset<fbs::coder::VectorCoder>
  pack(flatbuffers::FlatBufferBuilder& builder, const range_coder& value) {
    return pack(builder, static_cast<const vector_coder<Bitmap>&>(value));
  }

  friend caf::error
  unpack(const fbs::coder::VectorCoder& from, range_coder& to) {
    return unpack(from, static_cast<vector_coder<Bitmap>&>(to));
  }
};

/// Maintains one bitmap per *bit* of the value to encode.
/// For example, adding the value 4 appends a 1 to the bitmap for 2^2 and a
/// 0 to to all other bitmaps.
template <class Bitmap>
class bitslice_coder : public vector_coder<Bitmap> {
public:
  using super = vector_coder<Bitmap>;

  using typename super::bitmap_type;
  using typename super::size_type;
  using typename super::value_type;

  using super::super;

  bitmap_type& lazy_bitmap_at(size_t index) const {
    auto& result = this->bitmaps_[index];
    result.append_bits(false, this->size_ - result.size());
    return result;
  }

  bitmap_type& bitmap_at(size_t index) {
    return lazy_bitmap_at(index);
  }

  const bitmap_type& bitmap_at(size_t index) const {
    return lazy_bitmap_at(index);
  }

  void encode(value_type x, size_type n = 1) {
    VAST_ASSERT(Bitmap::max_size - this->size_ >= n);
    for (auto i = 0u; i < this->bitmaps_.size(); ++i)
      bitmap_at(i).append_bits(((x >> i) & 1) == 0, n);
    this->size_ += n;
  }

  // RangeEval-Opt for the special case with uniform base 2.
  Bitmap decode(relational_operator op, value_type x) const {
    switch (op) {
      default:
        break;
      case relational_operator::less:
      case relational_operator::less_equal:
      case relational_operator::greater:
      case relational_operator::greater_equal: {
        if (x == std::numeric_limits<value_type>::min()) {
          if (op == relational_operator::less)
            return Bitmap{this->size_, false};
          else if (op == relational_operator::greater_equal)
            return Bitmap{this->size_, true};
        } else if (op == relational_operator::less
                   || op == relational_operator::greater_equal) {
          --x;
        }
        auto result = x & 1 ? Bitmap{this->size_, true} : this->bitmaps_[0];
        for (auto i = 1u; i < this->bitmaps_.size(); ++i)
          if ((x >> i) & 1)
            result |= this->bitmaps_[i];
          else
            result &= this->bitmaps_[i];
        if (op == relational_operator::greater
            || op == relational_operator::greater_equal
            || op == relational_operator::not_equal)
          result.flip();
        return result;
      }
      case relational_operator::equal:
      case relational_operator::not_equal: {
        auto result = Bitmap{this->size_, true};
        for (auto i = 0u; i < this->bitmaps_.size(); ++i) {
          auto& bm = this->bitmaps_[i];
          result &= (((x >> i) & 1) ? ~bm : bm);
        }
        if (op == relational_operator::not_equal)
          result.flip();
        return result;
      }
      case relational_operator::in:
      case relational_operator::not_in: {
        if (x == 0)
          break;
        x = ~x;
        auto result = Bitmap{this->size_, false};
        for (auto i = 0u; i < this->bitmaps_.size(); ++i)
          if (((x >> i) & 1) == 0)
            result |= this->bitmaps_[i];
        if (op == relational_operator::in)
          result.flip();
        return result;
      }
    }
    return Bitmap{this->size_, false};
  }

  void skip(size_type n) {
    this->size_ += n;
  }

  void append(const bitslice_coder& other) {
    super::append(other, false);
  }

  friend flatbuffers::Offset<fbs::coder::VectorCoder>
  pack(flatbuffers::FlatBufferBuilder& builder, const bitslice_coder& value) {
    return pack(builder, static_cast<const vector_coder<Bitmap>&>(value));
  }

  friend caf::error
  unpack(const fbs::coder::VectorCoder& from, bitslice_coder& to) {
    return unpack(from, static_cast<vector_coder<Bitmap>&>(to));
  }
};

template <class T>
struct is_singleton_coder : std::false_type {};

template <class Bitmap>
struct is_singleton_coder<singleton_coder<Bitmap>> : std::true_type {};

template <class T>
struct is_equality_coder : std::false_type {};

template <class Bitmap>
struct is_equality_coder<equality_coder<Bitmap>> : std::true_type {};

template <class T>
struct is_range_coder : std::false_type {};

template <class Bitmap>
struct is_range_coder<range_coder<Bitmap>> : std::true_type {};

template <class T>
struct is_bitslice_coder : std::false_type {};

template <class Bitmap>
struct is_bitslice_coder<bitslice_coder<Bitmap>> : std::true_type {};

/// A multi-component (or multi-level) coder expresses values as a linear
/// combination according to a base vector. The literature refers to this
/// represenation as *attribute value decomposition*.
template <class Coder>
class multi_level_coder
  : detail::equality_comparable<multi_level_coder<Coder>> {
public:
  using coder_type = Coder;
  using bitmap_type = typename coder_type::bitmap_type;
  using size_type = typename coder_type::size_type;
  using value_type = typename coder_type::value_type;

  multi_level_coder() = default;
  multi_level_coder(const multi_level_coder&) = delete;
  multi_level_coder(multi_level_coder&&) = default;
  multi_level_coder& operator=(const multi_level_coder&) = delete;
  multi_level_coder& operator=(multi_level_coder&&) = default;
  ~multi_level_coder() = default;

  /// Constructs a multi-level coder from a given base.
  /// @param b The base to initialize this coder with.
  explicit multi_level_coder(base b) : base_{std::move(b)} {
    init();
  }

  void encode(value_type x, size_type n = 1) {
    if (xs_.empty())
      init();
    base_.decompose(x, xs_);
    for (auto i = 0u; i < base_.size(); ++i)
      coders_[i].encode(xs_[i], n);
  }

  auto decode(relational_operator op, value_type x) const {
    return coders_.empty() ? bitmap_type{} : decode(coders_, op, x);
  }

  void skip(size_type n) {
    for (auto& x : coders_)
      x.skip(n);
  }

  void append(const multi_level_coder& other) {
    VAST_ASSERT(coders_.size() == other.coders_.size());
    for (auto i = 0u; i < coders_.size(); ++i)
      coders_[i].append(other.coders_[i]);
  }

  size_type size() const {
    return coders_.empty() ? 0 : coders_[0].size();
  }

  size_t memusage() const {
    size_t acc = 0;
    acc += base_.memusage();
    acc += xs_.capacity() * sizeof(value_type);
    for (const auto& coder : coders_)
      acc += coder.memusage();
    return acc;
  }

  auto& storage() const {
    return coders_;
  }

  friend bool
  operator==(const multi_level_coder& x, const multi_level_coder& y) {
    return x.base_ == y.base_ && x.coders_ == y.coders_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, multi_level_coder& mlc) {
    return detail::apply_all(f, mlc.base_, mlc.xs_, mlc.coders_);
  }

  friend flatbuffers::Offset<fbs::coder::MultiLevelCoder>
  pack(flatbuffers::FlatBufferBuilder& builder,
       const multi_level_coder& value) {
    static_assert(sizeof(value_type) <= sizeof(uint64_t),
                  "value_type too large to be normalized to 64 bit");
    auto base_normalized = std::vector<uint64_t>{};
    base_normalized.reserve(value.base_.size());
    for (const auto& base_value : value.base_)
      base_normalized.emplace_back(static_cast<uint64_t>(base_value));
    const auto base_offset
      = fbs::coder::detail::CreateBaseDirect(builder, &base_normalized);
    constexpr auto coder_type
      = is_singleton_coder<Coder>::value ? fbs::coder::Coder::singleton
        : std::disjunction_v<is_equality_coder<Coder>, is_range_coder<Coder>,
                             is_bitslice_coder<Coder>>
          ? fbs::coder::Coder::vector
          : fbs::coder::Coder::multi_level;
    auto coder_offsets = std::vector<flatbuffers::Offset<fbs::Coder>>{};
    coder_offsets.reserve(value.coders_.size());
    for (const auto& coder : value.coders_) {
      const auto coder_offset = pack(builder, coder);
      coder_offsets.emplace_back(
        fbs::CreateCoder(builder, coder_type, coder_offset.Union()));
    }
    const auto coders_offset = builder.CreateVector(coder_offsets);
    return fbs::coder::CreateMultiLevelCoder(builder, base_offset,
                                             coders_offset);
  }

  friend caf::error
  unpack(const fbs::coder::MultiLevelCoder& from, multi_level_coder& to) {
    auto base_values = std::vector<value_type>{};
    base_values.reserve(from.base()->values()->size());
    for (const auto& base_value : *from.base()->values())
      base_values.emplace_back(static_cast<value_type>(base_value));
    to.base_ = vast::base{std::move(base_values)};
    using concrete_coder_type = std::conditional_t<
      is_singleton_coder<Coder>::value, fbs::coder::SingletonCoder,
      std::conditional_t<
        std::disjunction_v<is_equality_coder<Coder>, is_range_coder<Coder>,
                           is_bitslice_coder<Coder>>,
        fbs::coder::VectorCoder, fbs::coder::MultiLevelCoder>>;
    to.coders_.clear();
    to.coders_.reserve(from.coders()->size());
    for (const auto& from_coder : *from.coders()) {
      const auto* from_concrete_coder
        = from_coder->coder_as<concrete_coder_type>();
      if (!from_concrete_coder)
        return caf::make_error(
          ec::logic_error, "invalid vast.fbs.coder.MultiLevelCoder coder type");
      auto to_coder = Coder{};
      if (auto err = unpack(*from_concrete_coder, to_coder))
        return err;
      to.coders_.emplace_back(std::move(to_coder));
    }
    return caf::none;
  }

private:
  void init() {
    VAST_ASSERT(base_.well_defined());
    xs_.resize(base_.size());
    coders_.resize(base_.size());
    init_coders(coders_); // dispatch on coder_type
    VAST_ASSERT(coders_.size() == base_.size());
  }

  // TODO
  // We could further optimze the number of bitmaps per coder: any base b
  // requires only b-1 bitmaps because one can obtain any bitmap through
  // conjunction/disjunction of the others. While this decreases space
  // requirements by a factor of 1/b, it increases query time by b-1.

  void init_coders(std::vector<singleton_coder<bitmap_type>>&) {
    // Nothing to for singleton coders.
  }

  void init_coders(std::vector<range_coder<bitmap_type>>& coders) {
    // For range coders it suffices to use b-1 bitmaps because the last
    // bitmap always consists of all 1s and is hence superfluous.
    for (auto i = 0u; i < base_.size(); ++i)
      coders[i] = range_coder<bitmap_type>{base_[i] - 1};
  }

  template <class C>
  void init_coders(std::vector<C>& coders) {
    // All other multi-bitmap coders use one bitmap per unique value.
    for (auto i = 0u; i < base_.size(); ++i)
      coders[i] = C{base_[i]};
  }

  // Range-Eval-Opt
  auto decode(const std::vector<range_coder<bitmap_type>>& coders,
              relational_operator op, value_type x) const {
    VAST_ASSERT(
      !(op == relational_operator::in || op == relational_operator::not_in));
    // All coders must have the same number of elements.
    auto pred = [n = size()](const auto& c) {
      return c.size() == n;
    };
    VAST_ASSERT(std::all_of(coders.begin(), coders.end(), pred));
    // Check boundaries first.
    if (x == 0) {
      if (op == relational_operator::less) // A < min => false
        return bitmap_type{size(), false};
      else if (op == relational_operator::greater_equal) // A >= min => true
        return bitmap_type{size(), true};
    } else if (op == relational_operator::less
               || op == relational_operator::greater_equal) {
      --x;
    }
    base_.decompose(x, xs_);
    bitmap_type result{size(), true};
    auto get_bitmap = [&](size_t coder_index, size_t bitmap_index) -> auto& {
      return coders[coder_index].bitmap_at(bitmap_index);
    };
    switch (op) {
      default:
        return bitmap_type{size(), false};
      case relational_operator::less:
      case relational_operator::less_equal:
      case relational_operator::greater:
      case relational_operator::greater_equal: {
        if (xs_[0] < base_[0] - 1) // && bitmap != all_ones
          result = get_bitmap(0, xs_[0]);
        for (auto i = 1u; i < base_.size(); ++i) {
          if (xs_[i] != base_[i] - 1) // && bitmap != all_ones
            result &= get_bitmap(i, xs_[i]);
          if (xs_[i] != 0) // && bitmap != all_ones
            result |= get_bitmap(i, xs_[i] - 1);
        }
      } break;
      case relational_operator::equal:
      case relational_operator::not_equal: {
        for (auto i = 0u; i < base_.size(); ++i) {
          if (xs_[i] == 0) // && bitmap != all_ones
            result &= get_bitmap(i, 0);
          else if (xs_[i] == base_[i] - 1)
            result &= ~get_bitmap(i, base_[i] - 2);
          else
            result &= get_bitmap(i, xs_[i]) ^ get_bitmap(i, xs_[i] - 1);
        }
      } break;
    }
    if (op == relational_operator::greater
        || op == relational_operator::greater_equal
        || op == relational_operator::not_equal)
      result.flip();
    return result;
  }

  // If we don't have a range_coder, we only support simple equality queries
  // at this point.
  template <class C>
    requires(is_equality_coder<C>::value || is_bitslice_coder<C>::value)
  auto decode(const std::vector<C>& coders, relational_operator op,
              value_type x) const -> bitmap_type {
    VAST_ASSERT(op == relational_operator::equal
                || op == relational_operator::not_equal);
    base_.decompose(x, xs_);
    auto result = coders[0].decode(relational_operator::equal, xs_[0]);
    for (auto i = 1u; i < base_.size(); ++i)
      result &= coders[i].decode(relational_operator::equal, xs_[i]);
    if (op == relational_operator::not_equal
        || op == relational_operator::not_in)
      result.flip();
    return result;
  }

  base base_ = {};
  mutable std::vector<value_type> xs_ = {};
  std::vector<coder_type> coders_ = {};
};

template <class T>
struct is_multi_level_coder : std::false_type {};

template <class C>
struct is_multi_level_coder<multi_level_coder<C>> : std::true_type {};

} // namespace vast
