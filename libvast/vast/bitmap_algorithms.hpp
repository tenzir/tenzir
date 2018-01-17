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

#ifndef VAST_BITMAP_ALGORITHMS_HPP
#define VAST_BITMAP_ALGORITHMS_HPP

#include <algorithm>
#include <iterator>
#include <queue>
#include <type_traits>

#include "vast/aliases.hpp"
#include "vast/bits.hpp"
#include "vast/optional.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/range.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {

class bitmap;

namespace detail {

template <class T, class U>
struct eval_result_type {
  using type = std::conditional_t<std::is_same<T, U>::value, T, bitmap>;
};

template <class T, class U>
using eval_result_type_t = typename eval_result_type<T, U>::type;

} // namespace detail

/// Applies a bitwise operation on two immutable bitmaps, writing the result
/// into a new bitmap.
/// @tparam FillLHS A boolean flag that controls the algorithm behavior after
///                 one sequence has reached its end. If `true`, the algorithm
///                 will append the remaining bits of *lhs* to the result iff
///                 *lhs* is the longer bitmap. If `false`, the algorithm
///                 returns the result after the first sequence has reached an
///                 end.
/// @tparam FillRHS The same as *fill_lhs*, except that it concerns *rhs*.
/// @param lhs The LHS of the operation.
/// @param rhs The RHS of the operation
/// @param op The bitwise operation as block-wise lambda, e.g., for XOR:
///
///     [](auto lhs, auto rhs) { return lhs ^ rhs; }
///
/// @returns The result of a bitwise operation between *lhs* and *rhs*
/// according to *op*.
template <bool FillLHS, bool FillRHS, class LHS, class RHS, class Operation>
detail::eval_result_type_t<LHS, RHS>
binary_eval(LHS const& lhs, RHS const& rhs, Operation op) {
  using result_type = detail::eval_result_type_t<LHS, RHS>;
  using word_type = typename result_type::word_type;
  static_assert(
    detail::are_same<
      typename LHS::word_type::value_type,
      typename RHS::word_type::value_type,
      typename result_type::word_type::value_type
    >::value,
    "LHS, RHS, and result bitmaps must have same block type");
  // TODO: figure out whether we still need the notion of a "fill," i.e., a
  // homogeneous sequence greater-than-or-equal to the word size, or whether
  // we can operate on the bit sequences directly, possibly leading to
  // simplifications.
  auto is_fill = [](auto x) {
    return x->homogeneous() && x->size() >= word_type::width;
  };
  result_type result;
  // Initialize LHS.
  auto lhs_range = bit_range(lhs);
  auto lhs_begin = lhs_range.begin();
  auto lhs_end = lhs_range.end();
  // Initialize RHS.
  auto rhs_range = bit_range(rhs);
  auto rhs_begin = rhs_range.begin();
  auto rhs_end = rhs_range.end();
  // Iterate.
  uint64_t lhs_bits = lhs.empty() ? 0 : lhs_begin->size();
  uint64_t rhs_bits = rhs.empty() ? 0 : rhs_begin->size();
  while (lhs_begin != lhs_end && rhs_begin != rhs_end) {
    auto block = op(lhs_begin->data(), rhs_begin->data());
    if (is_fill(lhs_begin) && is_fill(rhs_begin)) {
      VAST_ASSERT(word_type::all_or_none(block));
      auto min_bits = std::min(lhs_bits, rhs_bits);
      result.append_bits(block, min_bits);
      lhs_bits -= min_bits;
      rhs_bits -= min_bits;
    } else if (is_fill(lhs_begin)) {
      VAST_ASSERT(rhs_bits > 0);
      VAST_ASSERT(rhs_bits <= word_type::width);
      result.append_block(block);
      lhs_bits -= word_type::width;
      rhs_bits = 0;
    } else if (is_fill(rhs_begin)) {
      VAST_ASSERT(lhs_bits > 0);
      VAST_ASSERT(lhs_bits <= word_type::width);
      result.append_block(block);
      rhs_bits -= word_type::width;
      lhs_bits = 0;
    } else {
      result.append_block(block, std::max(lhs_bits, rhs_bits));
      lhs_bits = rhs_bits = 0;
    }
    if (lhs_bits == 0 && ++lhs_begin != lhs_end)
      lhs_bits = lhs_begin->size();
    if (rhs_bits == 0 && ++rhs_begin != rhs_end)
      rhs_bits = rhs_begin->size();
  }
  // Fill the remaining bits, either with zeros or with the longer bitmap. If
  // we woudn't fill up the bitmap, we would end up with a shorter bitmap that
  // doesn't reflect the true result size.
  if (!FillLHS && !FillLHS) {
    auto max_size = std::max(lhs.size(), rhs.size());
    VAST_ASSERT(max_size >= result.size());
    result.append_bits(false, max_size - result.size());
  } else {
    if (FillLHS) {
      while (lhs_begin != lhs_end) {
        if (is_fill(lhs_begin))
          result.append_bits(lhs_begin->data(), lhs_bits);
        else
          result.append_block(lhs_begin->data(), lhs_begin->size());
        ++lhs_begin;
        if (lhs_begin != lhs_end)
          lhs_bits = lhs_begin->size();
      }
    }
    if (FillRHS) {
      while (rhs_begin != rhs_end) {
        if (is_fill(rhs_begin))
          result.append_bits(rhs_begin->data(), rhs_bits);
        else
          result.append_block(rhs_begin->data(), rhs_begin->size());
        ++rhs_begin;
        if (rhs_begin != rhs_end)
          rhs_bits = rhs_begin->size();
      }
    }
  }
  return result;
}

/// Evaluates a binary operation over multiple bitmaps.
/// @param begin The beginning of the bitmap range.
/// @param end The end of the bitmap range.
/// @param op A binary bitwise operation to execute over the given bitmaps.
/// @returns The application of *op* over the bitmaps *[begin,end)*.
/// @note This algorithm is "Option 3" described in setion 5 in Wu et al.'s
///       2004 paper titled *On the Performance of Bitmap Indices for
///       High-Cardinality Attributes*.
template <class Iterator, class Operation>
auto nary_eval(Iterator begin, Iterator end, Operation op) {
  using bitmap_type = std::decay_t<decltype(*begin)>;
  // Exposes a pointer to represent either a non-owned bitmap from the input
  // sequence or an intermediary result.
  struct element {
    explicit element(bitmap_type const* bm) : bitmap{bm} {
    }
    explicit element(bitmap_type&& bm)
      : data{std::make_shared<bitmap_type>(std::move(bm))},
        bitmap{data.get()} {
    }
    std::shared_ptr<bitmap_type> data;
    bitmap_type const* bitmap;
  };
  auto cmp = [](auto& lhs, auto& rhs) {
    // TODO: instead of using the bitmap size, we should consider whether
    // the rank yields better performance.
    return lhs.bitmap->size() > rhs.bitmap->size();
  };
  std::priority_queue<element, std::vector<element>, decltype(cmp)> queue{cmp};
  for (; begin != end; ++begin)
    queue.emplace(&*begin);
  // Evaluate bitmaps.
  while (!queue.empty()) {
    auto lhs = queue.top();
    queue.pop();
    if (queue.empty())
      // When our input sequence consists of a single bitmap, we end up with an
      // element that has no data. Otherwise we would have had a least one
      // intermediary result, which would be stored as data.
      return lhs.data ? std::move(*lhs.data) : *lhs.bitmap;
    auto rhs = queue.top();
    queue.pop();
    queue.emplace(op(*lhs.bitmap, *rhs.bitmap));
  }
  return bitmap_type{};
}

template <class LHS, class RHS>
auto binary_and(LHS const& lhs, RHS const& rhs) {
  auto op = [](auto x, auto y) { return x & y; };
  return binary_eval<false, false>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_or(LHS const& lhs, RHS const& rhs) {
  auto op = [](auto x, auto y) { return x | y; };
  return binary_eval<true, true>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_xor(LHS const& lhs, RHS const& rhs) {
  auto op = [](auto x, auto y) { return x ^ y; };
  return binary_eval<true, true>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_nand(LHS const& lhs, RHS const& rhs) {
  auto op = [](auto x, auto y) { return x & ~y; };
  return binary_eval<true, false>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_nor(LHS const& lhs, RHS const& rhs) {
  auto op = [](auto x, auto y) { return x | ~y; };
  return binary_eval<true, true>(lhs, rhs, op);
}

template <class Iterator>
auto nary_and(Iterator begin, Iterator end) {
  auto op = [](auto x, auto y) { return x & y; };
  return nary_eval(begin, end, op);
}

template <class Iterator>
auto nary_or(Iterator begin, Iterator end) {
  auto op = [](auto x, auto y) { return x | y; };
  return nary_eval(begin, end, op);
}

template <class Iterator>
auto nary_xor(Iterator begin, Iterator end) {
  auto op = [](auto x, auto y) { return x ^ y; };
  return nary_eval(begin, end, op);
}

/// Computes the *rank* of a Bitmap, i.e., the number of occurrences of a bit
/// value in *B[0,i]*.
/// @tparam Bit The bit value to count.
/// @param bm The bitmap whose rank to compute.
/// @param i The offset where to end counting.
/// @returns The population count of *bm* up to and including position *i*.
/// @pre `i > 0 && i < bm.size()`
template <bool Bit = true, class Bitmap>
typename Bitmap::size_type
rank(Bitmap const& bm, typename Bitmap::size_type i) {
  VAST_ASSERT(i > 0);
  VAST_ASSERT(i < bm.size());
  auto result = typename Bitmap::size_type{0};
  auto n = typename Bitmap::size_type{0};
  for (auto b : bit_range(bm)) {
    if (i >= n && i < n + b.size())
      return result + rank<Bit>(b, i - n);
    result += Bit ? rank<1>(b) : b.size() - rank<1>(b);
    n += b.size();
  }
  return result;
}

/// Computes the *rank* of a Bitmap, i.e., the number of occurrences of a bit.
/// @tparam Bit The bit value to count.
/// @param bm The bitmap whose rank to compute.
/// @returns The population count of *bm*.
template <bool Bit = true, class Bitmap>
typename Bitmap::size_type rank(Bitmap const& bm) {
  return bm.empty() ? 0 : rank<Bit>(bm, bm.size() - 1);
}

/// Computes the position of the i-th occurrence of a bit.
/// @tparam Bit the bit value to locate.
/// @param bm The bitmap to select from.
/// @param i The position of the *i*-th occurrence of *Bit* in *bm*.
///          If `i == -1`, then select the last occurrence of *Bit*.
/// @pre `i > 0`
/// @relates select_range span
template <bool Bit = true, class Bitmap>
typename Bitmap::size_type
select(Bitmap const& bm, typename Bitmap::size_type i) {
  VAST_ASSERT(i > 0);
  auto rnk = typename Bitmap::size_type{0};
  auto n = typename Bitmap::size_type{0};
  if (i == Bitmap::word_type::npos) {
    auto last = Bitmap::word_type::npos;
    for (auto b : bit_range(bm)) {
      auto l = find_last(b);
      if (l != Bitmap::word_type::npos)
        last = n + l;
      n += b.size();
    }
    return last;
  }
  for (auto b : bit_range(bm)) {
    auto count = Bit ? rank<1>(b) : b.size() - rank<1>(b);
    if (rnk + count >= i)
      return n + select<Bit>(b, i - rnk); // Last sequence.
    rnk += count;
    n += b.size();
  }
  return Bitmap::word_type::npos;
}

/// A higher-order range that takes a bit-sequence range and transforms it into
/// range of 1-bits. In ther words, this range provides an incremental
/// interface to the one-shot algorithm that ::select computes.
/// @relates select span
template <class BitRange>
class select_range : public detail::range_facade<select_range<BitRange>> {
public:
  using bits_type = std::decay_t<decltype(std::declval<BitRange>().get())>;
  using size_type = typename bits_type::size_type;
  using word_type = typename bits_type::word_type;

  select_range(BitRange rng) : rng_{rng} {
    if (!rng_.done()) {
      i_ = find_first(rng_.get());
      scan();
    }
  }

  size_type get() const {
    VAST_ASSERT(i_ != word_type::npos);
    return n_ + i_;
  }

  void next() {
    VAST_ASSERT(!done());
    i_ = find_next(rng_.get(), i_);
    scan();
  }

  void next(size_type n) {
    auto& bits = rng_.get();
    auto prev = rank(bits, i_);
    auto remaining = rank(bits) - prev;
    if (n <= remaining) {
      i_ = select(bits, prev + n);
      VAST_ASSERT(i_ != word_type::npos);
    } else {
      n -= remaining;
      i_ = word_type::npos;
      n_ += bits.size();
      rng_.next();
      while (rng_) {
        if (n > rng_.get().size()) {
          n -= rank(rng_.get());
        } else {
          i_ = select(rng_.get(), n);
          if (i_ != word_type::npos)
            break;
        }
        n_ += rng_.get().size();
        rng_.next();
      }
    }
  }

  void skip(size_type n) {
    VAST_ASSERT(n > 0);
    VAST_ASSERT(i_ != word_type::npos);
    auto remaining = rng_.get().size() - i_ - 1;
    if (n <= remaining) {
      i_ += n - 1;
      next();
    } else {
      n -= remaining;
      i_ = word_type::npos;
      n_ += rng_.get().size();
      rng_.next();
      while (rng_) {
        if (n > rng_.get().size()) {
          n -= rng_.get().size();
        } else {
          i_ = n - 2;
          next();
          break;
        }
        n_ += rng_.get().size();
        rng_.next();
      }
    }
  }

  bool done() const {
    return rng_.done() && i_ == word_type::npos;
  }

protected:
  void scan() {
    while (i_ == word_type::npos) {
      n_ += rng_.get().size();
      rng_.next();
      if (rng_.done())
        return;
      i_ = find_first(rng_.get());
    }
  }

  BitRange rng_;
  size_type n_ = 0;
  size_type i_ = word_type::npos;
};

/// Lifts a bit range into a ::select_range.
/// @param rng The bit range from which to construct a select range.
/// @returns The select frange for *rng*.
/// @relates select_range span
template <class Bitmap>
auto select(const Bitmap& bm) {
  return select_range<decltype(bit_range(bm))>(bit_range(bm));
}

/// Computes the *span* of a bitmap, i.e., the interval *[a,b]* with *a* being
/// the first and *b* the last position of a particular bit value.
/// @tparam Bit the bit value to locate.
/// @param bm The bitmap to select from.
/// @returns The span of *bm*.
/// @relates select
template <bool Bit = true, class Bitmap>
auto span(Bitmap const& bm) {
  auto result = std::make_pair(Bitmap::word_type::npos,
                               Bitmap::word_type::npos);
  auto n = typename Bitmap::size_type{0};
  auto rng = bit_range(bm);
  auto begin = rng.begin();
  auto end = rng.end();
  // Locate first position.
  for (; begin != end; n += begin->size(), ++begin) {
    auto first = find_first<Bit>(*begin);
    if (first != Bitmap::word_type::npos) {
      result.first = result.second = n + first;
      break;
    }
  }
  for (; begin != end; n += begin->size(), ++begin) {
    auto last = find_last<Bit>(*begin);
    if (last != Bitmap::word_type::npos)
      result.second = n + last;
  }
  return result;
}

/// Tests whether a bitmap has at least one bit of a given type set.
/// @tparam Bit The bit value to to test.
/// @param bm The bitmap to test.
/// @relates all
template <bool Bit = true, class Bitmap>
auto any(Bitmap const& bm) -> std::enable_if_t<Bit, bool> {
  for (auto b : bit_range(bm))
    if (b.data())
      return true;
  return false;
}

template <bool Bit, class Bitmap>
auto any(Bitmap const& bm) -> std::enable_if_t<!Bit, bool> {
  using word_type = typename Bitmap::word_type;
  for (auto b : bit_range(bm)) {
    auto x = b.data();
    if (b.size() <= word_type::width)
      x |= word_type::msb_fill(word_type::width - b.size());
    if (x != word_type::all)
      return true;
  }
  return false;
}

/// Tests whether a bitmap consists of a homogeneous sequence of a particular
/// bit value.
/// @tparam Bit the bit value to test.
/// @param bm The bitmap to test.
/// @returns `true` iff all bits in *bm* have value *Bit*.
/// @relates any
template <bool Bit = true, class Bitmap>
auto all(Bitmap const& bm) {
  if (bm.empty())
    return false;
  return !any<!Bit>(bm);
}

} // namespace vast

#endif
