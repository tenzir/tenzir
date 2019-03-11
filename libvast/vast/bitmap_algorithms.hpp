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

#include <algorithm>
#include <iterator>
#include <queue>
#include <type_traits>

#include <caf/error.hpp>

#include "vast/aliases.hpp"
#include "vast/bits.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/range.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/optional.hpp"

namespace vast {

class bitmap;

namespace detail {

template <class T, class U>
struct eval_result_type {
  using type = std::conditional_t<std::is_same_v<T, U>, T, bitmap>;
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
binary_eval(const LHS& lhs, const RHS& rhs, Operation op) {
  using result_type = detail::eval_result_type_t<LHS, RHS>;
  using lhs_bits_type = typename LHS::bits_type;
  using rhs_bits_type = typename RHS::bits_type;
  using result_bits_type = typename result_type::bits_type;
  static_assert(
    detail::are_same_v<lhs_bits_type, rhs_bits_type, result_bits_type>,
    "LHS, RHS, and result bitmaps must have same wod type");
  // Initialize.
  result_type result;
  auto lhs_range = bit_range(lhs);
  auto lhs_begin = lhs_range.begin();
  auto lhs_end = lhs_range.end();
  auto rhs_range = bit_range(rhs);
  auto rhs_begin = rhs_range.begin();
  auto rhs_end = rhs_range.end();
  auto lhs_bits = lhs.empty() ? lhs_bits_type{} : *lhs_begin++;
  auto rhs_bits = rhs.empty() ? rhs_bits_type{} : *rhs_begin++;
  // Iterate.
  while (!lhs_bits.empty() && !rhs_bits.empty()) {
    auto data = op(lhs_bits.data(), rhs_bits.data());
    if (lhs_bits.is_run() && !rhs_bits.is_run()) {
      result.append(result_bits_type{data, rhs_bits.size()});
      lhs_bits = drop(lhs_bits, rhs_bits.size());
      rhs_bits = {};
    } else if (!lhs_bits.is_run() && rhs_bits.is_run()) {
      result.append(result_bits_type{data, lhs_bits.size()});
      rhs_bits = drop(rhs_bits, lhs_bits.size());
      lhs_bits = {};
    } else {
      auto min = std::min(lhs_bits.size(), rhs_bits.size());
      result.append(result_bits_type{data, min});
      lhs_bits = drop(lhs_bits, min);
      rhs_bits = drop(rhs_bits, min);
    }
    // Get the next sequence if we exhausted one.
    if (lhs_bits.empty()) {
      if (lhs_begin != lhs_end) {
        lhs_bits = *lhs_begin++;
        VAST_ASSERT(!lhs_bits.empty());
      }
    }
    if (rhs_bits.empty()) {
      if (rhs_begin != rhs_end) {
        rhs_bits = *rhs_begin++;
        VAST_ASSERT(!rhs_bits.empty());
      }
    }
  }
  VAST_ASSERT(result.size() <= std::max(lhs.size(), rhs.size()));
  // Fill the remaining bits, either with zeros or with the longer bitmap. If
  // we woudn't fill up the bitmap, we would end up with a shorter bitmap that
  // doesn't reflect the true result size.
  if constexpr (FillLHS) {
    if (!lhs_bits.empty())
      result.append(lhs_bits);
    while (lhs_begin != lhs_end)
      result.append(*lhs_begin++);
  }
  if constexpr (FillRHS) {
    if (!rhs_bits.empty())
      result.append(rhs_bits);
    while (rhs_begin != rhs_end)
      result.append(*rhs_begin++);
  }
  auto max_size = std::max(lhs.size(), rhs.size());
  VAST_ASSERT(max_size >= result.size());
  result.append(false, max_size - result.size());
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
    explicit element(const bitmap_type* bm) : bitmap{bm} {
    }
    explicit element(bitmap_type&& bm)
      : data{std::make_shared<bitmap_type>(std::move(bm))},
        bitmap{data.get()} {
    }
    std::shared_ptr<bitmap_type> data;
    const bitmap_type* bitmap;
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
auto binary_and(const LHS& lhs, const RHS& rhs) {
  auto op = [](auto x, auto y) { return x & y; };
  return binary_eval<false, false>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_or(const LHS& lhs, const RHS& rhs) {
  auto op = [](auto x, auto y) { return x | y; };
  return binary_eval<true, true>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_xor(const LHS& lhs, const RHS& rhs) {
  auto op = [](auto x, auto y) { return x ^ y; };
  return binary_eval<true, true>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_nand(const LHS& lhs, const RHS& rhs) {
  auto op = [](auto x, auto y) { return x & ~y; };
  return binary_eval<true, false>(lhs, rhs, op);
}

template <class LHS, class RHS>
auto binary_nor(const LHS& lhs, const RHS& rhs) {
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
/// @pre `i < bm.size()`
template <bool Bit = true, class Bitmap>
typename Bitmap::size_type
rank(const Bitmap& bm, typename Bitmap::size_type i) {
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
typename Bitmap::size_type rank(const Bitmap& bm) {
  return bm.empty() ? 0 : rank<Bit>(bm, bm.size() - 1);
}

/// Computes the position of the i-th occurrence of a bit.
/// @tparam Bit the bit value to locate.
/// @param bm The bitmap to select from.
/// @param i The position of the *i*-th occurrence of *Bit* in *bm*.
///          If `i == -1`, then select the last occurrence of *Bit*.
/// @pre `i > 0`
/// @relates select_range frame
template <bool Bit = true, class Bitmap>
typename Bitmap::size_type
select(const Bitmap& bm, typename Bitmap::size_type i) {
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

/// A range over a Bitmap with various ways to move forward.
template <class BitRange>
class bitwise_range : public detail::range_facade<bitwise_range<BitRange>> {
public:
  using bits_type = std::decay_t<decltype(std::declval<BitRange>().get())>;
  using size_type = typename bits_type::size_type;
  static inline size_type npos = bits_type::npos;

  /// Constructs an ID range from a bit range.
  /// @param rng The bit range.
  bitwise_range(BitRange rng)
    : rng_{std::move(rng)},
      i_{rng_.done() ? npos : 0} {
    // nop
  }

  // -- range introspection ---------------------------------------------------

  /// @returns The current bit sequence.
  /// @pre `!done()`
  const bits_type& bits() const {
    VAST_ASSERT(!done());
    return rng_.get();
  }

  /// @returns The current position in the range.
  /// @pre `!done()`
  id offset() const {
    VAST_ASSERT(!done());
    return n_ + i_;
  }

  /// @returns The bit value at the current position.
  /// @pre `!done()`
  bool value() const {
    VAST_ASSERT(!done());
    return bits()[i_];
  }

  // -- range API -------------------------------------------------------------

  /// Retrieves the current position in the range.
  /// @pre `!done()`
  size_type get() const {
    VAST_ASSERT(!done());
    return offset();
  }

  /// @returns `true` if the range is done.
  bool done() const {
    return rng_.done() && i_ == npos;
  }

  /// Advances to the next bit in the range.
  void next() {
    VAST_ASSERT(!done());
    ++i_;
    if (i_ == bits().size()) {
      n_ += bits().size();
      rng_.next();
      i_ = rng_.done() ? npos : 0;
    }
  }

  // -- explicit range control ------------------------------------------------

  /// Moves the range forward by *k* bits from the current position.
  /// @param k The number of bits to seek forward.
  /// @pre `!done() && k > 0`
  void next(size_type k) {
    VAST_ASSERT(k > 0);
    VAST_ASSERT(i_ != npos);
    VAST_ASSERT(!bits().empty());
    auto remaining_bits = bits().size() - i_ - 1;
    if (k <= remaining_bits) {
      i_ += k - 1;
      next();
      return;
    }
    for (k -= remaining_bits, i_ = npos, n_ += bits().size(), rng_.next();
         !rng_.done() && k > 0;
         k -= bits().size(), n_ += bits().size(), rng_.next()) {
      if (k <= bits().size()) {
        i_ = k - 1;
        return;
      }
    }
  }

  /// Moves to the range to the next bit of a given value.
  /// @tparam Bit The bit value to move forward to.
  template <bool Bit = true>
  void select() {
    VAST_ASSERT(i_ != npos);
    i_ = find_next<Bit>(bits(), i_);
    while (i_ == npos) {
      n_ += bits().size();
      rng_.next();
      if (rng_.done())
        return;
      i_ = find_first<Bit>(bits());
    }
  }

  /// Moves the range forward by *k* bits having a given value. The effect of
  /// this function is equivalent to *k* invocations of `next<Bit>()`.
  /// @tparam Bit The bit value to move forward with.
  /// @param k The number bits of value *Bit* to move forward.
  template <bool Bit = true>
  void select(size_type k) {
    VAST_ASSERT(k > 0);
    VAST_ASSERT(i_ != npos);
    using vast::select;
    auto prev = rank<Bit>(bits(), i_);
    auto remaining = rank<Bit>(bits()) - prev;
    if (k <= remaining) {
      i_ = select<Bit>(bits(), prev + k);
      VAST_ASSERT(i_ != npos);
      return;
    }
    for (k -= remaining, i_ = npos, n_ += bits().size(), rng_.next();
         !rng_.done();
         n_ += bits().size(), rng_.next()) {
      VAST_ASSERT(k > 0);
      if (k > bits().size()) {
        k -= rank<Bit>(bits());
      } else {
        i_ = select<Bit>(bits(), k);
        if (i_ != npos)
          break;
      }
    }
  }

  /// Selects the next bit of a given value starting at given position.
  /// @param i The position to start  where to start the call to select().
  /// @pre `!done() && x >= offset()`
  template <bool Bit = true>
  void select_from(id x) {
    VAST_ASSERT(!done());
    VAST_ASSERT(x >= offset());
    if (x > offset()) {
      next(x - offset());
      if (done())
        return;
    }
    if (value() != Bit)
      select<Bit>();
  }

private:
  BitRange rng_;
  size_type i_ = npos;
  id n_ = 0;
};

/// Creates a bitwise_range from a bitmap.
template <class Bitmap>
auto each(Bitmap&& xs) {
  return bitwise_range{bit_range(xs)};
}

/// A higher-order range that takes a bit-sequence range and transforms it into
/// range of 1-bits. In ther words, this range provides an incremental
/// interface to the one-shot algorithm that ::select computes.
/// @relates select frame
template <bool Bit, class BitRange>
class select_range : public detail::range_facade<select_range<Bit, BitRange>> {
public:
  select_range(BitRange rng) : rng_{std::move(rng)} {
    if (!rng_.done() && rng_.bits()[0] != Bit)
      next();
  }

  auto get() const {
    return rng_.get();
  }

  bool done() const {
    return rng_.done();
  }

  void next() {
    rng_.template select<Bit>();
  }

  void next_from(id x) {
    rng_.template select_from<Bit>(x);
  }


private:
  bitwise_range<BitRange> rng_;
};

/// Creates a select_range over an ID sequence.
/// @param ids The IDs.
/// @returns A select_range for *ids*.
/// @relates select_range frame
template <bool Bit = true, class IDs>
auto select(const IDs& ids) {
  using range_type = decltype(bit_range(ids));
  return select_range<Bit, range_type>(bit_range(ids));
}

/// Traverses the 1-bits of a bitmap in conjunction with an iterator range that
/// represents half-open ID intervals.
/// @param bm The ID sequence to *select*.
/// @param begin An iterator to the beginning of the other range.
/// @param end An iterator to the end of the other range.
/// @param f A function that transforms *begin* into a half-open interval of IDs
///          *[x, y)* where *x* is the first and *y* one past the last ID.
/// @param g A function the performs a user-defined action if the current range
///          values falls into *(x, y)*, where `(x, y) = f(*begin)` .
/// @pre The range delimited by *begin* and *end* must be sorted in ascending
///      order.
template <class Bitmap, class Iterator, class F, class G>
caf::error select_with(const Bitmap& bm, Iterator begin, Iterator end, F f, G g) {
  auto pred = [&](const auto& x, auto y) { return f(x).second <= y; };
  auto lower_bound = [&](Iterator first, Iterator last, auto x) {
    return std::lower_bound(first, last, x, pred);
  };
  for (auto rng = select(bm);
       rng && begin != end;
       begin = lower_bound(begin, end, rng.get())) {
    // Get the current ID interval.
    auto [first, last] = f(*begin);
    // Make the ID range catch up if it's behind.
    if (rng.get() < first) {
      rng.next_from(first);
      if (!rng)
        break;
    }
    if (rng.get() >= first && rng.get() < last) {
      // If the next ID falls in the current slice, we invoke the processing
      // function and move forward.
      if (auto error = g(*begin))
        return error;
      rng.next_from(last);
      if (!rng)
        break;
    }
  }
  return caf::none;
}

/// Computes the *frame* of a bitmap, i.e., the interval *[a,b]* with *a* being
/// the first and *b* the last position of a particular bit value.
/// @tparam Bit the bit value to locate.
/// @param bm The bitmap to select from.
/// @returns The frame of *bm*.
/// @relates select
template <bool Bit = true, class Bitmap>
auto frame(const Bitmap& bm) {
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
bool any(const Bitmap& bm) {
  if constexpr (Bit) {
    for (auto b : bit_range(bm))
      if (b.data())
        return true;
  } else {
    using word_type = typename Bitmap::word_type;
    for (auto b : bit_range(bm)) {
      auto x = b.data();
      if (b.size() <= word_type::width)
        x |= word_type::msb_fill(word_type::width - b.size());
      if (x != word_type::all)
        return true;
    }
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
auto all(const Bitmap& bm) {
  if (bm.empty())
    return false;
  return !any<!Bit>(bm);
}

} // namespace vast
