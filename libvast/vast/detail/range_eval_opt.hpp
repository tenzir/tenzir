#ifndef VAST_DETAIL_RANGE_EVAL_OPT_HPP
#define VAST_DETAIL_RANGE_EVAL_OPT_HPP

#include <cstdint>

#include <algorithm>
#include <array>
#include <type_traits>

#include "vast/operator.hpp"
#include "vast/detail/decompose.hpp"
#include "vast/util/assert.hpp"

namespace vast {
namespace detail {

/// The *RangeEval-Opt* algorithm from Chee-Yong Chan and Yannis E. Ioannidis.
/// @todo Add optimizations from Ming-Chuan Wu to reduce the number of
///       bitstream scans (and bitwise operations).
/// @tparam Base The according to which to decompose the values.
/// @tparam Coder The coder type.
/// @tparam T The integral type of the value.
/// @tparam N The number of components.
/// @param coders The array of range coders holding the bitstreams.
/// @param op The relational operator evaluate *x* under.
/// @param x The value to evaluate.
template <typename Base, typename Coder, typename T, size_t N>
typename Coder::bitstream_type
range_eval_opt(std::array<Coder, N> const& coders, relational_operator op,
               T x) {
  static_assert(Base::components == N, "one component per base required");
  static_assert(std::is_unsigned<T>{}, "RangeEval-Opt requires unsigned types");
  static_assert(std::is_integral<T>{}, "RangeEval-Opt requires integers");
  auto rows = coders[0].rows();
  // All coders must have the same size.
  auto coder_pred = [=](auto c) { return c.rows() == rows; };
  VAST_ASSERT(std::all_of(coders.begin(), coders.end(), coder_pred));
  // Check boundaries first.
  if (x == std::numeric_limits<T>::min()) {
    if (op == less) // A < min => false
      return {rows, false};
    else if (op == greater_equal) // A >= min => true
      return {rows, true};
  } else if (op == less || op == greater_equal) {
    --x;
  }
  typename Coder::bitstream_type result{rows, true};
  auto xs = decompose(x, Base::values);
  switch (op) {
    default:
      return {rows, false};
    case less:
    case less_equal:
    case greater:
    case greater_equal: {
      if (xs[0] < Base::values[0] - 1) // && bitstream != all_ones
        result = coders[0][xs[0]];
      for (auto i = 1u; i < N; ++i) {
        if (xs[i] != Base::values[i] - 1) // && bitstream != all_ones
          result &= coders[i][xs[i]];
        if (xs[i] != 0) // && bitstream != all_ones
          result |= coders[i][xs[i] - 1];
      }
    } break;
    case equal:
    case not_equal: {
      for (auto i = 0u; i < N; ++i) {
        auto& c = coders[i];
        if (xs[i] == 0) // && bitstream != all_ones
          result &= c[0];
        else if (xs[i] == Base::values[i] - 1)
          result &= ~c[Base::values[i] - 2];
        else
          result &= c[xs[i]] ^ c[xs[i] - 1];
      }
    } break;
  }
  if (op == greater || op == greater_equal || op == not_equal)
    result.flip();
  return result;
}

} // namespace detail
} // namespace vast

#endif
