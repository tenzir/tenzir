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

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

/// A base for value (de)composition.
class base : detail::equality_comparable<base> {
public:
  using value_type = size_t;
  using vector_type = std::vector<value_type>;

  /// Constructs a uniform base with a given value.
  /// @param b The uniform value at all components.
  /// @param n The number of components.
  /// @returns A uniform base of value *b* with *n* components.
  static base uniform(value_type b, size_t n = 0);

  /// Constructs a uniform base with a given value.
  /// @param b The uniform value at all components.
  /// @param n The number of components. If 0, determine the number of
  ///          components automatically based on the cardinality of `T`'s
  ///          domain.
  /// @returns A uniform base of value *b* with *n* components.
  template <int Bits>
  static base uniform(size_t b) {
    static_assert(Bits > 0 && Bits <= 64, "Bits must be in (0, 64]");
    VAST_ASSERT(b > 0);
    size_t n = std::ceil(Bits / std::log2(b));
    return uniform(b, n);
  }

  base() = default;

  explicit base(vector_type xs);
  explicit base(std::initializer_list<value_type> xs);

  /// Checks whether the base has at least one value, and that all values
  /// are >= 2.
  /// @returns `true` iff this base is well-defined.
  bool well_defined() const;

  /// Decomposes a value into a sequence of values.
  /// @param x The value to decompose.
  /// @pre *rng* must cover at least `size()` values.
  template <class T, class Range>
  void decompose(T x, Range& rng) const {
    using std::begin;
    using std::end;
    VAST_ASSERT(end(rng) - begin(rng) >= static_cast<long>(size()));
    auto i = begin(rng);
    for (auto b : values_) {
      *i++ = x % b;
      x /= b;
    }
  }

  /// Composes a new value from a sequence of values.
  /// @param rng A range with the values to compose.
  /// @returns The composed value from *rng* according to this base.
  /// @pre *rng* must cover at least `size()` values.
  template <class T, class Range>
  T compose(Range&& rng) const {
    using std::begin;
    using std::end;
    VAST_ASSERT(end(rng) - begin(rng) >= static_cast<long>(size()));
    auto result = T{0};
    auto m = T{1};
    auto i = begin(rng);
    for (auto b : values_) {
      result += *i++ * m;
      m *= b;
    }
    return result;
  }

  // -- container -------------------------------------------------------------

  using iterator = typename vector_type::iterator;
  using const_iterator = typename vector_type::const_iterator;

  bool empty() const;

  size_t size() const;

  value_type& operator[](size_t i);
  value_type operator[](size_t i) const;

  iterator begin();
  const_iterator begin() const;

  iterator end();
  const_iterator end() const;

  // -- concepts --------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, base& b) {
    return f(b.values_);
  }

  friend bool operator==(const base& x, const base& y);

private:
  vector_type values_;
};

} // namespace vast

