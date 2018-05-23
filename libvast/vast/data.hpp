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

#include <chrono>
#include <iterator>
#include <regex>
#include <string>
#include <vector>
#include <map>
#include <type_traits>

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"

#include "vast/aliases.hpp"
#include "vast/address.hpp"
#include "vast/pattern.hpp"
#include "vast/subnet.hpp"
#include "vast/port.hpp"
#include "vast/none.hpp"
#include "vast/offset.hpp"
#include "vast/optional.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"
#include "vast/variant.hpp"
#include "vast/detail/flat_set.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/string.hpp"

namespace vast {

class data;
class json;

namespace detail {

template <class T>
using make_data_type = std::conditional_t<
    std::is_floating_point_v<T>,
    real,
    std::conditional_t<
      std::is_same_v<T, boolean>,
      boolean,
      std::conditional_t<
        std::is_unsigned_v<T>,
        count,
        std::conditional_t<
          std::is_signed_v<T>,
          integer,
          std::conditional_t<
            std::is_convertible_v<T, std::string>,
            std::string,
            std::conditional_t<
              std::is_same_v<T, std::string_view>,
              std::string,
              std::conditional_t<
                   std::is_same_v<T, none>
                || std::is_same_v<T, timespan>
                || std::is_same_v<T, timestamp>
                || std::is_same_v<T, pattern>
                || std::is_same_v<T, address>
                || std::is_same_v<T, subnet>
                || std::is_same_v<T, port>
                || std::is_same_v<T, enumeration>
                || std::is_same_v<T, vector>
                || std::is_same_v<T, set>
                || std::is_same_v<T, map>,
                T,
                std::false_type
              >
            >
          >
        >
      >
    >
  >;

using data_variant = variant<
  none,
  boolean,
  integer,
  count,
  real,
  timespan,
  timestamp,
  std::string,
  pattern,
  address,
  subnet,
  port,
  enumeration,
  vector,
  set,
  map 
>;

} // namespace detail

/// Converts a C++ type to the corresponding VAST data type.
template <class T>
using data_type = detail::make_data_type<std::decay_t<T>>;

/// A type-erased represenation of various types of data.
class data : detail::totally_ordered<data>,
             detail::addable<data> {
  friend access;

public:
  /// Default-constructs empty data.
  data(none = nil);

  /// Constructs data from optional data.
  /// @param x The optional data instance.
  template <class T>
  data(optional<T> x) : data{x ? std::move(*x) : data{}} {
  }

  /// Constructs data from a `std::chrono::duration`.
  /// @param x The duration to construct data from.
  template <class Rep, class Period>
  data(std::chrono::duration<Rep, Period> x) : data_{timespan{x}} {
  }

  /// Constructs data.
  /// @param x The instance to construct data from.
  template <
    class T,
    class = detail::disable_if_t<
      detail::is_same_or_derived_v<data, T>
      || std::is_same_v<data_type<T>, std::false_type>
    >
  >
  data(T&& x)
    : data_(data_type<T>(std::forward<T>(x))) {
  }

  data& operator+=(const data& rhs);

  friend bool operator==(const data& lhs, const data& rhs);
  friend bool operator<(const data& lhs, const data& rhs);

  template <class Inspector>
  friend auto inspect(Inspector&f, data& d) {
    return f(d.data_);
  }

  friend detail::data_variant& expose(data& d);

private:
  detail::data_variant data_;
};

//template <class T>
//using is_basic_data = std::integral_constant<
//    bool,
//    std::is_same<T, boolean>::value
//      || std::is_same<T, integer>::value
//      || std::is_same<T, count>::value
//      || std::is_same<T, real>::value
//      || std::is_same<T, timespan>::value
//      || std::is_same<T, timestamp>::value
//      || std::is_same<T, std::string>::value
//      || std::is_same<T, pattern>::value
//      || std::is_same<T, address>::value
//      || std::is_same<T, subnet>::value
//      || std::is_same<T, port>::value
//  >;
//
//template <class T>
//using is_container_data = std::integral_constant<
//    bool,
//    std::is_same<T, vector>::value
//      || std::is_same<T, set>::value
//      || std::is_same<T, table>::value
//  >;

// -- helpers -----------------------------------------------------------------

/// Retrieves data at a given offset.
/// @param v The vector to lookup.
/// @param o The offset to access.
/// @returns A pointer to the data at *o* or `nullptr` if *o* does not
///          describe a valid offset.
const data* get(const vector& v, const offset& o);
const data* get(const data& d, const offset& o);

/// Flattens a vector.
/// @param xs The vector to flatten.
/// @returns The flattened vector.
/// @see unflatten
vector flatten(const vector& xs);
vector flatten(vector&& xs);

/// Flattens a vector.
/// @param x The vector to flatten.
/// @returns The flattened vector as `data` if *x* is a `vector`.
/// @see unflatten
data flatten(const data& x);
data flatten(data&& x);

/// Unflattens a vector according to a record type.
/// @param xs The vector to unflatten according to *rt*.
/// @param rt The type that defines the vector structure.
/// @returns The unflattened vector of *xs* according to *rt*.
/// @see flatten
optional<vector> unflatten(const vector& xs, const record_type& rt);
optional<vector> unflatten(vector&& xs, const record_type& rt);

/// Unflattens a vector according to a record type.
/// @param x The vector to unflatten according to *t*.
/// @param t The type that defines the vector structure.
/// @returns The unflattened vector of *x* according to *t* if *x* is a
///          `vector` and *t* a `record_type`.
/// @see flatten
optional<vector> unflatten(const data& x, const type& t);
optional<vector> unflatten(data&& x, const type& t);

/// Evaluates a data predicate.
/// @param lhs The LHS of the predicate.
/// @param op The relational operator.
/// @param rhs The RHS of the predicate.
bool evaluate(const data& lhs, relational_operator op, const data& rhs);

// -- convertible -------------------------------------------------------------

bool convert(const vector& v, json& j);
bool convert(const set& v, json& j);
bool convert(const map& v, json& j);
bool convert(const data& v, json& j);

/// Converts data with a type to "zipped" JSON, i.e., the JSON object for
/// records contains the field names from the type corresponding to the given
/// data.
bool convert(const data& v, json& j, const type& t);

} // namespace vast

namespace std {

template <>
struct hash<vast::data> {
  size_t operator()(const vast::data& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

} // namespace std

