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
#include <string>
#include <tuple>
#include <type_traits>

#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/none.hpp>
#include <caf/optional.hpp>
#include <caf/variant.hpp>

#include "vast/aliases.hpp"
#include "vast/address.hpp"
#include "vast/pattern.hpp"
#include "vast/subnet.hpp"
#include "vast/port.hpp"
#include "vast/offset.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"

#include "vast/detail/operators.hpp"

namespace vast {

class data;
class json;

namespace detail {

template <class T>
using to_data_type = std::conditional_t<
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
               std::is_same_v<T, caf::none_t>
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
>;

} // namespace detail

/// Converts a C++ type to the corresponding VAST data type.
/// @relates data
template <class T>
using to_data_type = detail::to_data_type<std::decay_t<T>>;

/// A type-erased represenation of various types of data.
class data : detail::totally_ordered<data>,
             detail::addable<data> {
public:
  using types = caf::detail::type_list<
    caf::none_t,
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

  /// The sum type of all possible JSON types.
  using variant = caf::detail::tl_apply_t<types, caf::variant>;

  /// Default-constructs empty data.
  data() = default;

  /// Constructs data from optional data.
  /// @param x The optional data instance.
  template <class T>
  data(caf::optional<T> x) : data{x ? std::move(*x) : data{}} {
    // nop
  }

  /// Constructs data from a `std::chrono::duration`.
  /// @param x The duration to construct data from.
  template <class Rep, class Period>
  data(std::chrono::duration<Rep, Period> x) : data_{timespan{x}} {
    // nop
  }

  /// Constructs data.
  /// @param x The instance to construct data from.
  template <
    class T,
    class = detail::disable_if_t<
      std::is_same_v<to_data_type<T>, std::false_type>
    >
  >
  data(T&& x) : data_{to_data_type<T>(std::forward<T>(x))} {
    // nop
  }

  data& operator+=(const data& rhs);

  friend bool operator==(const data& lhs, const data& rhs);
  friend bool operator<(const data& lhs, const data& rhs);

  /// @cond PRIVATE

  variant& get_data() {
    return data_;
  }

  const variant& get_data() const {
    return data_;
  }

  template <class Inspector>
  friend auto inspect(Inspector&f, data& x) {
    return f(x.data_);
  }

  /// @endcond

private:
  variant data_;
};

// -- helpers -----------------------------------------------------------------

/// Maps a concrete data type to a corresponding @ref type.
/// @relates data type
template <class>
struct data_traits {
  using type = std::false_type;
};

#define VAST_DATA_TRAIT(name)                                                  \
  template <>                                                                  \
  struct data_traits<name> {                                                   \
    using type = name##_type;                                                  \
  }

VAST_DATA_TRAIT(boolean);
VAST_DATA_TRAIT(integer);
VAST_DATA_TRAIT(count);
VAST_DATA_TRAIT(real);
VAST_DATA_TRAIT(timespan);
VAST_DATA_TRAIT(timestamp);
VAST_DATA_TRAIT(pattern);
VAST_DATA_TRAIT(address);
VAST_DATA_TRAIT(subnet);
VAST_DATA_TRAIT(port);
VAST_DATA_TRAIT(vector);
VAST_DATA_TRAIT(set);
VAST_DATA_TRAIT(map);

#undef VAST_DATA_TRAIT

template <>
struct data_traits<caf::none_t> {
  using type = none_type;
};

template <>
struct data_traits<std::string> {
  using type = string_type;
};

/// @relates data type
template <class T>
using data_to_type = typename data_traits<T>::type;

/// @returns `true` if *x is a *basic* data.
/// @relates data
bool is_basic(const data& x);

/// @returns `true` if *x is a *complex* data.
/// @relates data
bool is_complex(const data& x);

/// @returns `true` if *x is a *recursive* data.
/// @relates data
bool is_recursive(const data& x);

/// @returns `true` if *x is a *container* data.
/// @relates data
bool is_container(const data& x);

/// Retrieves data at a given offset.
/// @param v The vector to lookup.
/// @param o The offset to access.
/// @returns A pointer to the data at *o* or `nullptr` if *o* does not
///          describe a valid offset.
const data* get(const vector& v, const offset& o);
const data* get(const data& d, const offset& o);

/// Flattens a vector recursively according to a record type such that only
/// nested records are lifted into parent vector.
/// @param xs The vector to flatten.
/// @param t The record type according to which *xs* should be flattened.
/// @returns The flattened vector if the nested structure of *xs* is congruent
///           to *t*.
/// @see unflatten
caf::optional<vector> flatten(const vector& xs, const record_type& t);
caf::optional<data> flatten(const data& x, type t);

/// Unflattens a vector according to a record type.
/// @param xs The vector to unflatten according to *rt*.
/// @param rt The type that defines the vector structure.
/// @returns The unflattened vector of *xs* according to *rt*.
/// @see flatten
caf::optional<vector> unflatten(const vector& xs, const record_type& rt);
caf::optional<data> unflatten(const data& x, type t);

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

/// @relates data
template <class... Ts>
auto make_vector(Ts... xs) {
  return vector{data{std::move(xs)}...};
}

template <class... Ts>
auto make_vector(std::tuple<Ts...> tup) {
  return std::apply([](auto&... xs) { return make_vector(std::move(xs)...); },
                    tup);
}

} // namespace vast

namespace caf {

template <>
struct sum_type_access<vast::data> : default_sum_type_access<vast::data> {};

} // namespace caf

namespace std {

template <>
struct hash<vast::data> {
  size_t operator()(const vast::data& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

} // namespace std
