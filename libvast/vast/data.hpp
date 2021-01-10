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

#include "vast/address.hpp"
#include "vast/aliases.hpp"
#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/operators.hpp"
#include "vast/offset.hpp"
#include "vast/pattern.hpp"
#include "vast/subnet.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/none.hpp>
#include <caf/optional.hpp>
#include <caf/variant.hpp>

#include <chrono>
#include <string>
#include <tuple>
#include <type_traits>

namespace vast {

class data;
class json;

namespace detail {

// clang-format off
template <class T>
using to_data_type = std::conditional_t<
  std::is_floating_point_v<T>,
  real,
  std::conditional_t<
    std::is_same_v<T, bool>,
    bool,
    std::conditional_t<
      std::is_unsigned_v<T>,
      std::conditional_t<
        // TODO (ch7585): Define enumeration and count as strong typedefs to
        //                avoid error-prone heuristics like this one.
        sizeof(T) == 1,
        enumeration,
        count
      >,
      std::conditional_t<
        std::is_signed_v<T>,
        integer,
        std::conditional_t<
          std::is_convertible_v<T, std::string>,
          std::string,
          std::conditional_t<
               std::is_same_v<T, caf::none_t>
            || std::is_same_v<T, duration>
            || std::is_same_v<T, time>
            || std::is_same_v<T, pattern>
            || std::is_same_v<T, address>
            || std::is_same_v<T, subnet>
            || std::is_same_v<T, list>
            || std::is_same_v<T, map>
            || std::is_same_v<T, record>,
            T,
            std::false_type
          >
        >
      >
    >
  >
>;
// clang-format on

} // namespace detail

/// Converts a C++ type to the corresponding VAST data type.
/// @relates data
template <class T>
using to_data_type = detail::to_data_type<std::decay_t<T>>;

/// A type-erased represenation of various types of data.
/// @note Be careful when constructing a `vector<data>` from a single `list`.
/// Brace-initialization may behave unexpectedly, creating a nested vector
/// instead of copying the elements of the list for gcc only. This happens for
/// two reasons:
/// - `data` can be implicitly constructed from `list`, which is exactly
///   `vector<data>`. This is not easily fixable, because `data` is a
///   `variant<list, ...>`, which is implicitly constructible from `list` by
///   design.
/// - Core Working Group (CWG) issue #2137 has not been fixed in clang yet.
///   c.f. https://wg21.cmeerw.net/cwg/issue2137, causing brace-initialization
///   to erroneously behave like other means of construction.
class data : detail::totally_ordered<data>,
             detail::addable<data> {
public:
  // clang-format off
  using types = caf::detail::type_list<
    caf::none_t,
    bool,
    integer,
    count,
    real,
    duration,
    time,
    std::string,
    pattern,
    address,
    subnet,
    enumeration,
    list,
    map,
    record
  >;
  // clang-format on

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
  data(std::chrono::duration<Rep, Period> x) : data_{duration{x}} {
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

  friend bool operator==(const data& lhs, const data& rhs);
  friend bool operator<(const data& lhs, const data& rhs);

  // These operators need to be templates so they're instantiated at a later
  // point in time, because there'd be a cyclic dependency otherwise.
  // caf::variant<Ts...> is just a placeholder for vast::data_view here.

  template <class... Ts>
  friend bool operator==(const data& lhs, const caf::variant<Ts...>& rhs) {
    return is_equal(lhs, rhs);
  }

  template <class... Ts>
  friend bool operator==(const caf::variant<Ts...>& lhs, const data& rhs) {
    return is_equal(lhs, rhs);
  }

  template <class... Ts>
  friend bool operator!=(const data& lhs, const caf::variant<Ts...>& rhs) {
    return !is_equal(lhs, rhs);
  }

  template <class... Ts>
  friend bool operator!=(const caf::variant<Ts...>& lhs, const data& rhs) {
    return !is_equal(lhs, rhs);
  }

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

VAST_DATA_TRAIT(bool);
VAST_DATA_TRAIT(integer);
VAST_DATA_TRAIT(count);
VAST_DATA_TRAIT(real);
VAST_DATA_TRAIT(duration);
VAST_DATA_TRAIT(time);
VAST_DATA_TRAIT(pattern);
VAST_DATA_TRAIT(address);
VAST_DATA_TRAIT(subnet);
VAST_DATA_TRAIT(enumeration);
VAST_DATA_TRAIT(list);
VAST_DATA_TRAIT(map);
VAST_DATA_TRAIT(record);

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

/// Creates a record instance for a given record type. The number of data
/// instances must correspond to the number of fields in the flattened version
/// of the record.
/// @param rt The record type
/// @param xs The record fields.
/// @returns A record according to the fields as defined in *rt*.
caf::optional<record> make_record(const record_type& rt, std::vector<data>&& xs);

/// Flattens a record recursively.
record flatten(const record& r);

/// Flattens a record recursively according to a record type such
/// that only nested records are lifted into parent list.
/// @param r The record to flatten.
/// @param rt The record type according to which *r* should be flattened.
/// @returns The flattened record if the nested structure of *r* is a valid
///          subset of *rt*.
/// @see unflatten
caf::optional<record> flatten(const record& r, const record_type& rt);
caf::optional<data> flatten(const data& x, const type& t);

/// Unflattens a flattened record.
record unflatten(const record& r);

/// Unflattens a record according to a record type such that the record becomes
/// a recursive structure.
/// @param r The record to unflatten according to *rt*.
/// @param rt The type that defines the record structure.
/// @returns The unflattened record of *r* according to *rt*.
/// @see flatten
caf::optional<record> unflatten(const record& r, const record_type& rt);
caf::optional<data> unflatten(const data& x, const type& t);

/// Merges one record into another such that the source overwrites potential
/// keys in the destination.
/// @param src The record to merge into *dst*.
/// @param dst The result record containing the merged result.
void merge(const record& src, record& dst);

/// Evaluates a data predicate.
/// @param lhs The LHS of the predicate.
/// @param op The relational operator.
/// @param rhs The RHS of the predicate.
bool evaluate(const data& lhs, relational_operator op, const data& rhs);

// -- convertible -------------------------------------------------------------

bool convert(const list& xs, json& j);
bool convert(const map& xs, json& j);
bool convert(const record& xs, json& j);
bool convert(const data& xs, json& j);

/// Converts data with a type to "zipped" JSON, i.e., the JSON object for
/// records contains the field names from the type corresponding to the given
/// data.
bool convert(const data& x, json& j, const type& t);

caf::error convert(const record& xs, caf::dictionary<caf::config_value>& ys);
caf::error convert(const record& xs, caf::config_value& cv);
caf::error convert(const data& x, caf::config_value& cv);

caf::error convert(const caf::dictionary<caf::config_value>& xs, record& ys);
caf::error convert(const caf::dictionary<caf::config_value>& xs, data& y);
caf::error convert(const caf::config_value& x, data& y);

// -- YAML -------------------------------------------------------------

/// Parses YAML into a data.
/// @param str The string containing the YAML content
/// @returns The parsed YAML as data, or an error.
caf::expected<data> from_yaml(std::string_view str);

/// Loads YAML from a file.
/// @param file The file to load.
/// @returns The parsed YAML or an error.
caf::expected<data> load_yaml(const path& file);

/// Loads all *.yml and *.yaml files in a given directory.
/// @param dir The directory to traverse recursively.
/// @param max_recursion The maximum number of nested directories to traverse
/// before aborting.
/// @returns The parsed YAML, one `data` instance per file, or an error.
caf::expected<std::vector<std::pair<path, data>>>
load_yaml_dir(const path& dir, size_t max_recursion = defaults::max_recursion);

/// Prints data as YAML.
/// @param x The data instance.
/// @returns The YAML representation of *x*, or an error.
caf::expected<std::string> to_yaml(const data& x);

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
