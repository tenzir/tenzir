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

#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>

#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/none.hpp"
#include "vast/time.hpp"

#include "vast/detail/type_traits.hpp"

namespace vast {

/// A type-safe overlay over an immutable sequence of bytes.
template <class>
struct view;

/// @relates view
template <class T>
using view_t = typename view<T>::type;

/// @relates view
template <>
struct view<none> {
  using type = none;
};

/// @relates view
template <>
struct view<boolean> {
  using type = boolean;
};

/// @relates view
template <>
struct view<integer> {
  using type = integer;
};

/// @relates view
template <>
struct view<count> {
  using type = count;
};

/// @relates view
template <>
struct view<real> {
  using type = real;
};

/// @relates view
template <>
struct view<timespan> {
  using type = timespan;
};

/// @relates view
template <>
struct view<timestamp> {
  using type = timestamp;
};

/// @relates view
template <>
struct view<std::string> {
  using type = std::string_view;
};

//// @relates view
template <>
struct view<pattern> {
  using type = std::string_view;
};

// @relates view
struct vector_view;

/// @relates view
using vector_view_ptr = caf::intrusive_ptr<vector_view>;

/// @relates view
template <>
struct view<vector> {
  using type = vector_view_ptr;
};

/// A type-erased view over variout types of data.
/// @relates view
using data_view_variant = std::variant<
  none,
  view_t<boolean>,
  view_t<integer>,
  view_t<count>,
  view_t<real>,
  view_t<timespan>,
  view_t<timestamp>,
  view_t<std::string>,
  view_t<vector>
>;

/// @relates view
template <>
struct view<data> {
  using type = data_view_variant;
};

/// @relates view
struct vector_view : public caf::ref_counted {
  using value_type = view_t<data>;
  using size_type = size_t;

  virtual ~vector_view() = default;

  /// Retrieves a specific element.
  /// @param i The position of the element to retrieve.
  /// @returns A view to the element at position *i*.
  virtual value_type at(size_type i) const = 0;

  /// @returns The number of elements in the container.
  virtual size_type size() const = 0;
};

/// A view over a @ref vector.
/// @relates view
class default_vector_view : public vector_view {
public:
  default_vector_view(const vector& xs);

  value_type at(size_type i) const override;

  size_type size() const override;

private:
  const vector& xs_;
};

/// Creates a type-erased data view from a specific type.
/// @relates view
template <class T>
view_t<data> make_view(const T& x) {
  constexpr auto directly_constructible
    = detail::is_any_v<T, boolean, integer, count, real, timespan,
                       timestamp, std::string>;
  if constexpr (directly_constructible) {
    return view_t<data>{x};
  } else if constexpr (std::is_same_v<T, pattern>) {
    return {}; // TODO
  } else if constexpr (std::is_same_v<T, address>) {
    return {}; // TODO
  } else if constexpr (std::is_same_v<T, subnet>) {
    return {}; // TODO
  } else if constexpr (std::is_same_v<T, port>) {
    return {}; // TODO
  } else if constexpr (std::is_same_v<T, vector>) {
    return vector_view_ptr{new default_vector_view{x}};
  } else if constexpr (std::is_same_v<T, set>) {
    return {}; // TODO
  } else if constexpr (std::is_same_v<T, table>) {
    return {}; // TODO
  } else {
    return {};
  }
}

view_t<data> make_view(const data& x);

} // namespace vast
