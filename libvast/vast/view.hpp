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

#include <array>
#include <cstdint>
#include <string>
#include <type_traits>

#include <caf/intrusive_ptr.hpp>
#include <caf/make_counted.hpp>
#include <caf/ref_counted.hpp>
#include <caf/variant.hpp>

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/time.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/iterator.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {

/// A type-safe overlay over an immutable sequence of bytes.
template <class>
struct view;

/// @relates view
template <class T>
using view_t = typename view<T>::type;

#define VAST_VIEW_TRAIT(type_name)                                             \
  template <>                                                                  \
  struct view<type_name> {                                                     \
    using type = type_name;                                                    \
  };                                                                           \
  inline auto materialize(type_name x) {                                       \
    return x;                                                                  \
  }

VAST_VIEW_TRAIT(boolean);
VAST_VIEW_TRAIT(integer);
VAST_VIEW_TRAIT(count);
VAST_VIEW_TRAIT(real);
VAST_VIEW_TRAIT(timespan);
VAST_VIEW_TRAIT(timestamp);
VAST_VIEW_TRAIT(port);
VAST_VIEW_TRAIT(address);
VAST_VIEW_TRAIT(subnet);

#undef VAST_VIEW_TRAIT

/// @relates view
template <>
struct view<caf::none_t> {
  using type = caf::none_t;
};


/// @relates view
template <>
struct view<std::string> {
  using type = std::string_view;
};


/// @relates view
class pattern_view : detail::totally_ordered<pattern_view> {
public:
  static pattern glob(std::string_view x);

  pattern_view(const pattern& x);

  bool match(std::string_view x) const;
  bool search(std::string_view x) const;
  std::string_view string() const;

  friend bool operator==(pattern_view x, pattern_view y) noexcept;
  friend bool operator<(pattern_view x, pattern_view y) noexcept;

private:
  std::string_view pattern_;
};

//// @relates view
template <>
struct view<pattern> {
  using type = pattern_view;
};


struct vector_view_ptr;
struct set_view_ptr;
struct map_view_ptr;

/// @relates view
template <>
struct view<vector> {
  using type = vector_view_ptr;
};

/// @relates view
template <>
struct view<set> {
  using type = set_view_ptr;
};

/// @relates view
template <>
struct view<map> {
  using type = map_view_ptr;
};

/// A type-erased view over variout types of data.
/// @relates view
using data_view = caf::variant<
  view_t<caf::none_t>,
  view_t<boolean>,
  view_t<integer>,
  view_t<count>,
  view_t<real>,
  view_t<timespan>,
  view_t<timestamp>,
  view_t<std::string>,
  view_t<pattern>,
  view_t<address>,
  view_t<subnet>,
  view_t<port>,
  view_t<vector>,
  view_t<set>,
  view_t<map>
>;

/// @relates view
template <>
struct view<data> {
  using type = data_view;
};

template <class T>
struct container_view;

/// @relates view
template <class T>
using container_view_ptr = caf::intrusive_ptr<container_view<T>>;

namespace detail {

/// @relates view
template <class T>
class container_view_iterator
  : public detail::iterator_facade<
      container_view_iterator<T>,
      T,
      std::random_access_iterator_tag,
      T
    > {
  friend iterator_access;

public:
  container_view_iterator(typename container_view_ptr<T>::const_pointer ptr,
                          size_t pos)
    : view_{ptr}, position_{pos} {
    // nop
  }

  auto dereference() const {
    return view_->at(position_);
  }

  void increment() {
    ++position_;
  }

  void decrement() {
    --position_;
  }

  template <class N>
  void advance(N n) {
    position_ += n;
  }

  bool equals(container_view_iterator other) const {
    return view_ == other.view_ && position_ == other.position_;
  }

  auto distance_to(container_view_iterator other) const {
    return other.position_ - position_;
  }

private:
  typename container_view_ptr<T>::const_pointer view_;
  size_t position_;
};

} // namespace detail

/// Base class for container views.
/// @relates view
template <class T>
struct container_view : caf::ref_counted {
  using value_type = T;
  using size_type = size_t;
  using iterator = detail::container_view_iterator<T>;
  using const_iterator = iterator;

  virtual ~container_view() = default;

  iterator begin() const {
    return {this, 0};
  }

  iterator end() const {
    return {this, size()};
  }

  /// Retrieves a specific element.
  /// @param i The position of the element to retrieve.
  /// @returns A view to the element at position *i*.
  virtual value_type at(size_type i) const = 0;

  /// @returns The number of elements in the container.
  virtual size_type size() const noexcept = 0;
};

// @relates view
struct vector_view_ptr : container_view_ptr<data_view> {};

// @relates view
struct set_view_ptr : container_view_ptr<data_view> {};

// @relates view
struct map_view_ptr : container_view_ptr<std::pair<data_view, data_view>> {};

/// A view over a @ref vector.
/// @relates view
class default_vector_view
  : public container_view<data_view>,
    detail::totally_ordered<default_vector_view> {
public:
  default_vector_view(const vector& xs);

  value_type at(size_type i) const override;

  size_type size() const noexcept override;

private:
  const vector& xs_;
};

/// A view over a @ref set.
/// @relates view
class default_set_view
  : public container_view<data_view>,
    detail::totally_ordered<default_set_view> {
public:
  default_set_view(const set& xs);

  value_type at(size_type i) const override;

  size_type size() const noexcept override;

private:
  const set& xs_;
};

/// A view over a @ref map.
/// @relates view
class default_map_view
  : public container_view<std::pair<data_view, data_view>>,
    detail::totally_ordered<default_map_view> {
public:
  default_map_view(const map& xs);

  value_type at(size_type i) const override;

  size_type size() const noexcept override;

private:
  const map& xs_;
};

/// Creates a view from a specific type.
/// @relates view
template <class T>
view_t<T> make_view(const T& x) {
  constexpr auto directly_constructible
    = detail::is_any_v<T, caf::none_t, boolean, integer, count, real, timespan,
                       timestamp, std::string, pattern, address, subnet, port>;
  if constexpr (directly_constructible) {
    return x;
  } else if constexpr (std::is_same_v<T, vector>) {
    return vector_view_ptr{caf::make_counted<default_vector_view>(x)};
  } else if constexpr (std::is_same_v<T, set>) {
    return set_view_ptr{caf::make_counted<default_set_view>(x)};
  } else if constexpr (std::is_same_v<T, map>) {
    return map_view_ptr{caf::make_counted<default_map_view>(x)};
  } else {
    VAST_ASSERT(!"missing branch");
    return {};
  }
}

/// Creates a view from a string literal.
/// @relates view
template <size_t N>
view_t<std::string> make_view(const char (&xs)[N]) {
  return std::string_view(xs, N - 1);
}

/// @relates view data
data_view make_view(const data& x);

/// Creates a type-erased data view from a specific type.
/// @relates view
template <class T>
data_view make_data_view(const T& x) {
  return make_view(x);
}

// -- materialization ----------------------------------------------------------

constexpr auto materialize(caf::none_t x) {
  return x;
}

std::string materialize(std::string_view x);

pattern materialize(pattern_view x);

vector materialize(vector_view_ptr xs);

set materialize(set_view_ptr xs);

map materialize(map_view_ptr xs);

data materialize(data_view xs);

} // namespace vast
