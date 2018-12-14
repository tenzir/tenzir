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
struct view_trait;

/// @relates view_trait
template <class T>
using view = typename view_trait<T>::type;

#define VAST_VIEW_TRAIT(type_name)                                             \
  inline auto materialize(type_name x) {                                       \
    return x;                                                                  \
  }                                                                            \
  template <>                                                                  \
  struct view_trait<type_name> {                                               \
    using type = type_name;                                                    \
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

/// @relates view_trait
template <>
struct view_trait<caf::none_t> {
  using type = caf::none_t;
};


/// @relates view_trait
template <>
struct view_trait<std::string> {
  using type = std::string_view;
};


/// @relates view_trait
class pattern_view : detail::totally_ordered<pattern_view> {
public:
  static pattern glob(std::string_view x);

  pattern_view(const pattern& x);

  bool match(std::string_view x) const;
  bool search(std::string_view x) const;
  std::string_view string() const;

private:
  std::string_view pattern_;
};

/// @relates pattern_view
bool operator==(pattern_view x, pattern_view y) noexcept;

/// @relates pattern_view
bool operator<(pattern_view x, pattern_view y) noexcept;

//// @relates view_trait
template <>
struct view_trait<pattern> {
  using type = pattern_view;
};

template <class T>
class container_view_handle;

struct vector_view_ptr;
struct set_view_ptr;
struct map_view_ptr;

// @relates view_trait
using vector_view_handle = container_view_handle<vector_view_ptr>;

// @relates view_trait
using set_view_handle = container_view_handle<set_view_ptr>;

// @relates view_trait
using map_view_handle = container_view_handle<map_view_ptr>;


/// @relates view_trait
template <>
struct view_trait<vector> {
  using type = vector_view_handle;
};

/// @relates view_trait
template <>
struct view_trait<set> {
  using type = set_view_handle;
};

/// @relates view_trait
template <>
struct view_trait<map> {
  using type = map_view_handle;
};

/// A type-erased view over variout types of data.
/// @relates view_trait
using data_view = caf::variant<
  view<caf::none_t>,
  view<boolean>,
  view<integer>,
  view<count>,
  view<real>,
  view<timespan>,
  view<timestamp>,
  view<std::string>,
  view<pattern>,
  view<address>,
  view<subnet>,
  view<port>,
  view<vector>,
  view<set>,
  view<map>
>;

/// @relates view_trait
template <>
struct view_trait<data> {
  using type = data_view;
};

template <class T>
struct container_view;

/// @relates view_trait
template <class T>
using container_view_ptr = caf::intrusive_ptr<container_view<T>>;

/// @relates container_view
template <class Pointer>
class container_view_handle
  : detail::totally_ordered<container_view_handle<Pointer>> {
public:
  container_view_handle() = default;

  container_view_handle(Pointer ptr) : ptr_{ptr} {
    // nop
  }

  explicit operator bool() const {
    return static_cast<bool>(ptr_);
  }

  auto operator->() const {
    return ptr_.get();
  }

  const auto& operator*() const {
    return *ptr_;
  }

private:
  Pointer ptr_;
};

template <class Pointer>
bool operator==(const container_view_handle<Pointer>& x,
                const container_view_handle<Pointer>& y) {
  return x && y && *x == *y;
}

template <class Pointer>
bool operator<(const container_view_handle<Pointer>& x,
               const container_view_handle<Pointer>& y) {
  if (!x)
    return static_cast<bool>(y);
  if (!y)
    return false;
  return *x < *y;
}

namespace detail {

/// @relates view_trait
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
/// @relates view_trait
template <class T>
struct container_view
  : caf::ref_counted,
    detail::totally_ordered<container_view<T>> {
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

template <class T>
bool operator==(const container_view<T>& xs, const container_view<T>& ys) {
  if (xs.size() != ys.size())
    return false;
  for (auto i = 0u; i < xs.size(); ++i)
    if (xs.at(i) != ys.at(i))
      return false;
  return true;
}

template <class T>
bool operator<(const container_view<T>& xs, const container_view<T>& ys) {
  if (xs.size() != ys.size())
    return xs.size() < ys.size();
  for (auto i = 0u; i < xs.size(); ++i)
    if (xs.at(i) < ys.at(i))
      return true;
  return false;
}

// @relates view_trait
struct vector_view_ptr : container_view_ptr<data_view> {};

// @relates view_trait
struct set_view_ptr : container_view_ptr<data_view> {};

// @relates view_trait
struct map_view_ptr : container_view_ptr<std::pair<data_view, data_view>> {};

/// A view over a @ref vector.
/// @relates view_trait
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
/// @relates view_trait
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
/// @relates view_trait
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
/// @relates view_trait
template <class T>
view<T> make_view(const T& x) {
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
/// @relates view_trait
template <size_t N>
view<std::string> make_view(const char (&xs)[N]) {
  return std::string_view(xs, N - 1);
}

/// Creates a view from a `std::string_view`.
/// @relates view_trait
constexpr view<std::string> make_view(std::string_view xs) {
  return xs;
}

/// @relates view data
data_view make_view(const data& x);

/// Creates a type-erased data view from a specific type.
/// @relates view_trait
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

vector materialize(vector_view_handle xs);

set materialize(set_view_handle xs);

map materialize(map_view_handle xs);

data materialize(data_view xs);

} // namespace vast
