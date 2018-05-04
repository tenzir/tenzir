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

#include <string>
#include <string_view>

#include "vast/optional.hpp"
#include "vast/expected.hpp"
#include "vast/data.hpp"

#include "vast/detail/steady_map.hpp"

namespace vast {

/// A map for CLI options.
class option_map {
public:
  // -- types ----------------------------------------------------------------

  using map_type = detail::steady_map<std::string, data>;
  using key_type = map_type::key_type;
  using mapped_type = map_type::mapped_type;
  using value_type = map_type::value_type;
  using size_type = map_type::size_type;
  using difference_type = map_type::difference_type;
  using allocator_type = map_type::allocator_type;
  using reference = map_type::reference;
  using const_reference = map_type::const_reference;
  using pointer = map_type::pointer;
  using const_pointer = map_type::const_pointer;
  using iterator = map_type::iterator;
  using const_iterator = map_type::const_iterator;
  using reverse_iterator = map_type::reverse_iterator;
  using const_reverse_iterator = map_type::const_reverse_iterator;

  // -- lookup ---------------------------------------------------------------

  optional<const mapped_type&> operator[](std::string_view name) const;

  // -- modifiers ------------------------------------------------------------

  expected<void> add(const key_type& name, const mapped_type& x);

  void set(const key_type& name, const mapped_type& x);

  void clear();

  // -- iterators ------------------------------------------------------------

  iterator begin();

  const_iterator begin() const;

  iterator end();

  const_iterator end() const;

  reverse_iterator rbegin();

  const_reverse_iterator rbegin() const;

  reverse_iterator rend();

  const_reverse_iterator rend() const;

  // -- capacity -------------------------------------------------------------

  bool empty() const;

  size_type size() const;

private:
  map_type xs_;
};

template <class T>
optional<const T&> get(const option_map& xs, std::string_view name) {
  auto x = xs[name];
  if (!x)
    return {};
  if (auto result = get_if<T>(*x); result)
    return *result;
  return {};
}

template <class T>
T get_or(const option_map& xs, std::string_view name,
         const T& default_value) {
  if (auto x = get<detail::make_data_type<T>>(xs, name); x)
    return *x;
  return default_value;
}

} // namespace vast

