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

// FIXME: Use string_view instead of const std::string& where apropiate.
// The Steady_map currently does not allow to use string_views to search for
// strings

/// A map for CLI options.
class option_map {
public:
  // -- types ----------------------------------------------------------------

  using key_type = std::string;
  using mapped_type = data;
  using value_type = std::pair<key_type, mapped_type>;
  using map_type = detail::steady_map<key_type, mapped_type>;
  using size_type = typename map_type::size_type;
  using difference_type = typename map_type::difference_type;
  using allocator_type = typename map_type::allocator_type;
  using reference = typename map_type::reference;
  using const_reference = typename map_type::const_reference;
  using pointer = typename map_type::pointer;
  using const_pointer = typename map_type::const_pointer;
  using iterator = typename map_type::iterator;
  using const_iterator = typename map_type::const_iterator;
  using reverse_iterator = typename map_type::reverse_iterator;
  using const_reverse_iterator = typename map_type::const_reverse_iterator;

  // -- lookup ---------------------------------------------------------------

  optional<mapped_type> get(const key_type& name) const;

  mapped_type get_or(const key_type& name,
                     const mapped_type& default_value) const;

  optional<mapped_type> operator[](const key_type& name) const;

  // -- modifiers ------------------------------------------------------------

  expected<void> add(const key_type& name, const mapped_type& x);

  void set(const key_type& name, const mapped_type& x);

  inline void clear() {
    xs_.clear();
  }

  // -- iterators ------------------------------------------------------------

  inline auto begin() {
    return xs_.begin();
  }

  inline auto begin() const {
    return xs_.begin();
  }

  inline auto end() {
    return xs_.end();
  }

  inline auto end() const {
    return xs_.end();
  }

  inline auto rbegin() {
    return xs_.rbegin();
  }

  inline auto rbegin() const {
    return xs_.rbegin();
  }

  inline auto rend() {
    return xs_.rend();
  }

  inline auto rend() const {
    return xs_.rend();
  }

  // -- capacity -------------------------------------------------------------

  inline auto empty() const {
    return xs_.empty();
  }

  inline auto size() const {
    return xs_.size();
  }
private:
  map_type xs_;
};

} // namespace vast

