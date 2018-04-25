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

// FIXME: const std::string& to string_view

/// A map for CLI options.
class option_map {
public:
  // FIXME: the map api is not complete
  using map_type = detail::steady_map<std::string, data>;
  using iterator = map_type::iterator;
  using const_iterator = map_type::const_iterator;
  using reverse_iterator = map_type::reverse_iterator;
  using const_reverse_iterator = map_type::const_reverse_iterator;
  using size_type = map_type::size_type;

  optional<data> get(const std::string& name) const;

  data get_or(const std::string& name, const data& default_value) const;

  inline optional<data> operator[](const std::string& name) const {
    return get(name);
  }

  void set(const std::string& name, const data& x);

  expected<void> add(const std::string& name, const data& x);

  void clear() {
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

