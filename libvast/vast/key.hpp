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

#ifndef VAST_KEY_HPP
#define VAST_KEY_HPP

#include <string>

#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/detail/stack_vector.hpp"

namespace vast {

/// A sequence of names identifying a resource.
struct key : detail::stack_vector<std::string, 128> {
  using super = detail::stack_vector<std::string, 128>;
  using super::super;

  static constexpr char delimiter = '.';

  /// Creates a key string reprentation of an arbitrary sequence.
  template <typename... Ts>
  static std::string str(Ts&&... xs);
};

} // namespace vast

#include "vast/concept/printable/vast/key.hpp"

template <typename... Ts>
std::string vast::key::str(Ts&&... xs) {
  using vast::to_string;
  using std::to_string;
  return to_string(key{to_string(xs)...});
}

#endif // VAST_KEY_HPP
