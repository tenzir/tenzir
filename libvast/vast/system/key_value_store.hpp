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

#ifndef VAST_SYSTEM_KEY_VALUE_STORE_HPP
#define VAST_SYSTEM_KEY_VALUE_STORE_HPP

#include <caf/stateful_actor.hpp>
#include <caf/replies_to.hpp>
#include <caf/typed_actor.hpp>

#include "vast/optional.hpp"

#include "vast/system/atoms.hpp"

namespace vast::system {

template <class Key, class Value>
using key_value_store_type = caf::typed_actor<
  // Updates the value of a specific key.
  class caf::replies_to<put_atom, Key, Value>::template with<ok_atom>,
  // Adds a value to a specific key and returns old key.
  class caf::replies_to<add_atom, Key, Value>::template with<Value>,
  // Deletes a key-value pair.
  class caf::replies_to<delete_atom, Key>::template with<ok_atom>,
  // Retrieves the value for a given key pair.
  class caf::replies_to<get_atom, Key>::template with<optional<Value>>
>;

} // namespace vast::system

#endif
