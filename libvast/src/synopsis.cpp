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

#include "vast/synopsis.hpp"

#include <typeindex>

#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/serializer.hpp>

#include "vast/boolean_synopsis.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/timestamp_synopsis.hpp"

#include "vast/detail/overload.hpp"

namespace vast {

synopsis::synopsis(vast::type x) : type_{std::move(x)} {
  // nop
}

synopsis::~synopsis() {
  // nop
}

const vast::type& synopsis::type() const {
  return type_;
}

caf::error inspect(caf::serializer& sink, synopsis_ptr& ptr) {
  if (!ptr) {
    static type dummy;
    return sink(dummy);
  }
  return caf::error::eval(
    [&] { return sink(ptr->type()); },
    [&] { return ptr->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, synopsis_ptr& ptr) {
  // Read synopsis type.
  type t;
  if (auto err = source(t))
    return err;
  // Only nullptr has an none type.
  if (!t) {
    ptr.reset();
    return caf::none;
  }
  // Deserialize into a new instance.
  auto new_ptr = factory<synopsis>::make(std::move(t), synopsis_options{});
  if (!new_ptr)
    return ec::invalid_synopsis_type;
  if (auto err = new_ptr->deserialize(source))
    return err;
  // Change `ptr` only after successfully deserializing.
  using std::swap;
  swap(ptr, new_ptr);
  return caf::none;
}

} // namespace vast
