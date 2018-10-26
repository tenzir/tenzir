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

#include <caf/actor_system.hpp>
#include <caf/runtime_settings_map.hpp>

#include "vast/error.hpp"
#include "vast/min_max_synopsis.hpp"

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
    type dummy;
    return sink(dummy);
  }
  return caf::error::eval([&] { return sink(ptr->type()); },
                          [&] { return ptr->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, synopsis_ptr& ptr) {
  type t;
  auto err = source(t);
  if (err)
    return err;
  // Only default-constructed synopses have an empty type.
  if (!t) {
    ptr.reset();
    return caf::none;
  }
  if (source.context() != nullptr) {
    auto factory = find_synopsis_factory(source.context()->system());
    ptr = factory ? factory(std::move(t)) : make_synopsis(std::move(t));
  } else {
    ptr = make_synopsis(std::move(t));
  }
  return ptr->deserialize(source);
}

namespace {

class timestamp_synopsis : public min_max_synopsis<timestamp> {
public:
  timestamp_synopsis(vast::type x) 
    : min_max_synopsis<timestamp>{std::move(x), timestamp::max(),
                                  timestamp::min()} {
    // nop
  }
};

} // namespace <anonymous>

synopsis_ptr make_synopsis(type x) {
  return caf::visit(detail::overload(
    [&](const timestamp_type&) -> synopsis_ptr {
      return caf::make_counted<timestamp_synopsis>(std::move(x));
    },
    [](const auto&) -> synopsis_ptr {
      return nullptr;
    }), x);
}

// TODO: we should find a way to associate a key-value pair with the
// runtime-settings map, with key being an atom and value a function pointer.
// Right now, it's a brittle setup where the user must manage two keys.

synopsis_factory find_synopsis_factory(caf::actor_system& sys) {
  using generic_fun = caf::runtime_settings_map::generic_function_pointer;
  auto val = sys.runtime_settings().get(caf::atom("SYNOPSIS_F"));
  if (auto f = caf::get_if<generic_fun>(&val))
    return reinterpret_cast<synopsis_ptr (*)(type)>(*f);
  return {};
}

caf::atom_value find_synopsis_factory_tag(caf::actor_system& sys) {
  auto val = sys.runtime_settings().get(caf::atom("SYNOPSIS_T"));
  if (auto x = caf::get_if<caf::atom_value>(&val))
    return *x;
  return {};
}

} // namespace vast
