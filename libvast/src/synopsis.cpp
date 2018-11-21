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
#include "vast/logger.hpp"
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

caf::atom_value synopsis::factory_id() const noexcept {
  return caf::atom("Sy_Default");
}

caf::error inspect(caf::serializer& sink, synopsis_ptr& ptr) {
  if (!ptr) {
    type dummy;
    return sink(dummy);
  }
  return caf::error::eval(
    [&] { return sink(ptr->type(), ptr->factory_id()); },
    [&] { return ptr->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, synopsis_ptr& ptr) {
  // Read synopsis type.
  type t;
  if (auto err = source(t))
    return err;
  // Only default-constructed synopses have an empty type.
  if (!t) {
    ptr.reset();
    return caf::none;
  }
  // Select factory based on the implementation ID.
  synopsis_factory f;
  if (auto ex = deserialize_synopsis_factory(source))
    f = ex->second;
  else
    return std::move(ex.error());
  // Deserialize into a new instance.
  auto new_ptr = f(std::move(t), synopsis_options{});
  if (!new_ptr)
    return ec::invalid_synopsis_type;
  if (auto err = new_ptr->deserialize(source))
    return err;
  // Change `ptr` only after successfully deserializing.
  using std::swap;
  swap(ptr, new_ptr);
  return caf::none;
}

synopsis_ptr make_synopsis(type x, const synopsis_options&) {
  return caf::visit(detail::overload(
    [&](const timestamp_type&) -> synopsis_ptr {
      return caf::make_counted<timestamp_synopsis>(std::move(x));
    },
    [](const auto&) -> synopsis_ptr {
      return nullptr;
    }), x);
}

expected<std::pair<caf::atom_value, synopsis_factory>>
deserialize_synopsis_factory(caf::deserializer& source) {
  // Select factory based on the implementation ID.
  caf::atom_value impl_id;
  if (auto err = source(impl_id))
    return err;
  synopsis_factory f;
  if (impl_id == caf::atom("Sy_Default")) {
    f = make_synopsis;
  } else if (source.context() == nullptr) {
    return caf::sec::no_context;
  } else {
    using generic_fun = caf::runtime_settings_map::generic_function_pointer;
    auto& sys = source.context()->system();
    auto val = sys.runtime_settings().get(impl_id);
    if (!caf::holds_alternative<generic_fun>(val)) {
      VAST_ERROR_ANON("synopsis",
                      "has no factory function for implementation key",
                      impl_id);
      return ec::invalid_synopsis_type;
    }
    f = reinterpret_cast<synopsis_factory>(caf::get<generic_fun>(val));
  }
  return std::make_pair(impl_id, f);
}

void set_synopsis_factory(caf::actor_system& sys, caf::atom_value id,
                          synopsis_factory factory) {
  using generic_fun = caf::runtime_settings_map::generic_function_pointer;
  auto f = reinterpret_cast<generic_fun>(factory);
  sys.runtime_settings().set(id, f);
  sys.runtime_settings().set(caf::atom("Sy_FACTORY"), id);
}

caf::expected<std::pair<caf::atom_value, synopsis_factory>>
get_synopsis_factory(caf::actor_system& sys) {
  auto x = sys.runtime_settings().get(caf::atom("Sy_FACTORY"));
  if (auto factory_id = caf::get_if<caf::atom_value>(&x)) {
    auto y = sys.runtime_settings().get(*factory_id);
    using generic_fun = caf::runtime_settings_map::generic_function_pointer;
    auto fun = caf::get_if<generic_fun>(&y);
    if (fun == nullptr)
      return make_error(ec::unspecified, "incomplete synopsis factory setup");
    auto factory = reinterpret_cast<synopsis_factory>(*fun);
    return std::make_pair(*factory_id, factory);
  }
  return caf::no_error;
}

} // namespace vast
