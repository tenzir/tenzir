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
  auto new_ptr = f(std::move(t));
  if (auto err = new_ptr->deserialize(source))
    return err;
  // Change `ptr` only after successfully deserializing.
  using std::swap;
  swap(ptr, new_ptr);
  return caf::none;
}

namespace {

class timestamp_synopsis final : public min_max_synopsis<timestamp> {
public:
  timestamp_synopsis(vast::type x)
    : min_max_synopsis<timestamp>{std::move(x), timestamp::max(),
                                  timestamp::min()} {
    // nop
  }

  bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(timestamp_synopsis))
      return false;
    auto& dref = static_cast<const timestamp_synopsis&>(other);
    return type() == dref.type() && min() == dref.min() && max() == dref.max();
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

expected<std::pair<caf::atom_value, synopsis_factory>>
deserialize_synopsis_factory(caf::deserializer& source) {
  // Select factory based on the implementation ID.
  caf::atom_value impl_id;
  if (auto err = source(impl_id))
    return err;
  synopsis_factory f;
  if (impl_id == caf::atom("Sy_Default")) {
    f = make_synopsis;
  } else {
    if (source.context() != nullptr)
      return caf::sec::no_context;
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

} // namespace vast
