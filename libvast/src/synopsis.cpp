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
  auto new_ptr = make_synopsis(std::move(t), synopsis_options{});
  if (!new_ptr)
    return ec::invalid_synopsis_type;
  if (auto err = new_ptr->deserialize(source))
    return err;
  // Change `ptr` only after successfully deserializing.
  using std::swap;
  swap(ptr, new_ptr);
  return caf::none;
}

namespace {

std::unordered_map<std::type_index, synopsis_factory> factories_;

std::type_index make_factory_index(const type& t) {
  auto f = detail::overload(
    [](const auto& x) { return std::type_index{typeid(x)}; },
    [](const alias_type& x) { return make_factory_index(x.value_type); }
  );
  return caf::visit(f, t);
}

struct factory_initializer {
  factory_initializer() {
    add_synopsis_factory<boolean_synopsis, boolean_type>();
    add_synopsis_factory<timestamp_synopsis, timestamp_type>();
  }
};

auto initializer = factory_initializer{};

} // namespace <anonymous>

synopsis_ptr make_synopsis(type x, const synopsis_options& opts) {
  if (auto f = get_synopsis_factory(x))
    return f(std::move(x), opts);
  return nullptr;
}

bool add_synopsis_factory(type x, synopsis_factory factory) {
  VAST_ASSERT(factory != nullptr);
  return factories_.emplace(make_factory_index(x), factory).second;
}

synopsis_factory get_synopsis_factory(const type& x) {
  auto i = factories_.find(make_factory_index(x));
  return i != factories_.end() ? i->second : nullptr;
}

} // namespace vast
