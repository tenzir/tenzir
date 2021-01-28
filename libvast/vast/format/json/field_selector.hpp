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

#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/string.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"

#include <simdjson.h>
#include <unordered_map>

namespace vast::format::json {

template <class Specification>
struct field_selector {
  field_selector() {
    // nop
  }

  caf::optional<vast::record_type> operator()(const vast::json::object& j) {
    auto i = j.find(Specification::field);
    if (i == j.end())
      return caf::none;
    auto field = caf::get_if<vast::json::string>(&i->second);
    if (!field) {
      VAST_WARN("{} got a {} field with a non-string value",
                detail::id_or_name(this), Specification::field);
      return caf::none;
    }
    auto it = types.find(*field);
    if (it == types.end()) {
      // Keep a list of failed keys to avoid spamming the user with warnings.
      if (unknown_types.insert(*field).second)
        VAST_WARN("{} does not have a layout for {}  {}",
                  detail::id_or_name(this), Specification::field, *field);
      return caf::none;
    }
    auto type = it->second;
    return type;
  }

  caf::optional<vast::record_type>
  operator()(const ::simdjson::dom::object& j) {
    auto el = j.at_key(Specification::field);
    if (el.error())
      return caf::none;
    auto event_type = el.value().get_string();
    if (event_type.error()) {
      VAST_WARN("{} got a {} field with a non-string value",
                detail::id_or_name(this), Specification::field);
      return caf::none;
    }
    auto field = std::string{event_type.value()};
    auto it = types.find(field);
    if (it == types.end()) {
      // Keep a list of failed keys to avoid spamming the user with warnings.
      if (unknown_types.insert(field).second)
        VAST_WARN("{} does not have a layout for {}  {}",
                  detail::id_or_name(this), Specification::field, field);
      return caf::none;
    }
    return it->second;
  }

  caf::error schema(const vast::schema& s) {
    for (auto& t : s) {
      auto sn = detail::split(t.name(), ".");
      if (sn.size() != 2)
        continue;
      auto r = caf::get_if<record_type>(&t);
      if (!r)
        continue;
      if (sn[0] == Specification::prefix)
        // The temporary string can be dropped with c++20.
        // See https://wg21.link/p0919.
        types[std::string{sn[1]}] = *r;
    }
    return caf::none;
  }

  vast::schema schema() const {
    vast::schema result;
    for (auto& [key, value] : types)
      result.add(value);
    return result;
  }

  /// A map of all seen types.
  std::unordered_map<std::string, record_type> types;

  /// A set of all unknown types; used to avoid printing duplicate warnings.
  std::unordered_set<std::string> unknown_types;
};

} // namespace vast::format::json
