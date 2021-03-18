// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/string.hpp"
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

  caf::optional<vast::record_type>
  operator()(const ::simdjson::dom::object& j) {
    auto el = j.at_key(Specification::field);
    if (el.error())
      return caf::none;
    auto event_type = el.value().get_string();
    if (event_type.error()) {
      VAST_WARN("{} got a {} field with a non-string value",
                detail::pretty_type_name(this), Specification::field);
      return caf::none;
    }
    auto field = std::string{event_type.value()};
    auto it = types.find(field);
    if (it == types.end()) {
      // Keep a list of failed keys to avoid spamming the user with warnings.
      if (unknown_types.insert(field).second)
        VAST_WARN("{} does not have a layout for {} {}",
                  detail::pretty_type_name(this), Specification::field, field);
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
