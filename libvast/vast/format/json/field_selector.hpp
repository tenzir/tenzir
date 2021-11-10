//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
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
  std::optional<vast::type> operator()(const ::simdjson::dom::object& j) {
    auto el = j.at_key(Specification::field);
    if (el.error())
      return {};
    auto event_type = el.value().get_string();
    if (event_type.error()) {
      VAST_WARN("{} got a {} field with a non-string value",
                detail::pretty_type_name(this), Specification::field);
      return {};
    }
    auto field = std::string{event_type.value()};
    auto it = types.find(field);
    if (it == types.end()) {
      // Keep a list of failed keys to avoid spamming the user with warnings.
      if (unknown_types.insert(field).second)
        VAST_WARN("{} does not have a layout for {} {}",
                  detail::pretty_type_name(this), Specification::field, field);
      return {};
    }
    return it->second;
  }

  caf::error schema(const vast::schema& s) {
    for (const auto& t : s) {
      auto sn = detail::split(t.name(), ".");
      if (sn.size() != 2)
        continue;
      if (!caf::holds_alternative<record_type>(t))
        continue;
      if (sn[0] == Specification::prefix) {
        // The temporary string can be dropped with c++20.
        // See https://wg21.link/p0919.
        types.emplace(sn[1], t);
      }
    }
    return caf::none;
  }

  [[nodiscard]] vast::schema schema() const {
    vast::schema result;
    for (const auto& [key, value] : types)
      result.add(value);
    return result;
  }

  static const char* name() {
    return Specification::name;
  }

  /// A map of all seen types.
  std::unordered_map<std::string, type> types;

  /// A set of all unknown types; used to avoid printing duplicate warnings.
  std::unordered_set<std::string> unknown_types;
};

} // namespace vast::format::json
