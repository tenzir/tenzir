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

#include <unordered_map>

namespace vast::format::json {

struct field_selector_specification {
  const char* field;       ///< The field containing the type name.
  const char* prefix;      ///< The stripped type name.
  const char* reader_name; ///< The name of the json reader.
};

template <field_selector_specification (*FieldSelectorSpecification)()>
struct field_selector {
  field_selector() {
    // nop
  }

  caf::optional<vast::record_type> operator()(const vast::json::object& j) {
    auto i = j.find(spec.field);
    if (i == j.end())
      return caf::none;
    auto field = caf::get_if<vast::json::string>(&i->second);
    if (!field) {
      VAST_WARNING(this, "got a", spec.field, "field with a non-string value");
      return caf::none;
    }
    auto it = types.find(*field);
    if (it == types.end()) {
      // Keep a list of failed keys to avoid spamming the user with warnings.
      if (unknown_types.insert(*field).second)
        VAST_WARNING(this, "does not have a layout for", spec.field, *field);
      return caf::none;
    }
    auto type = it->second;
    return type;
  }

  caf::error schema(const vast::schema& s) {
    for (auto& t : s) {
      auto sn = detail::split(t.name(), ".");
      if (sn.size() != 2)
        continue;
      auto r = caf::get_if<record_type>(&t);
      if (!r)
        continue;
      if (sn[0] == spec.prefix)
        // The temporary string can be dropped with c++20.
        // See https://wg21.link/p0919.
        types[std::string{sn[1]}] = flatten(*r);
    }
    return caf::none;
  }

  vast::schema schema() const {
    vast::schema result;
    for (auto& [key, value] : types)
      result.add(value);
    return result;
  }

  static const char* name() {
    return spec.reader_name;
  }

  /// Contains information about the field selector.
  static inline constexpr field_selector_specification spec
    = FieldSelectorSpecification();

  /// A map of all seen types.
  std::unordered_map<std::string, record_type> types;

  /// A set of all unknown types; used to avoid printing duplicate warnings.
  std::unordered_set<std::string> unknown_types;
};

} // namespace vast::format::json
