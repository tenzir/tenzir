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

struct suricata {
  suricata() {
    // nop
  }

  caf::optional<vast::record_type> operator()(const vast::json::object& j) {
    auto i = j.find("event_type");
    if (i == j.end())
      return caf::none;
    auto event_type = caf::get_if<vast::json::string>(&i->second);
    if (!event_type) {
      VAST_WARNING(this, "got an event_type field with a non-string value");
      return caf::none;
    }
    auto it = types.find(*event_type);
    if (it == types.end()) {
      VAST_VERBOSE(this, "does not have a layout for event_type", *event_type);
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
      if (sn[0] == "suricata")
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
    return "suricata-reader";
  }

  std::unordered_map<std::string, record_type> types;
};

} // namespace vast::format::json
