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
#include "vast/format/json/selector.hpp"
#include "vast/logger.hpp"
#include "vast/module.hpp"

#include <simdjson.h>
#include <unordered_map>

namespace vast::format::json {

struct field_selector final : selector {
  /// Constructs a field selector given a field name and a type prefix.
  /// @pre `!field_name.empty()`
  inline field_selector(std::string field_name, std::string type_prefix)
    : field_name_{std::move(field_name)}, type_prefix_{std::move(type_prefix)} {
    VAST_ASSERT(!field_name_.empty());
  }

  inline std::optional<vast::type>
  operator()(const ::simdjson::dom::object& j) const override {
    auto el = j.at_key(field_name_);
    if (el.error() != ::simdjson::error_code::SUCCESS)
      return {};
    auto field = el.is_string() ? std::string{el.value().get_string().value()}
                                : simdjson::to_string(el.value());
    auto it = types.find(field);
    if (it == types.end()) {
      // Keep a list of failed keys to avoid spamming the user with warnings.
      if (unknown_types.insert(field).second)
        VAST_WARN("{} does not have a schema for {} {}",
                  detail::pretty_type_name(this), field_name_, field);
      return {};
    }
    return it->second;
  }

  inline caf::error module(const vast::module& m) override {
    for (const auto& t : m) {
      if (!caf::holds_alternative<record_type>(t))
        continue;
      const auto type_name = t.name();
      const auto [name_mismatch, prefix_mismatch]
        = std::mismatch(type_name.begin(), type_name.end(),
                        type_prefix_.begin(), type_prefix_.end());
      if (prefix_mismatch == type_prefix_.end()
          && name_mismatch != type_name.end() && *name_mismatch == '.') {
        const auto remaining_name
          = type_name.substr(1 + name_mismatch - type_name.begin());
        types.emplace(remaining_name, t);
      }
    }
    return caf::none;
  }

  inline vast::module module() const override {
    vast::module result;
    for (const auto& [key, value] : types)
      result.add(value);
    return result;
  }

private:
  /// The field that contains the event name.
  std::string field_name_ = {};

  /// The prefix of the event name type.
  std::string type_prefix_ = {};

  /// A map of all seen types.
  std::unordered_map<std::string, type> types = {};

  /// A set of all unknown types; used to avoid printing duplicate warnings.
  mutable std::unordered_set<std::string> unknown_types = {};
};

} // namespace vast::format::json
