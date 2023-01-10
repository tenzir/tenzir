//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/flat_map.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/format/json/selector.hpp"
#include "vast/hash/hash_append.hpp"
#include "vast/hash/xxhash.hpp"
#include "vast/logger.hpp"

#include <caf/expected.hpp>

#include <optional>
#include <simdjson.h>

namespace vast::format::json {

struct default_selector final : selector {
private:
  template <typename Prefix>
  static void
  make_names_schema_impl(std::vector<std::string>& entries, Prefix& prefix,
                         const ::simdjson::dom::object& obj) {
    for (const auto f : obj) {
      prefix.emplace_back(f.key);
      if (f.value.type() != ::simdjson::dom::element_type::OBJECT)
        entries.emplace_back(detail::join(prefix.begin(), prefix.end(), "."));
      else
        make_names_schema_impl(entries, prefix, f.value);
      prefix.pop_back();
    }
  }

  static inline auto make_names_schema(const ::simdjson::dom::object& obj) {
    std::vector<std::string> entries;
    entries.reserve(100);
    auto prefix = detail::stack_vector<std::string_view, 64>{};
    make_names_schema_impl(entries, prefix, obj);
    std::sort(entries.begin(), entries.end());
    return entries;
  }

public:
  /// Locates the type for a given JSON object.
  inline std::optional<type>
  operator()(const ::simdjson::dom::object& obj) const override {
    if (type_cache.empty())
      return {};
    // Iff there is only one type in the type cache, allow the JSON reader to
    // use it despite not being an exact match.
    if (type_cache.size() == 1)
      return type_cache.begin()->second;
    if (auto search_result = type_cache.find(make_names_schema(obj));
        search_result != type_cache.end())
      return search_result->second;
    return {};
  }

  /// Sets the module.
  inline caf::error module(const vast::module& mod) override {
    if (mod.empty())
      return caf::make_error(ec::invalid_configuration,
                             "no schema provided or type "
                             "too restricted");
    for (const auto& entry : mod) {
      if (!caf::holds_alternative<record_type>(entry))
        continue;
      if (entry.name().empty()) {
        VAST_WARN("unexpectedly unnamed schema in schema: {}", entry);
        continue;
      }
      std::vector<std::string> cache_entry;
      const auto& schema = caf::get<record_type>(entry);
      for (const auto& [_, index] : schema.leaves())
        cache_entry.emplace_back(schema.key(index));
      std::sort(cache_entry.begin(), cache_entry.end());
      type_cache.insert({std::move(cache_entry), entry});
    }
    return caf::none;
  }

  /// Retrieves the current module.
  inline vast::module module() const override {
    vast::module result;
    for (const auto& [k, v] : type_cache)
      result.add(v);
    return result;
  }

private:
  detail::flat_map<std::vector<std::string>, type> type_cache = {};
};

} // namespace vast::format::json
