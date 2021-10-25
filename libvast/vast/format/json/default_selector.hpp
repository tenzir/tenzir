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
#include "vast/hash/hash_append.hpp"
#include "vast/hash/xxhash.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"

#include <caf/expected.hpp>

#include <optional>
#include <simdjson.h>

namespace vast::format::json {

struct default_selector {
private:
  template <typename Prefix>
  static void
  make_names_layout_impl(std::vector<std::string>& entries, Prefix& prefix,
                         const ::simdjson::dom::object& obj) {
    for (const auto f : obj) {
      prefix.emplace_back(f.key);
      if (f.value.type() != ::simdjson::dom::element_type::OBJECT)
        entries.emplace_back(detail::join(prefix.begin(), prefix.end(), "."));
      else
        make_names_layout_impl(entries, prefix, f.value);
      prefix.pop_back();
    }
  }

  static auto make_names_layout(const ::simdjson::dom::object& obj) {
    std::vector<std::string> entries;
    entries.reserve(100);
    auto prefix = detail::stack_vector<std::string_view, 64>{};
    make_names_layout_impl(entries, prefix, obj);
    std::sort(entries.begin(), entries.end());
    return entries;
  }

public:
  std::optional<legacy_record_type>
  operator()(const ::simdjson::dom::object& obj) const {
    if (type_cache.empty())
      return {};
    // Iff there is only one type in the type cache, allow the JSON reader to
    // use it despite not being an exact match.
    if (type_cache.size() == 1)
      return type_cache.begin()->second;
    if (auto search_result = type_cache.find(make_names_layout(obj));
        search_result != type_cache.end())
      return search_result->second;
    return {};
  }

  caf::error schema(vast::schema sch) {
    if (sch.empty())
      return caf::make_error(ec::invalid_configuration,
                             "no schema provided or type "
                             "too restricted");
    for (auto& entry : sch) {
      if (!caf::holds_alternative<legacy_record_type>(entry))
        continue;
      auto layout = caf::get<legacy_record_type>(entry);
      std::vector<std::string> cache_entry;
      for (auto& [k, v] : layout.fields)
        cache_entry.emplace_back(k);
      std::sort(cache_entry.begin(), cache_entry.end());
      type_cache.insert({std::move(cache_entry), std::move(layout)});
    }
    return caf::none;
  }

  [[nodiscard]] vast::schema schema() const {
    vast::schema result;
    for (const auto& [k, v] : type_cache)
      result.add(v);
    return result;
  }

  static const char* category() {
    return "json";
  }

  static const char* name() {
    return "json-reader";
  }

  detail::flat_map<std::vector<std::string>, legacy_record_type> type_cache
    = {};
};

} // namespace vast::format::json
