//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/parse_query.hpp"

#include "vast/collect.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/legacy_pipeline.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/config_value.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>
#include <caf/string_algorithms.hpp>

#include <optional>

namespace vast::system {

caf::expected<std::pair<expression, std::optional<legacy_pipeline>>>
parse_query(const std::string& query) {
  // Get all query languages, but make sure that VAST is at the front.
  // TODO: let the user choose exactly one language instead.
  auto languages = collect(plugins::get<legacy_language_plugin>());
  if (const auto* vast = plugins::find<legacy_language_plugin>("VAST")) {
    const auto it = std::find(languages.begin(), languages.end(), vast);
    VAST_ASSERT_CHEAP(it != languages.end());
    std::rotate(languages.begin(), it, it + 1);
  }
  for (const auto& language : languages) {
    if (auto query_result = language->make_query(query))
      return query_result;
    else
      VAST_DEBUG("failed to parse query as {} language: {}", language->name(),
                 query_result.error());
  }
  return caf::make_error(ec::syntax_error,
                         fmt::format("invalid query: {}", query));
}

caf::expected<std::pair<expression, std::optional<legacy_pipeline>>>
parse_query(std::vector<std::string>::const_iterator begin,
            std::vector<std::string>::const_iterator end) {
  if (begin == end)
    return caf::make_error(ec::syntax_error, "no query expression given");
  auto query = detail::join(begin, end, " ");
  return parse_query(query);
}

caf::expected<std::pair<expression, std::optional<legacy_pipeline>>>
parse_query(const std::vector<std::string>& args) {
  return parse_query(args.begin(), args.end());
}

caf::expected<std::pair<expression, std::optional<legacy_pipeline>>>
parse_query(const spawn_arguments& args) {
  auto query_result = system::parse_query(args.inv.arguments);
  if (!query_result)
    return query_result.error();
  return query_result;
}

} // namespace vast::system
