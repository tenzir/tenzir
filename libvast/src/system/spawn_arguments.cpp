//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_arguments.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/detail/collect.hpp"
#include "vast/detail/load_contents.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/module.hpp"
#include "vast/plugin.hpp"

#include <caf/config_value.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>
#include <caf/string_algorithms.hpp>

#include <filesystem>
#include <optional>

namespace vast::system {

caf::expected<expression>
normalized_and_validated(std::vector<std::string>::const_iterator begin,
                         std::vector<std::string>::const_iterator end) {
  if (begin == end)
    return caf::make_error(ec::syntax_error, "no query expression given");
  auto query = detail::join(begin, end, " ");
  // Get all query languages, but make sure that VAST is at the front.
  // TODO: let the user choose exactly one language instead.
  auto query_languages = collect(plugins::get<query_language_plugin>());
  if (const auto* vastql = plugins::find<query_language_plugin>("VASTQL")) {
    const auto it
      = std::find(query_languages.begin(), query_languages.end(), vastql);
    VAST_ASSERT(it != query_languages.end());
    std::rotate(query_languages.begin(), it, it + 1);
  }
  for (const auto& query_language : query_languages) {
    if (auto expr = query_language->make_query(query))
      return normalize_and_validate(std::move(*expr));
    else
      VAST_DEBUG("failed to parse query as {} language: {}",
                 query_language->name(), expr.error());
  }
  return caf::make_error(ec::syntax_error,
                         fmt::format("invalid query: {}", query));
}

caf::expected<expression>
normalized_and_validated(const std::vector<std::string>& args) {
  return normalized_and_validated(args.begin(), args.end());
}

caf::expected<expression>
normalized_and_validated(const spawn_arguments& args) {
  auto& arguments = args.inv.arguments;
  return normalized_and_validated(arguments.begin(), arguments.end());
}

caf::expected<expression> get_expression(const spawn_arguments& args) {
  if (args.expr)
    return *args.expr;
  auto expr = system::normalized_and_validated(args.inv.arguments);
  if (!expr)
    return expr.error();
  return *expr;
}

caf::expected<std::optional<module>> read_module(const spawn_arguments& args) {
  auto module_file_ptr = caf::get_if<std::string>(&args.inv.options, "schema");
  if (!module_file_ptr)
    return std::optional<module>{std::nullopt};
  auto str = detail::load_contents(std::filesystem::path{*module_file_ptr});
  if (!str)
    return str.error();
  auto result = to<module>(*str);
  if (!result)
    return result.error();
  return std::optional<module>{std::move(*result)};
}

caf::error unexpected_arguments(const spawn_arguments& args) {
  return caf::make_error(ec::syntax_error, "unexpected argument(s)",
                         caf::join(args.inv.arguments.begin(),
                                   args.inv.arguments.end(), " "));
}

} // namespace vast::system
