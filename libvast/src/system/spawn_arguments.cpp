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
#include "vast/detail/load_contents.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/schema.hpp"

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
  // Always try parsing as VAST expression first.
  if (auto e = to<expression>(query))
    return normalize_and_validate(std::move(*e));
  // If that fails, we try all query language plugins, currently in a
  // non-deterministic order.
  // TODO: let the user choose exactly one language instead.
  for (const auto& plugin : plugins::get()) {
    if (const auto* language = plugin.as<query_language_plugin>()) {
      if (auto expr = language->parse(query))
        return normalize_and_validate(std::move(*expr));
      else
        VAST_DEBUG("failed to parse query as {} language: {}", language->name(),
                   expr.error());
    }
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

caf::expected<std::optional<schema>> read_schema(const spawn_arguments& args) {
  auto schema_file_ptr = caf::get_if<std::string>(&args.inv.options, "schema");
  if (!schema_file_ptr)
    return std::optional<schema>{std::nullopt};
  auto str = detail::load_contents(std::filesystem::path{*schema_file_ptr});
  if (!str)
    return str.error();
  auto result = to<schema>(*str);
  if (!result)
    return result.error();
  return std::optional<schema>{std::move(*result)};
}

caf::error unexpected_arguments(const spawn_arguments& args) {
  return caf::make_error(ec::syntax_error, "unexpected argument(s)",
                         caf::join(args.inv.arguments.begin(),
                                   args.inv.arguments.end(), " "));
}

} // namespace vast::system
