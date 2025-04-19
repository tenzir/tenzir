//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/diagnostics.hpp>
#include <tenzir/location.hpp>
#include <tenzir/tql2/ast.hpp>

namespace tenzir::plugins::google_cloud_pubsub {

/// Transforms URI `project_id/argument_name` into a set of arguments to use
/// by the parser
inline auto make_uri_transform(std::string_view argument_name) {
  return [argument = std::string{argument_name}](
           located<std::string> uri,
           diagnostic_handler& dh) -> failure_or<std::vector<ast::expression>> {
    auto res = std::vector<ast::expression>{};
    auto slash = uri.inner.find('/');
    if (slash == uri.inner.npos
        or uri.inner.find('/', slash + 1) != uri.inner.npos) {
      diagnostic::error("failed to to parse Google Cloud Pub/Sub URI")
        .hint("the expected format is `project_id/{}`", argument)
        .primary(uri)
        .emit(dh);
      return failure::promise();
    }
    auto make = [](std::string name, std::string text,
                   location loc) -> ast::assignment {
      auto sel = ast::field_path::try_from(ast::root_field{
        ast::identifier{std::move(name), location::unknown},
      });
      TENZIR_ASSERT(sel);
      return {
        std::move(*sel),
        loc,
        ast::constant{std::move(text), loc},
      };
    };
    auto project_id_loc = uri.source;
    auto argument_loc = uri.source;
    if (uri.source.end - uri.source.begin == uri.inner.size() + 2) {
      project_id_loc = location{uri.source.begin, uri.source.begin + slash};
      argument_loc = location{uri.source.begin + slash + 1, uri.source.end};
    }
    auto project_id = uri.inner.substr(0, slash);
    res.push_back(make("project_id", project_id, project_id_loc));
    auto argument_value = uri.inner.substr(slash + 1);
    res.push_back(make(std::move(argument), argument_value, argument_loc));
    return res;
  };
}
} // namespace tenzir::plugins::google_cloud_pubsub
