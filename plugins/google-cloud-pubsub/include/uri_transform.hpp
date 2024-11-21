#pragma once
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/argument_parser2.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <google/cloud/pubsub/subscriber.h>

namespace tenzir::plugins::google_cloud_pubsub {

/// Transforms URI `project_id/argument_name` into a set of arguments to use
/// by the parser
inline auto make_uri_transform(std::string_view argument_name) {
  return [argument = std::string{argument_name}](located<std::string> uri,
                                                 diagnostic_handler& dh) {
    auto res = std::vector<ast::expression>{};
    auto slash = uri.inner.find('/');
    if (slash == uri.inner.npos) {
      diagnostic::error("Failed to to parse google cloud Pub/Sub URI")
        .primary(uri)
        .emit(dh);
      return res;
    }
    auto make = [](std::string name, std::string text,
                   location loc) -> ast::assignment {
      auto sel = ast::simple_selector::try_from(ast::root_field{
        ast::identifier{std::move(name), location::unknown},
      });
      TENZIR_ASSERT(sel);
      return {
        std::move(*sel),
        loc,
        ast::constant{std::move(text), loc},
      };
    };
    auto project_id = uri.inner.substr(0, slash);
    auto project_id_loc = location{uri.source.begin, uri.source.begin + slash};
    res.push_back(make("project_id", project_id, project_id_loc));
    auto argument_value = uri.inner.substr(slash + 1);
    auto argument_loc = location{uri.source.begin + slash + 1, uri.source.end};
    res.push_back(make(std::move(argument), argument_value, argument_loc));
    return res;
  };
}
} // namespace tenzir::plugins::google_cloud_pubsub
