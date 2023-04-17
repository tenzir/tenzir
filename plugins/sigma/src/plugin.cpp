//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "sigma/parse.hpp"

#include <vast/data.hpp>
#include <vast/error.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/plugin.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

namespace vast::plugins::sigma {

class plugin final : public virtual language_plugin {
  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "sigma";
  }

  auto parse_query(std::string_view query) const
    -> caf::expected<pipeline> override {
    if (auto yaml = from_yaml(query)) {
      auto parsed = parse_rule(*yaml);
      if (parsed) {
        return pipeline::parse(fmt::format("where {}", *parsed));
      }
      return std::move(parsed.error());
    } else {
      return caf::make_error(ec::invalid_query,
                             fmt::format("not a Sigma rule: {}", yaml.error()));
    }
  }
};

} // namespace vast::plugins::sigma

VAST_REGISTER_PLUGIN(vast::plugins::sigma::plugin)
