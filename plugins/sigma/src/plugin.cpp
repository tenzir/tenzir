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
#include <vast/plugin.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

namespace vast::plugins::sigma {

class plugin final : public virtual query_language_plugin {
  caf::error initialize(data) override {
    return caf::none;
  }

  [[nodiscard]] std::string_view name() const override {
    return "sigma";
  }

  [[nodiscard]] caf::expected<expression>
  parse(std::string_view query) const override {
    if (auto yaml = from_yaml(query))
      return parse_rule(*yaml);
    else
      return caf::make_error(ec::invalid_query,
                             fmt::format("not a Sigma rule: {}", yaml.error()));
  }
};

} // namespace vast::plugins::sigma

VAST_REGISTER_PLUGIN(vast::plugins::sigma::plugin)
