//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/error.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::vastql {

class plugin final : public virtual query_language_plugin {
  caf::error initialize(data) override {
    return caf::none;
  }

  [[nodiscard]] std::string name() const override {
    return "VASTQL";
  }

  [[nodiscard]] caf::expected<expression>
  make_query(std::string_view query) const override {
    auto result = expression{};
    if (parsers::expr(query, result))
      return result;
    return caf::make_error(ec::invalid_query,
                           fmt::format("not a valid query: '{}'", query));
  }
};

} // namespace vast::plugins::vastql

VAST_REGISTER_PLUGIN(vast::plugins::vastql::plugin)
