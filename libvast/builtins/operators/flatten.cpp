//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline.hpp"

#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>

namespace vast::plugins::flatten {

namespace {

// Does nothing with the input.
class flatten_operator final : public crtp_operator<flatten_operator> {
public:
  flatten_operator(std::string separator) : separator_{std::move(separator)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto seen = std::unordered_set<type>{};
    for (auto&& slice : input) {
      auto result = vast::flatten(slice, separator_);
      // We only warn once per schema that we had to rename a set of fields.
      if (seen.insert(slice.schema()).second
          && not result.renamed_fields.empty()) {
        ctrl.warn(
          caf::make_error(ec::convert_error,
                          fmt::format("the flatten operator renamed fields due "
                                      "to conflicting names: {}",
                                      fmt::join(result.renamed_fields, ", "))));
      }
      co_yield std::move(result).slice;
    }
  }

  auto to_string() const -> std::string override {
    return fmt::format("flatten '{}'", separator_);
  }

private:
  std::string separator_ = {};
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "flatten";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::operator_arg;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(optional_ws_or_comment >> operator_arg)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto separator = std::string{};
    if (!p(f, l, separator)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "flatten operator: '{}'",
                                                      pipeline)),
      };
    }
    if (separator.find('\'') != std::string::npos) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        "failed to parse flatten operator: separator must not "
                        "contain a single quote"),
      };
    }
    if (separator.empty())
      separator = ".";
    return {
      std::string_view{f, l},
      std::make_unique<flatten_operator>(std::move(separator)),
    };
  }
};

} // namespace

} // namespace vast::plugins::flatten

VAST_REGISTER_PLUGIN(vast::plugins::flatten::plugin)
