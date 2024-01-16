//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>

namespace tenzir::plugins::enrich {

namespace {

class plugin final : public virtual operator_parser_plugin {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = false,
      .transformation = true,
      .sink = false,
    };
  }

  auto name() const -> std::string override {
    return "enrich";
  };

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    const auto token = located<std::string>{"enrich", location::unknown};
    auto context_pi = prepend_token{token, p};
    const auto* context_plugin = plugins::find_operator("context");
    if (not context_plugin) {
      diagnostic::error("`context` plugin is required")
        .note("from `enrich`")
        .throw_();
    }
    return context_plugin->parse_operator(context_pi);
  }
};

} // namespace

} // namespace tenzir::plugins::enrich

TENZIR_REGISTER_PLUGIN(tenzir::plugins::enrich::plugin)
