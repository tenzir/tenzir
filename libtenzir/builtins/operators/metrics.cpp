//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::metrics {

namespace {

class plugin final : public virtual operator_parser_plugin {
public:
  auto name() const -> std::string override {
    return "metrics";
  };

  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"metrics", "https://docs.tenzir.com/"
                                             "operators/metrics"};
    auto name = std::optional<std::string>{};
    auto live = false;
    auto retro = false;
    parser.add(name, "<name>");
    parser.add("--live", live);
    parser.add("--retro", retro);
    parser.parse(p);
    if (not live) {
      retro = true;
    }
    const auto definition
      = fmt::format("export --internal{}{} | where #schema == {}",
                    live ? " --live" : "", retro ? " --retro" : "",
                    name ? fmt::format("\"tenzir.metrics.{}\"", *name)
                         : "/tenzir\\.metrics\\..+/");
    auto result = pipeline::internal_parse_as_operator(definition);
    if (not result) {
      diagnostic::error("failed to transform `metrics` operator into `{}`",
                        definition)
        .hint("{}", result.error())
        .throw_();
    }
    return std::move(*result);
  }
};

} // namespace

} // namespace tenzir::plugins::metrics

TENZIR_REGISTER_PLUGIN(tenzir::plugins::metrics::plugin)
