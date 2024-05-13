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

namespace tenzir::plugins::reverse {

namespace {

class plugin final : public virtual operator_parser_plugin {
public:
  auto name() const -> std::string override {
    return "reverse";
  };

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"reverse", "https://docs.tenzir.com/"
                                             "operators/reverse"};
    parser.parse(p);
    auto result = pipeline::internal_parse_as_operator("slice ::-1");
    if (not result) {
      diagnostic::error("failed to transform `reverse` into `slice` operator: "
                        "{}",
                        result.error())
        .throw_();
    }
    return std::move(*result);
  }
};

} // namespace

} // namespace tenzir::plugins::reverse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::reverse::plugin)
