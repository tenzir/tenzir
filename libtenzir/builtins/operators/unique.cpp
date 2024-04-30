//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::unique {

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
    return "unique";
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"unique", "https://docs.tenzir.com/"
                                            "operators/unique"};
    parser.parse(p);
    auto result
      = pipeline::internal_parse_as_operator("deduplicate --distance 1");
    if (not result) {
      diagnostic::error(result.error())
        .note("failed to parse `deduplicate`, which is required for `unique`")
        .throw_();
    }
    return std::move(*result);
  }
};

} // namespace

} // namespace tenzir::plugins::unique

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unique::plugin)
