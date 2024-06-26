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
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::tail {

namespace {

class plugin final : public virtual operator_parser_plugin,
                     public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tail";
  };

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"tail", "https://docs.tenzir.com/"
                                          "operators/tail"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice -{}:", count.value_or(10)));
    if (not result) {
      diagnostic::error("failed to transform `tail` into `slice` operator: {}",
                        result.error())
        .throw_();
    }
    return std::move(*result);
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto count = std::optional<uint64_t>{};
    argument_parser2::op("tail").add(count, "<count>").parse(inv, ctx);
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice -{}:", count.value_or(10)));
    if (not result) {
      diagnostic::error("failed to transform `tail` into `slice` operator: {}",
                        result.error())
        .emit(ctx);
      return nullptr;
    }
    return std::move(*result);
  }
};

} // namespace

} // namespace tenzir::plugins::tail

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tail::plugin)
