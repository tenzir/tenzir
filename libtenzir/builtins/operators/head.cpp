//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::head {

namespace {

class plugin final : public virtual operator_parser_plugin,
                     public virtual tql2::operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "head";
  };

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"head", "https://docs.tenzir.com/"
                                          "operators/head"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice --end {}", count.value_or(10)));
    if (not result) {
      diagnostic::error("failed to transform `head` into `slice` operator: {}",
                        result.error())
        .throw_();
    }
    return std::move(*result);
  }

  auto
  make_operator(tql2::ast::entity self, std::vector<tql2::ast::expression> args,
                tql2::context& ctx) const -> operator_ptr override {
    // TODO: This is quite bad.
    if (args.size() > 1) {
      diagnostic::error("TODO").primary(self.get_location()).emit(ctx);
      return nullptr;
    }
    auto count = std::optional<uint64_t>{};
    if (args.size() == 1) {
      auto count_data = tql2::const_eval(args[0], ctx);
      if (count_data) {
        auto count_ptr = caf::get_if<int64_t>(&*count_data);
        if (not count_ptr || *count_ptr < 0) {
          diagnostic::error("expected a positive integer")
            .primary(args[0].get_location())
            .emit(ctx);
        } else {
          count = *count_ptr;
        }
      }
    }
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice --end {}", count.value_or(10)));
    if (not result) {
      diagnostic::error("failed to transform `head` into `slice` operator: {}",
                        result.error())
        .emit(ctx);
      return nullptr;
    }
    return std::move(*result);
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
