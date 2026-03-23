//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::legacy {

namespace {

class plugin final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "legacy";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto string = located<std::string>{};
    TRY(argument_parser2::operator_(name())
          .positional("definition", string)
          .parse(inv, ctx));
    auto [pipe, diags] = tql::parse_internal_with_diags(string.inner);
    for (auto& diag : diags) {
      ctx.dh().emit(std::move(diag));
    }
    if (not pipe) {
      diagnostic::error("failed to parse legacy pipeline")
        .primary(string)
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<pipeline>(std::move(*pipe));
  }
};

} // namespace

} // namespace tenzir::plugins::legacy

TENZIR_REGISTER_PLUGIN(tenzir::plugins::legacy::plugin)
