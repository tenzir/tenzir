//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::local_remote {

namespace {

auto location_name(operator_location location) -> std::string_view {
  switch (location) {
    case operator_location::local:
      return "local";
    case operator_location::remote:
      return "remote";
    case operator_location::anywhere:
      break;
  }
  TENZIR_UNREACHABLE();
}

template <detail::string_literal Name, operator_location Location>
class plugin final : public virtual operator_compiler_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  const record& global_config) -> caf::error override {
    auto no_location_overrides
      = try_get_or(global_config, "tenzir.no-location-overrides", false);
    if (not no_location_overrides) {
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("failed to parse "
                                         "`tenzir.no-location-overrides` "
                                         "option: {}",
                                         no_location_overrides.error()));
    }
    return {};
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  };

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto loc = inv.op.get_location();
    if (inv.args.size() != 1) {
      diagnostic::error("`{}` expects a single pipeline argument",
                        location_name(Location))
        .primary(loc)
        .emit(ctx);
      return failure::promise();
    }
    auto* pipe_expr = try_as<ast::pipeline_expr>(inv.args[0]);
    if (not pipe_expr) {
      diagnostic::error("`{}` expects a pipeline argument `{{ … }}`",
                        location_name(Location))
        .primary(inv.args[0])
        .emit(ctx);
      return failure::promise();
    }
    diagnostic::warning("`{}` has no effect in the new executor",
                        location_name(Location))
      .primary(loc)
      .emit(ctx);
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    return pipe_ir;
  }
};

using local_plugin = plugin<"local", operator_location::local>;
using remote_plugin = plugin<"remote", operator_location::remote>;

} // namespace

} // namespace tenzir::plugins::local_remote

TENZIR_REGISTER_PLUGIN(tenzir::plugins::local_remote::local_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::local_remote::remote_plugin)
