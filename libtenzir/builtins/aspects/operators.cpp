//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/tql2/exec.hpp>

namespace tenzir::plugins::operators {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)plugin_config;
    udos_ = get_or(global_config, "tenzir.operators", udos_);
    return {};
  }

  auto name() const -> std::string override {
    return "operators";
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto builder = series_builder{};
    // Add operator plugins.
    for (const auto* plugin : plugins::get<operator_parser_plugin>()) {
      auto event = builder.record();
      const auto signature = plugin->signature();
      event.field("name", plugin->name());
      event.field("definition").null();
      event.field("source", signature.source);
      event.field("transformation", signature.transformation);
      event.field("sink", signature.sink);
    }
    // Add user-defined operators.
    for (const auto& [udo, definition] : udos_) {
      auto def_str = try_as<std::string>(&definition);
      if (not def_str) {
        // Invalid UDOs are just ignored for `show operators`, as we don't care
        // about them here.
        continue;
      }
      auto diag = null_diagnostic_handler{};
      auto provider = session_provider::make(diag);
      auto op = parse_and_compile(*def_str, provider.as_session());
      if (op.is_error()) {
        diagnostic::warning("user-defined operator `{}` failed to parse", udo)
          .emit(ctrl.diagnostics());
        continue;
      }
      auto event = builder.record();
      const auto signature = (*op).infer_signature();
      event.field("name", udo);
      event.field("definition", *def_str);
      event.field("source", signature.source);
      event.field("transformation", signature.transformation);
      event.field("sink", signature.sink);
    }
    co_yield builder.finish_assert_one_slice("tenzir.operator");
  }

private:
  record udos_ = {};
};

} // namespace

} // namespace tenzir::plugins::operators

TENZIR_REGISTER_PLUGIN(tenzir::plugins::operators::plugin)
