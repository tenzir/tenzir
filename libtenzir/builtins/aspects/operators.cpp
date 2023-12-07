//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

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

  auto location() const -> operator_location override {
    return operator_location::anywhere;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto builder = series_builder{};
    // Add operator plugins.
    for (const auto* plugin : plugins::get<operator_parser_plugin>()) {
      auto event = builder.record();
      auto signature = plugin->signature();
      event.field("name", plugin->name());
      event.field("definition").null();
      event.field("source", signature.source);
      event.field("transformation", signature.transformation);
      event.field("sink", signature.sink);
    }
    // Add user-defined operators.
    for (const auto& [udo, definition] : udos_) {
      auto def_str = caf::get_if<std::string>(&definition);
      if (not def_str) {
        // Invalid UDOs are just ignored for `show operators`, as we don't care
        // about them here.
        continue;
      }
      auto op = pipeline::internal_parse(*def_str);
      if (not op) {
        diagnostic::warning("user-defined operator `{}` failed to parse: {}",
                            udo, op.error())
          .emit(ctrl.diagnostics());
        continue;
      }
      auto event = builder.record();
      event.field("name", udo);
      event.field("definition", *def_str);
      const auto void_output = op->infer_type<void>();
      const auto bytes_output = op->infer_type<chunk_ptr>();
      const auto events_output = op->infer_type<table_slice>();
      event.field("source", static_cast<bool>(void_output));
      event.field("transformation",
                  (bytes_output and not bytes_output->is<void>())
                    or (events_output and not events_output->is<void>()));
      event.field("sink", (void_output and void_output->is<void>())
                            or (bytes_output and bytes_output->is<void>())
                            or (events_output and events_output->is<void>()));
    }
    co_yield builder.finish_assert_one_slice("tenzir.operator");
  }

private:
  record udos_ = {};
};

} // namespace

} // namespace tenzir::plugins::operators

TENZIR_REGISTER_PLUGIN(tenzir::plugins::operators::plugin)
