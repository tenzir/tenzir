//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>

namespace tenzir::plugins::operators {

namespace {

/// A type that represents an operator.
auto operator_type() -> type {
  return type{
    "tenzir.operator",
    record_type{
      {"name", string_type{}},
      {"source", bool_type{}},
      {"transformation", bool_type{}},
      {"sink", bool_type{}},
    },
  };
}

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "operators";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto builder = table_slice_builder{operator_type()};
    for (const auto* plugin : plugins::get<operator_parser_plugin>()) {
      auto signature = plugin->signature();
      if (not(builder.add(plugin->name()) && builder.add(signature.source)
              && builder.add(signature.transformation)
              && builder.add(signature.sink))) {
        diagnostic::error("failed to add operator").emit(ctrl.diagnostics());
        co_return;
      }
    }
    co_yield builder.finish();
  }
};

} // namespace

} // namespace tenzir::plugins::operators

TENZIR_REGISTER_PLUGIN(tenzir::plugins::operators::plugin)
