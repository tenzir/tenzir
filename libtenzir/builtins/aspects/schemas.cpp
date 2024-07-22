//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::schemas {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "schemas";
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    // TODO: Some of the the requests this operator makes are blocking, so
    // we have to create a scoped actor here; once the operator API uses
    // async we can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    auto components
      = get_node_components<catalog_actor>(blocking_self, ctrl.node());
    if (!components) {
      diagnostic::error(components.error())
        .note("failed to get catalog")
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield {};
    auto [catalog] = std::move(*components);
    auto schemas = std::unordered_set<type>{};
    ctrl.self()
      .request(catalog, caf::infinite, atom::get_v)
      .await(
        [&](std::vector<partition_synopsis_pair>& synopses) {
          for (const auto& [id, synopsis] : synopses) {
            TENZIR_ASSERT(synopsis);
            TENZIR_ASSERT(synopsis->schema);
            schemas.insert(synopsis->schema);
          }
        },
        [&ctrl](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to get partitions")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    auto builder = series_builder{};
    for (const auto& schema : schemas) {
      builder.data(schema.to_definition());
      co_yield builder.finish_assert_one_slice(
        fmt::format("tenzir.schema.{}", schema.make_fingerprint()));
    }
  }
};

} // namespace

} // namespace tenzir::plugins::schemas

TENZIR_REGISTER_PLUGIN(tenzir::plugins::schemas::plugin)
