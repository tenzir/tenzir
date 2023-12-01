//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::partitions {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "partitions";
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
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
      ctrl.abort(std::move(components.error()));
      co_return;
    }
    co_yield {};
    auto [catalog] = std::move(*components);
    auto synopses = std::vector<partition_synopsis_pair>{};
    auto error = caf::error{};
    ctrl.self()
      .request(catalog, caf::infinite, atom::get_v)
      .await(
        [&synopses](std::vector<partition_synopsis_pair>& result) {
          synopses = std::move(result);
        },
        [&error](caf::error err) {
          error = std::move(err);
        });
    co_yield {};
    if (error) {
      ctrl.abort(std::move(error));
      co_return;
    }
    auto builder = series_builder{};
    using namespace tenzir::si_literals;
    constexpr auto max_rows = size_t{8_Ki};
    for (auto i = 0u; i < synopses.size(); ++i) {
      auto& synopsis = synopses[i];
      auto event = builder.record();
      event.field("uuid").data(fmt::to_string(synopsis.uuid));
      event.field("memusage").data(synopsis.synopsis->memusage());
      event.field("events").data(synopsis.synopsis->events);
      event.field("min_import_time").data(synopsis.synopsis->min_import_time);
      event.field("max_import_time").data(synopsis.synopsis->max_import_time);
      event.field("version").data(synopsis.synopsis->version);
      event.field("schema").data(synopsis.synopsis->schema.name());
      event.field("schema_id")
        .data(synopsis.synopsis->schema.make_fingerprint());
      event.field("internal")
        .data(synopsis.synopsis->schema.attribute("internal").has_value());
      if ((i + 1) % max_rows == 0) {
        for (auto&& result :
             builder.finish_as_table_slice("tenzir.partition")) {
          co_yield std::move(result);
        }
      }
    }
    if (synopses.size() % max_rows != 0) { // no empty slices
      for (auto&& result : builder.finish_as_table_slice("tenzir.partition")) {
        co_yield std::move(result);
      }
    }
  }
};

} // namespace

} // namespace tenzir::plugins::partitions

TENZIR_REGISTER_PLUGIN(tenzir::plugins::partitions::plugin)
