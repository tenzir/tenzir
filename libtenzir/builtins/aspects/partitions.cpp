//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::partitions {

namespace {

/// A type that represents a partition.
auto partition_type() -> type {
  return type{
    "tenzir.partition",
    record_type{
      {"uuid", string_type{}},
      {"memory_usage", uint64_type{}},
      {"min_import_time", time_type{}},
      {"max_import_time", time_type{}},
      {"version", uint64_type{}},
      {"schema", string_type{}},
      {"schema_id", string_type{}},
    },
  };
}

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
    auto builder = table_slice_builder{partition_type()};
    using namespace tenzir::si_literals;
    constexpr auto max_rows = size_t{8_Ki};
    for (auto i = 0u; i < synopses.size(); ++i) {
      auto& synopsis = synopses[i];
      if (not(builder.add(fmt::to_string(synopsis.uuid))
              && builder.add(uint64_t{synopsis.synopsis->memusage()})
              && builder.add(synopsis.synopsis->min_import_time)
              && builder.add(synopsis.synopsis->max_import_time)
              && builder.add(synopsis.synopsis->version)
              && builder.add(synopsis.synopsis->schema.name())
              && builder.add(synopsis.synopsis->schema.make_fingerprint()))) {
        diagnostic::error("failed to add partition entry")
          .emit(ctrl.diagnostics());
        co_return;
      }
      if ((i + 1) % max_rows == 0) {
        co_yield builder.finish();
        builder = std::exchange(builder, table_slice_builder{partition_type()});
      }
    }
    if (synopses.size() % max_rows != 0) // no empty slices
      co_yield builder.finish();
  }
};

} // namespace

} // namespace tenzir::plugins::partitions

TENZIR_REGISTER_PLUGIN(tenzir::plugins::partitions::plugin)
