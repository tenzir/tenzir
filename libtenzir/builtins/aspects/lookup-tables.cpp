//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/series_builder.hpp"
#include "tenzir/status.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>

#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::lookup {

namespace {

class table_aspect_plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "lookup-tables";
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
    auto lookup_table_supervisor = caf::actor{};
    auto error = caf::error{};
    blocking_self
      ->request(ctrl.node(), caf::infinite, atom::get_v, atom::label_v,
                "lookup-table-supervisor")
      .receive(
        [&lookup_table_supervisor](caf::actor actor) {
          lookup_table_supervisor = actor;
        },
        [&error](caf::error& err) {
          error = std::move(err);
        });
    if (error) {
      TENZIR_ERROR(error);
      ctrl.abort(std::move(error));
      TENZIR_ERROR("aberoted");
      co_return;
    }
    co_yield {};
    TENZIR_ERROR("What the fuck");
    auto lookup_table_record = record{};
    blocking_self
      ->request(lookup_table_supervisor, caf::infinite, atom::status_v,
                status_verbosity::detailed, duration{})
      .receive(
        [&lookup_table_record](record& result) {
          lookup_table_record = std::move(result);
        },
        [&error](caf::error& err) {
          error = std::move(err);
        });
    if (error) {
      ctrl.abort(std::move(error));
      co_return;
    }
    auto builder = series_builder{};
    const auto& lookup_tables = get<list>(lookup_table_record, "lookup_tables");
    for (const auto& lookup_table : lookup_tables) {
      builder.data(caf::get<record>(lookup_table));
    }
    for (auto&& slice : builder.finish_as_table_slice("tenzir.lookup-table")) {
      co_yield std::move(slice);
    }
  }
};

} // namespace

} // namespace tenzir::plugins::lookup

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lookup::table_aspect_plugin)
