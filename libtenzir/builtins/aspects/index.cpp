//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/status.hpp>

#include <caf/actor_registry.hpp>
#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::index {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "index";
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    const auto index
      = ctrl.self().system().registry().get<index_actor>("tenzir.index");
    auto status = record{};
    ctrl.self()
      .mail(atom::status_v, status_verbosity::debug, duration::max())
      .request(index, caf::infinite)
      .then(
        [&](record& result) {
          status = std::move(result);
          ctrl.set_waiting(false);
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to get index status")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
    auto builder = series_builder{};
    builder.data(status);
    co_yield builder.finish_assert_one_slice("tenzir.index");
  }
};

} // namespace

} // namespace tenzir::plugins::index

TENZIR_REGISTER_PLUGIN(tenzir::plugins::index::plugin)
