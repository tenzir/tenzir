//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/status.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::index {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "index";
  }

  auto show(exec_ctx ctx) const -> generator<table_slice> override {
    // TODO: Some of the the requests this operator makes are blocking, so
    // we have to create a scoped actor here; once the operator API uses
    // async we can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctx.ctrl().self().system());
    auto components
      = get_node_components<index_actor>(blocking_self, ctx.ctrl().node());
    if (!components) {
      diagnostic::error(components.error())
        .note("failed to get index")
        .emit(ctx);
      co_return;
    }
    co_yield {};
    auto [index] = std::move(*components);
    auto status = record{};
    ctx.ctrl()
      .self()
      .request(index, caf::infinite, atom::status_v, status_verbosity::debug,
               duration::max())
      .await(
        [&](record& result) {
          status = std::move(result);
        },
        [&](const caf::error& err) {
          diagnostic::error(err).note("failed to get index status").emit(ctx);
        });
    co_yield {};
    auto builder = series_builder{};
    builder.data(status);
    co_yield builder.finish_assert_one_slice("tenzir.index");
  }
};

} // namespace

} // namespace tenzir::plugins::index

TENZIR_REGISTER_PLUGIN(tenzir::plugins::index::plugin)
