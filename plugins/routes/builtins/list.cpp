//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/config.hpp"
#include "routes/routes_manager_actor.hpp"
#include "tenzir/series_builder.hpp"
#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <caf/actor_registry.hpp>
#include <algorithm>

namespace tenzir::plugins::routes::list {

namespace {

class list_operator final : public crtp_operator<list_operator> {
public:
  list_operator() noexcept = default;

  auto name() const -> std::string override {
    return "routes::list";
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {}; // signal readiness
    const auto router = ctrl.self().system().registry().get<routes_manager_actor>("routes-manager");
    // TODO: Consider having this operator subscribe to the routes manager
    // instead of fetching the state just once, and then returning a new value
    // whenever the config changes.
    auto cfg = std::optional<config>{};
    ctrl.self().mail(atom::list_v)
      .request(router, caf::infinite)
      .then(
        [&](config result) {
          ctrl.set_waiting(false);
          cfg = std::move(result);
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to get router config")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
    TENZIR_ASSERT(cfg);
    auto builder = series_builder{};
    builder.data(cfg->to_record());
    co_yield builder.finish_assert_one_slice("routes.config");
  }

  auto internal() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, list_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin2<list_operator> {
public:
  auto name() const -> std::string override {
    return "routes::list";
  };

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("routes::list")
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<list_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::routes::list

TENZIR_REGISTER_PLUGIN(tenzir::plugins::routes::list::plugin)
