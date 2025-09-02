//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <caf/actor_registry.hpp>

#include "routes/routes_manager_actor.hpp"
#include "routes/config.hpp"

namespace tenzir::plugins::routes::sync {

namespace {

class sync_operator final : public crtp_operator<sync_operator> {
public:
  sync_operator() noexcept = default;

  auto name() const -> std::string override {
    return "routes::sync";
  }

  auto operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    const auto router = ctrl.self().system().registry().get<routes_manager_actor>("routes-manager");
    auto sp = session_provider::make(ctrl.diagnostics());
    auto ctx = sp.as_session();
    for (const auto& batch : input) {
      if (batch.rows() == 0) {
        co_yield {};
        continue;
      }
      // Process each incoming table slice (batch)
      for (const auto& record : batch.values()) {
        auto cfg = config::make(record, ctx);
        if (not cfg) {
          continue;
        }
        ctrl.self().mail(atom::update_v, std::move(*cfg))
          .request(router, caf::infinite)
          .then(
            [&] {
              ctrl.set_waiting(false);
            },
            [&](const caf::error& err) {
              diagnostic::error(err)
                .note("failed to update router config")
                .emit(ctrl.diagnostics());
            });
        ctrl.set_waiting(true);
        co_yield {};
      }
    }
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

  friend auto inspect(auto& f, sync_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin2<sync_operator> {
public:
  auto name() const -> std::string override {
    return "routes::sync";
  };

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("routes::sync")
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<sync_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::routes::sync

TENZIR_REGISTER_PLUGIN(tenzir::plugins::routes::sync::plugin)
