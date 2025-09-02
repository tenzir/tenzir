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

#include "routes/routes_manager_actor.hpp"
#include "routes/proxy_actor.hpp"
#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>

namespace tenzir::plugins::routes::output {

namespace {

class output_operator final : public crtp_operator<output_operator> {
public:
  output_operator() = default;

  output_operator(located<std::string> name) noexcept : name_(std::move(name)) {
  }

  auto name() const -> std::string override {
    return "routes::output";
  }

  auto operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    co_yield {}; // signal readiness
    // Get the routes-manager actor from the registry
    const auto routes_manager = ctrl.self().system().registry()
      .get<routes_manager_actor>("routes-manager");
    // Send the proxy actor to the routes-manager
    auto output = named_output_actor{
      name_.inner,
      ctrl.self().spawn<caf::linked>(caf::actor_from_state<proxy>),
    };
    ctrl.self().mail(atom::add_v, output)
      .request(routes_manager, caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](const caf::error& err) {
          ctrl.set_waiting(false);
          diagnostic::error(err)
            .note("failed to register output with routes manager")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
    // Forward all table slices to the proxy
    for (const auto& batch : input) {
      if (batch.rows() == 0) {
        co_yield {};
        continue;
      }
      // Forward the table slice to the proxy actor
      ctrl.self().mail(atom::put_v, batch)
        .request(output.handle, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            ctrl.set_waiting(false);
            diagnostic::error(err)
              .note("failed to forward table slice to proxy")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
    // At the end of input, forward an empty table slice to signal end
    ctrl.self().mail(atom::put_v, table_slice{})
      .request(output.handle, caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](const caf::error& err) {
          ctrl.set_waiting(false);
          diagnostic::error(err)
            .note("failed to signal end of input to proxy")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
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
    // TODO: Could be unordered?
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, output_operator& x) -> bool {
    return f.object(x).fields(f.field("name", x.name_));
  }

private:
  located<std::string> name_;
};

class plugin final : public virtual operator_plugin2<output_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto name = located<std::string>{};
    TRY(argument_parser2::operator_(this->name())
          .positional("name", name, "string")
          .parse(inv, ctx));
    return std::make_unique<output_operator>(std::move(name));
  }
};

} // namespace

} // namespace tenzir::plugins::routes::output

TENZIR_REGISTER_PLUGIN(tenzir::plugins::routes::output::plugin)
