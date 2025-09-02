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

namespace tenzir::plugins::routes::input {

namespace {

class input_operator final : public crtp_operator<input_operator> {
public:
  input_operator() = default;

  input_operator(located<std::string> name) noexcept : name_(std::move(name)) {
  }

  auto name() const -> std::string override {
    return "routes::input";
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {}; // signal readiness
    // Get the routes-manager actor from the registry
    const auto routes_manager = ctrl.self().system().registry()
      .get<routes_manager_actor>("routes-manager");
    // Send the proxy actor to the routes-manager
    auto input = named_input_actor{
      name_.inner,
      ctrl.self().spawn<caf::linked>(caf::actor_from_state<proxy>),
    };
    ctrl.self().mail(atom::add_v, input)
      .request(routes_manager, caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](const caf::error& err) {
          ctrl.set_waiting(false);
          diagnostic::error(err)
            .note("failed to register input with routes manager")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
    // Repeatedly get table slices from the proxy actor
    while (true) {
      auto slice = table_slice{};
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::get_v)
        .request(input.handle, caf::infinite)
        .then(
          [&](table_slice result) {
            ctrl.set_waiting(false);
            slice = std::move(result);
          },
          [&](const caf::error& err) {
            ctrl.set_waiting(false);
            diagnostic::error(err)
              .note("failed to get table slice from proxy")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      // TODO: Reconsider.
      // If we get an empty slice, we're done
      if (slice.rows() == 0) {
        break;
      }
      co_yield std::move(slice);
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
    // TODO: This could handle predicate pushdown if we wanted that to affect
    // metrics. Not sure.
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, input_operator& x) -> bool {
    return f.object(x).fields(f.field("name", x.name_));
  }

private:
  located<std::string> name_;
};

class plugin final : public virtual operator_plugin2<input_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto name = located<std::string>{};
    TRY(argument_parser2::operator_(this->name())
          .positional("name", name, "string")
          .parse(inv, ctx));
    return std::make_unique<input_operator>(std::move(name));
  }
};

} // namespace

} // namespace tenzir::plugins::routes::input

TENZIR_REGISTER_PLUGIN(tenzir::plugins::routes::input::plugin)
