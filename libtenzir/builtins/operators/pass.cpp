//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::pass {

namespace {

using diag_debug_actor
  = caf::typed_actor<auto(table_slice)->caf::result<table_slice>>;

struct diag_debug_state {
  static constexpr inline const char* name = "diag-debug";

  diag_debug_actor::pointer self = {};
  receiver_actor<diagnostic> diagnostics = {};
};

auto diag_debug(diag_debug_actor::stateful_pointer<diag_debug_state> self,
                receiver_actor<diagnostic> diagnostics)
  -> diag_debug_actor::behavior_type {
  self->state.self = self;
  self->state.diagnostics = std::move(diagnostics);

  diagnostic::warning("inside spawn")
    .emit(shared_diagnostic_handler{*self, self->state.diagnostics});

  return {[self](table_slice x) -> caf::result<table_slice> {
    diagnostic::warning("inside behavior")
      .emit(shared_diagnostic_handler{*self, self->state.diagnostics});
    return x;
  }};
}

// Does nothing with the input.
class pass_operator final : public crtp_operator<pass_operator> {
public:
  auto operator()(generator<table_slice> x, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto handle
      = ctrl.self().spawn(diag_debug, ctrl.shared_diagnostics_receiver());
    for (auto&& slice : x) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // diagnostic::warning("test from pass").emit(ctrl.shared_diagnostics());
      table_slice s;
      ctrl.self()
        .request(handle, caf::infinite, slice)
        .await(
          [&](table_slice& slice) {
            s = std::move(slice);
          },
          [](const caf::error&) {});
      co_yield {};
      co_yield s;
    }
  }

  auto name() const -> std::string override {
    return "pass";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, pass_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<pass_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    argument_parser{"pass", "https://docs.tenzir.com/operators/pass"}.parse(p);
    return std::make_unique<pass_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::pass

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pass::plugin)
