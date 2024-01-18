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

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::diagnostics {

namespace {

class diagnostics_operator final : public crtp_operator<diagnostics_operator> {
public:
  diagnostics_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    std::vector<record> stored_diagnostics;
    ctrl.self()
      .request(ctrl.node(), caf::infinite, atom::get_v, atom::diagnostics_v)
      .await(
        [&](std::vector<record>& v) {
          stored_diagnostics = std::move(v);
        },
        [&](const caf::error& error) {
          diagnostic::error("failed to retrieve diagnostics from node: {}",
                            error)
            .docs("https://docs.tenzir.com/operators/diagnostics")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    auto b = series_builder{type{record_type{}}};
    for (auto&& diag : stored_diagnostics) {
      b.data(diag);
    }
    for (auto&& slice : b.finish_as_table_slice("tenzir.diagnostics")) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "diagnostics";
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto&, diagnostics_operator&) -> bool {
    return true;
  }
};

class plugin final : public virtual operator_plugin<diagnostics_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"diagnostics", "https://docs.tenzir.com/next/"
                                                 "operators/diagnostics"};
    // TODO: When we merge this into export, enable live diagnostics.
    // parser.add("--live", live);
    parser.parse(p);
    return std::make_unique<diagnostics_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::diagnostics

TENZIR_REGISTER_PLUGIN(tenzir::plugins::diagnostics::plugin)
