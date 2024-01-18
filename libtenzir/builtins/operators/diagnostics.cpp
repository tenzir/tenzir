//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <chrono>

namespace tenzir::plugins::diagnostics {

namespace {

class diagnostics_operator final : public crtp_operator<diagnostics_operator> {
public:
  diagnostics_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    std::vector<std::pair<std::chrono::system_clock::time_point, diagnostic>>
      stored_diagnostics;
    TENZIR_ERROR("requesting");
    auto blocking_self = caf::scoped_actor{ctrl.self().system()};
    ctrl.self()
      .request(ctrl.node(), caf::infinite, atom::get_v, atom::diagnostics_v)
      .await(
        [&](std::vector<
            std::pair<std::chrono::system_clock::time_point, diagnostic>>& v) {
          TENZIR_ERROR("recieved {}", v.size());
          stored_diagnostics = std::move(v);
          TENZIR_ERROR("moved");
        },
        [](caf::error) {
          TENZIR_ERROR("ouch");
        });
    co_yield {};
    TENZIR_ERROR("before builder");
    auto b = series_builder{type{record_type{}}};
    for (auto&& diag : stored_diagnostics) {
      TENZIR_ERROR("diag");
      auto r = b.record();
      r.field("ts", diag.first);
      r.field("message", diag.second.message);
      r.field("severity", fmt::to_string(diag.second.severity));
      auto notes = r.field("notes").list();
      for (const auto& note : diag.second.notes) {
        auto notes_r = notes.record();
        notes_r.field("kind", fmt::to_string(note.kind));
        notes_r.field("message", note.message);
      }
      auto annotations = r.field("annotations").list();
      for (const auto& anno : diag.second.annotations) {
        auto annos_r = annotations.record();
        annos_r.field("primary", anno.primary);
        annos_r.field("text", anno.text);
        annos_r.field("source", fmt::format("{:?}", anno.source));
      }
    }
    TENZIR_ERROR("yielding");
    for (auto&& slice : b.finish_as_table_slice("tenzir.diagnostics"))
      co_yield std::move(slice);
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
    // TODO: When we merge this into export, enable live export.
    // parser.add("--live", live);
    parser.parse(p);
    return std::make_unique<diagnostics_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::diagnostics

TENZIR_REGISTER_PLUGIN(tenzir::plugins::diagnostics::plugin)
