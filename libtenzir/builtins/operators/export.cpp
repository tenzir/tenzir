//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/passive_partition.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/type.h>
#include <caf/event_based_actor.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace tenzir::plugins::export_ {

struct bridge_state {
  std::queue<table_slice> buffer = {};
  size_t num_buffered = {};
  caf::typed_response_promise<table_slice> rp = {};
  expression expr = {};
};

caf::behavior make_bridge(caf::stateful_actor<bridge_state>* self,
                          importer_actor importer, expression expr) {
  self->state.expr = std::move(expr);
  self
    ->request(importer, caf::infinite, atom::subscribe_v,
              caf::actor_cast<receiver_actor<table_slice>>(self))
    .then([]() {},
          [self](const caf::error& err) {
            self->quit(add_context(err, "failed to subscribe to importer"));
          });
  return {
    [self](table_slice slice) {
      auto filtered = filter(std::move(slice), self->state.expr);
      if (not filtered) {
        return;
      }
      if (self->state.rp.pending()) {
        self->state.rp.deliver(std::move(*filtered));
      } else if (self->state.num_buffered < (1 << 22)) {
        self->state.num_buffered += filtered->rows();
        self->state.buffer.push(std::move(*filtered));
      } else {
        TENZIR_WARN("`export --live` dropped {} events because it failed to "
                    "keep up",
                    filtered->rows());
      }
    },
    [self](atom::get) -> caf::result<table_slice> {
      if (self->state.rp.pending()) {
        return caf::make_error(ec::logic_error,
                               "live exporter bridge promise out of sync");
      }
      if (self->state.buffer.empty()) {
        self->state.rp = self->make_response_promise<table_slice>();
        return self->state.rp;
      }
      auto result = std::move(self->state.buffer.front());
      self->state.buffer.pop();
      self->state.num_buffered -= result.rows();
      return result;
    },
  };
};

struct export_mode {
  bool live = true;
  bool retro = false;

  export_mode() = default;
  export_mode(bool live_, bool retro_) : live{live_}, retro{retro_} {
    TENZIR_ASSERT(live || retro);
  }

  friend auto inspect(auto& f, export_mode& x) -> bool {
    return f.object(x).fields(f.field("live", x.live),
                              f.field("retro", x.retro));
  }
};

class export_operator final : public crtp_operator<export_operator> {
public:
  export_operator() = default;

  explicit export_operator(expression expr, export_mode mode)
    : expr_{std::move(expr)}, mode_{mode} {
  }

  auto
  run_live(operator_control_plane& ctrl, caf::scoped_actor& blocking_self) const
    -> generator<table_slice> {
    // TODO: Some of the the requests this operator makes are blocking, so we
    // have to create a scoped actor here; once the operator API uses async we
    // can offer a better mechanism here.
    auto components
      = get_node_components<importer_actor>(blocking_self, ctrl.node());
    if (!components) {
      diagnostic::error(components.error())
        .note("failed to get importer")
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield {};
    auto [importer] = std::move(*components);
    auto bridge = ctrl.self().spawn<caf::linked>(make_bridge, importer, expr_);
    while (true) {
      auto result = table_slice{};
      ctrl.set_waiting(true);
      ctrl.self()
        .request(bridge, caf::infinite, atom::get_v)
        .then(
          [&](table_slice& response) {
            ctrl.set_waiting(false);
            result = std::move(response);
          },
          [&](const caf::error& err) {
            diagnostic::error(err).emit(ctrl.diagnostics());
          });
      co_yield {};
      co_yield std::move(result);
    }
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: Some of the the requests this operator makes are blocking, so we
    // have to create a scoped actor here; once the operator API uses async we
    // can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    if (mode_.retro) {
      auto components
        = get_node_components<catalog_actor, accountant_actor, filesystem_actor>(
          blocking_self, ctrl.node());
      if (!components) {
        diagnostic::error(components.error())
          .note("failed to get importer")
          .emit(ctrl.diagnostics());
        co_return;
      }
      co_yield {};
      auto [catalog, accountant, fs] = std::move(*components);
      auto current_slice = std::optional<table_slice>{};
      auto query_context
        = tenzir::query_context::make_extract("export", blocking_self, expr_);
      query_context.id = uuid::random();
      TENZIR_DEBUG("export operator starts catalog lookup with id {} and "
                   "expression {}",
                   query_context.id, expr_);
      auto current_result = catalog_lookup_result{};
      ctrl.self()
        .request(catalog, caf::infinite, atom::candidates_v, query_context)
        .await(
          [&current_result](catalog_lookup_result result) {
            current_result = std::move(result);
          },
          [&ctrl](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to perform catalog lookup")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      for (const auto& [type, info] : current_result.candidate_infos) {
        auto bound_expr = tailor(info.exp, type);
        if (not bound_expr) {
          // failing to bind is not an error.
          continue;
        }
        query_context.expr = std::move(*bound_expr);
        for (const auto& partition_info : info.partition_infos) {
          const auto& uuid = partition_info.uuid;
          auto partition = blocking_self->spawn(
            passive_partition, uuid, accountant, fs,
            std::filesystem::path{"index"} / fmt::format("{:l}", uuid));
          auto receiving_slices = true;
          auto current_error = caf::error{};
          blocking_self->send(partition, atom::query_v, query_context);
          while (receiving_slices) {
            blocking_self->receive(
              [&current_slice](table_slice slice) {
                current_slice = std::move(slice);
              },
              [&receiving_slices](uint64_t) {
                receiving_slices = false;
              },
              [&receiving_slices, &current_error](caf::error e) {
                receiving_slices = false;
                current_error = std::move(e);
              });
            if (current_error) {
              diagnostic::warning(current_error).emit(ctrl.diagnostics());
              co_yield {};
              continue;
            }
            if (current_slice) {
              co_yield *current_slice;
              current_slice.reset();
            } else {
              co_yield {};
            }
          }
        }
      }
    }
    if (mode_.live) {
      for (auto x : run_live(ctrl, blocking_self)) {
        co_yield x;
      }
    }
  }

  auto name() const -> std::string override {
    return "export";
  }

  auto detached() const -> bool override {
    if (mode_.retro) {
      return true;
    }
    return false;
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto internal() const -> bool override {
    return true;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    auto clauses = std::vector<expression>{};
    if (expr_ != caf::none and expr_ != trivially_true_expression()) {
      clauses.push_back(expr_);
    }
    if (filter != caf::none and filter != trivially_true_expression()) {
      clauses.push_back(filter);
    }
    auto expr = clauses.empty() ? trivially_true_expression()
                                : expression{conjunction{std::move(clauses)}};
    return optimize_result{trivially_true_expression(), event_order::ordered,
                           std::make_unique<export_operator>(std::move(expr),
                                                             mode_)};
  }

  friend auto inspect(auto& f, export_operator& x) -> bool {
    return f.object(x).fields(f.field("expression", x.expr_),
                              f.field("mode", x.mode_));
  }

private:
  expression expr_;
  export_mode mode_;
};

class plugin final : public virtual operator_plugin<export_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"export", "https://docs.tenzir.com/"
                                            "operators/export"};
    bool live = false;
    bool retro = false;
    auto internal = false;
    auto low_priority = false;
    parser.add("--live", live);
    parser.add("--retro", retro);
    parser.add("--low-priority", low_priority);
    parser.add("--internal", internal);
    parser.parse(p);
    // The --low-priority option is currently a no-op, and will be brought back
    // alongside the database plugin.
    (void)low_priority;
    if (not live) {
      retro = true;
    }
    export_mode mode{live, retro};
    return std::make_unique<export_operator>(
      expression{
        predicate{
          meta_extractor{meta_extractor::internal},
          relational_operator::equal,
          data{internal},
        },
      },
      mode);
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto live = false;
    auto retro = false;
    auto internal = false;
    argument_parser2::operator_("export")
      .add("live", live)
      .add("retro", retro)
      .add("internal", internal)
      .parse(inv, ctx);
    if (not live) {
      // TODO: export live=false, retro=false
      retro = true;
    }
    return std::make_unique<export_operator>(
      expression{
        predicate{
          meta_extractor{meta_extractor::internal},
          relational_operator::equal,
          data{internal},
        },
      },
      export_mode{live, retro});
  }
};

} // namespace tenzir::plugins::export_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::plugin)
