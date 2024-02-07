//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/query_cursor.hpp"

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
      if (not filtered)
        return;
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

class export_operator final : public crtp_operator<export_operator> {
public:
  export_operator() = default;

  explicit export_operator(expression expr, bool live, bool low_priority)
    : expr_{std::move(expr)}, live_{live}, low_priority_(low_priority) {
  }

  auto run_live(operator_control_plane& ctrl) const -> generator<table_slice> {
    // TODO: Some of the the requests this operator makes are blocking, so we
    // have to create a scoped actor here; once the operator API uses async we
    // can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
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
    auto bridge = ctrl.self().spawn(make_bridge, importer, expr_);
    auto next = table_slice{};
    while (true) {
      ctrl.self()
        .request(bridge, caf::infinite, atom::get_v)
        .await(
          [&next](table_slice& response) {
            next = std::move(response);
          },
          [&ctrl](caf::error e) {
            diagnostic::error(e).emit(ctrl.diagnostics());
          });
      co_yield std::move(next);
    }
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    if (live_) {
      for (auto x : run_live(ctrl)) {
        co_yield x;
      }
      co_return;
    }
    // TODO: Some of the the requests this operator makes are blocking, so we
    // have to create a scoped actor here; once the operator API uses async we
    // can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    auto components
      = get_node_components<index_actor>(blocking_self, ctrl.node());
    if (!components) {
      diagnostic::error(components.error())
        .note("failed to get importer")
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield {};
    auto [index] = std::move(*components);
    auto query_context
      = tenzir::query_context::make_extract("export", blocking_self, expr_);
    query_context.priority = low_priority_ ? query_context::priority::low
                                           : query_context::priority::normal;
    auto query_cursor = tenzir::query_cursor{};
    ctrl.self()
      .request(index, caf::infinite, atom::evaluate_v, query_context)
      .await(
        [&query_cursor](tenzir::query_cursor cursor) {
          query_cursor = cursor;
        },
        [&ctrl](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to perform catalog lookup")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    if (query_cursor.candidate_partitions == 0) {
      co_return;
    }
    auto inflight_partitions = query_cursor.scheduled_partitions;
    TENZIR_DEBUG("export operator got {}/{} partitions ({} in flight)",
                 query_cursor.scheduled_partitions,
                 query_cursor.candidate_partitions, inflight_partitions);
    auto current_slice = std::optional<table_slice>{};
    while (true) {
      if (inflight_partitions == 0) {
        if (query_cursor.scheduled_partitions
            == query_cursor.candidate_partitions) {
          break;
        }
        constexpr auto BATCH_SIZE = uint32_t{1};
        ctrl.self()
          .request(index, caf::infinite, atom::query_v, query_cursor.id,
                   BATCH_SIZE)
          .await(
            [&]() {
              query_cursor.scheduled_partitions += BATCH_SIZE;
              inflight_partitions += BATCH_SIZE;
              TENZIR_DEBUG(
                "export operator got {}/{} partitions ({} in flight)",
                query_cursor.scheduled_partitions,
                query_cursor.candidate_partitions, inflight_partitions);
            },
            [&](const caf::error& err) {
              diagnostic::error(err)
                .note("failed to request further results")
                .emit(ctrl.diagnostics());
            });
        co_yield {};
      }
      while (inflight_partitions > 0) {
        blocking_self->receive(
          [&](table_slice& slice) {
            current_slice = std::move(slice);
          },
          [&](atom::done) {
            inflight_partitions = 0;
          },
          [&](const caf::error& err) {
            diagnostic::warning(err).emit(ctrl.diagnostics());
            inflight_partitions = 0;
          });
        if (current_slice) {
          co_yield std::move(*current_slice);
          current_slice.reset();
        } else {
          co_yield {};
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "export";
  }

  auto detached() const -> bool override {
    return !live_;
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
    if (live_)
      return do_not_optimize(*this);
    auto clauses = std::vector<expression>{};
    if (expr_ != caf::none and expr_ != trivially_true_expression()) {
      clauses.push_back(expr_);
    }
    if (filter != caf::none and filter != trivially_true_expression()) {
      clauses.push_back(filter);
    }
    auto expr = clauses.empty() ? trivially_true_expression()
                                : expression{conjunction{std::move(clauses)}};
    return optimize_result{
      trivially_true_expression(), event_order::ordered,
      std::make_unique<export_operator>(std::move(expr), live_, low_priority_)};
  }

  friend auto inspect(auto& f, export_operator& x) -> bool {
    return f.object(x).fields(f.field("expression", x.expr_),
                              f.field("live", x.live_));
  }

private:
  expression expr_;
  bool live_;
  bool low_priority_;
};

class plugin final : public virtual operator_plugin<export_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"export", "https://docs.tenzir.com/next/"
                                            "operators/sources/export"};
    bool live = false;
    bool low_priority = false;
    auto internal = false;
    parser.add("--live", live);
    parser.add("--internal", internal);
    // TODO: Ideally this should be one level further up, ie.
    // `tenzir --low-priority <pipeline>`
    parser.add("--low-priority", low_priority);
    parser.parse(p);
    return std::make_unique<export_operator>(
      expression{
        predicate{
          meta_extractor{meta_extractor::internal},
          relational_operator::equal,
          data{internal},
        },
      },
      live, low_priority);
  }
};

} // namespace tenzir::plugins::export_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::plugin)
