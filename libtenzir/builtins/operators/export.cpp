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
#include <tenzir/uuid.hpp>

#include <arrow/type.h>
#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace tenzir::plugins::export_ {

struct bridge_state {
  std::queue<table_slice> buffer;
  caf::typed_response_promise<table_slice> rp;
};

caf::behavior
make_bridge(caf::stateful_actor<bridge_state>* self, importer_actor importer) {
  self
    ->request(importer, caf::infinite,
              caf::actor_cast<stream_sink_actor<table_slice>>(self))
    .then([](caf::outbound_stream_slot<table_slice>) {},
          [self](caf::error err) {
            self->quit(err);
          });

  return {
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      return caf::attach_stream_sink(
               self, in,
               [](caf::unit_t&) {
                 // nop
               },
               [=](caf::unit_t&, table_slice slice) {
                 if (self->state.rp.pending())
                   self->state.rp.deliver(std::move(slice));
                 else
                   self->state.buffer.push(std::move(slice));
               },
               [=](caf::unit_t&, const caf::error& err) {
                 if (err)
                   TENZIR_ERROR("{} got error during streaming: {}", *self,
                                err);
               })
        .inbound_slot();
    },
    [self](atom::get) -> caf::result<table_slice> {
      if (self->state.rp.pending()) {
        return caf::make_error(ec::logic_error,
                               "live exporter bride promise out of sync.");
      }
      if (self->state.buffer.empty()) {
        self->state.rp = self->make_response_promise<table_slice>();
        return self->state.rp;
      }
      auto result = std::move(self->state.buffer.front());
      self->state.buffer.pop();
      return result;
    },
  };
};

class export_operator final : public crtp_operator<export_operator> {
public:
  export_operator() = default;

  explicit export_operator(expression expr, bool live)
    : expr_{std::move(expr)}, live_{live} {
  }

  auto run_live(operator_control_plane& ctrl) const -> generator<table_slice> {
    // TODO: Some of the the requests this operator makes are blocking, so we
    // have to create a scoped actor here; once the operator API uses async we
    // can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    auto components
      = get_node_components<importer_actor>(blocking_self, ctrl.node());
    if (!components) {
      ctrl.abort(std::move(components.error()));
      co_return;
    }
    co_yield {};
    auto [importer] = std::move(*components);
    auto bridge = ctrl.self().spawn(make_bridge, importer);
    table_slice next;
    while (true) {
      ctrl.self()
        .request(bridge, caf::infinite, atom::get_v)
        .await(
          [&next](table_slice& response) {
            next = std::move(response);
          },
          [&ctrl](caf::error e) {
            diagnostic::error("{}", e).emit(ctrl.diagnostics());
          });
      co_yield next;
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
      = get_node_components<catalog_actor, accountant_actor, filesystem_actor>(
        blocking_self, ctrl.node());
    if (!components) {
      ctrl.abort(std::move(components.error()));
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
    auto current_error = caf::error{};
    ctrl.self()
      .request(catalog, caf::infinite, atom::candidates_v, query_context)
      .await(
        [&current_result](catalog_lookup_result result) {
          current_result = std::move(result);
        },
        [&current_error](caf::error e) {
          current_error = std::move(e);
        });
    co_yield {};
    if (current_error) {
      ctrl.abort(std::move(current_error));
      co_return;
    }
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
        auto recieving_slices = true;
        blocking_self->send(partition, atom::query_v, query_context);
        while (recieving_slices) {
          blocking_self->receive(
            [&current_slice](table_slice slice) {
              current_slice = std::move(slice);
            },
            [&recieving_slices](uint64_t) {
              recieving_slices = false;
            },
            [&recieving_slices, &current_error](caf::error e) {
              recieving_slices = false;
              current_error = std::move(e);
            });
          if (current_error) {
            ctrl.warn(std::move(current_error));
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

  auto name() const -> std::string override {
    return "export";
  }

  auto detached() const -> bool override {
    if (live_)
      return false;
    return true;
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
    return optimize_result{trivially_true_expression(), event_order::ordered,
                           std::make_unique<export_operator>(std::move(expr),
                                                             live_)};
  }

  friend auto inspect(auto& f, export_operator& x) -> bool {
    return f.object(x).fields(f.field("expression", x.expr_),
                              f.field("live", x.live_));
  }

private:
  expression expr_;
  bool live_;
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
    parser.add("--live", live);
    parser.parse(p);
    return std::make_unique<export_operator>(trivially_true_expression(), live);
  }
};

} // namespace tenzir::plugins::export_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::plugin)
