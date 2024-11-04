//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_executor.hpp"
#include "tenzir/scope_linked.hpp"
#include "tenzir/tql2/exec.hpp"

#include <tenzir/tql2/plugin.hpp>

#include <deque>

namespace tenzir::plugins::load_balance {

namespace {

using load_balancer_actor = caf::typed_actor<
  //
  auto(atom::write, table_slice events)->caf::result<void>,
  //
  auto(atom::read)->caf::result<table_slice>>
  //
  ::extend_with<metrics_receiver_actor>
  //
  ::extend_with<receiver_actor<diagnostic>>;

struct load_balancer_state {
  [[maybe_unused]] static constexpr auto name = "load-balance-host";

  load_balancer_actor::pointer self{};
  shared_diagnostic_handler diagnostics;
  metrics_receiver_actor metrics;
  std::vector<pipeline_executor_actor> executors;
  std::deque<caf::typed_response_promise<table_slice>> reads;
  std::deque<std::pair<table_slice, caf::typed_response_promise<void>>> writes;
  bool finished = false;
  uint64_t operator_index{};
  uint64_t next_metrics_id = 0;
  detail::stable_map<std::pair<uint64_t, uint64_t>, uint64_t> metrics_id_map;

  void finish() {
    TENZIR_WARN("marking load_balancer as finished");
    finished = true;
    // If there are any outstanding reads, we know that there are no remaining
    // writes. Thus it's fine to mark all outstanding reads as done.
    TENZIR_WARN("marking {} outstanding reads as done", reads.size());
    for (auto& read : reads) {
      read.deliver(table_slice{});
    }
    reads.clear();
  }
};

class load_balance_source final : public crtp_operator<load_balance_source> {
public:
  load_balance_source() = default;

  explicit load_balance_source(load_balancer_actor load_balancer)
    : load_balancer_{std::move(load_balancer)} {
  }

  auto name() const -> std::string override {
    return "internal-load-balance-source";
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    TENZIR_WARN("beginning execution of load_balance_source");
    TENZIR_ASSERT(load_balancer_);
    while (true) {
      auto result = table_slice{};
      ctrl.set_waiting(true);
      ctrl.self()
        .request(load_balancer_, caf::infinite, atom::read_v)
        .then(
          [&](table_slice slice) {
            result = std::move(slice);
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            // TODO
            TENZIR_WARN("read failed: {}", err);
            diagnostic::error("load balancer read failed: {}", err)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      // We signal completion with an empty table slice.
      if (result.rows() == 0) {
        TENZIR_WARN("load_balance_source detected end");
        break;
      }
      TENZIR_WARN("load_balance_source read {} events", result.rows());
      co_yield std::move(result);
    }
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_balance_source& x) -> bool {
    return f.apply(x.load_balancer_);
  }

private:
  load_balancer_actor load_balancer_;
};

template <class Map, class Key, class F>
auto get_or_compute(Map& map, Key&& key, F&& f) -> decltype(auto) {
  class implicit_caller {
  public:
    explicit implicit_caller(F&& f) : f_{std::forward<F>(f)} {
    }

    explicit(false) operator typename Map::mapped_type() {
      return std::forward<F>(f_)();
    }

  private:
    F&& f_;
  };
  return map
    .try_emplace(std::forward<Key>(key), implicit_caller(std::forward<F>(f)))
    .first->second;
}

auto make_load_balancer(
  load_balancer_actor::stateful_pointer<load_balancer_state> self,
  std::vector<pipeline> pipes, shared_diagnostic_handler diagnostics,
  metrics_receiver_actor metrics, uint64_t operator_index, bool is_hidden,
  const node_actor& node) -> load_balancer_actor::behavior_type {
  TENZIR_WARN("spawning load balancer");
  self->attach_functor([] {
    TENZIR_WARN("destroyed load balancer");
  });
  self->state.self = self;
  self->state.diagnostics = std::move(diagnostics);
  self->state.metrics = std::move(metrics);
  self->state.operator_index = operator_index;
  self->state.executors.reserve(pipes.size());
  for (auto& pipe : pipes) {
    pipe.prepend(std::make_unique<load_balance_source>(self));
    auto has_terminal = false;
    // TODO: Link? Monitor?
    TENZIR_WARN("spawning inner executor");
    auto executor = self->spawn<caf::monitored>(
      pipeline_executor, pipe, self, self, node, has_terminal, is_hidden);
    executor->attach_functor([] {
      TENZIR_WARN("inner executor terminated");
    });
    self->request(executor, caf::infinite, atom::start_v)
      .then(
        []() {
          // TODO
          TENZIR_WARN("started pipeline executor successfully");
        },
        [](const caf::error& err) {
          // TODO: This should probably forward the diagnostic.
          TENZIR_WARN("failed to start pipeline executor: {}", err);
        });
    self->state.executors.push_back(std::move(executor));
  }
  self->set_exit_handler([self](caf::exit_msg& msg) {
    if (msg.reason != caf::exit_reason::user_shutdown) {
      // Unexpected error.
      TENZIR_WARN("load balancer got unexpected exit msg: {}", msg.reason);
      self->quit(msg.reason);
    }
    // Let sources know that we are done.
    self->state.finish();
    // Wait for the inner executors to terminate.
    // Or is this the wrong place?
    // We want to wait with the termination of the outer executor.
    // How can we enforce that? It probably assumes that it's done when the
    // generator returns. Or is it only after destruction of the generator?
    // self->quit(msg.reason);
  });
  self->set_down_handler([self](const caf::down_msg& msg) {
    auto it = std::ranges::find(self->state.executors, msg.source,
                                &pipeline_executor_actor::address);
    TENZIR_ASSERT(it != self->state.executors.end());
    self->state.executors.erase(it);
    // TODO: What if not finished?
    if (self->state.finished) {
      self->quit();
    }
  });
  return {
    [self](atom::write, table_slice events) -> caf::result<void> {
      // TODO: If `events` is too big, we could consider splitting it up.
      TENZIR_ASSERT(events.rows() > 0);
      if (not self->state.reads.empty()) {
        TENZIR_WARN("writing {} events directly", events.rows());
        self->state.reads.front().deliver(std::move(events));
        self->state.reads.pop_front();
        return {};
      }
      TENZIR_WARN("writing {} events delayed", events.rows());
      self->state.writes.emplace_back(std::move(events),
                                      self->make_response_promise<void>());
      return self->state.writes.back().second;
    },
    [self](atom::read) -> caf::result<table_slice> {
      if (not self->state.writes.empty()) {
        auto events = std::move(self->state.writes.front().first);
        TENZIR_WARN("reading {} events directly", events.rows());
        self->state.writes.front().second.deliver();
        self->state.writes.pop_front();
        return events;
      }
      if (self->state.finished) {
        return table_slice{};
      }
      TENZIR_WARN("reading events delayed");
      self->state.reads.emplace_back(
        self->make_response_promise<table_slice>());
      return self->state.reads.back();
    },
    [self](uint64_t op_index, uint64_t metric_index,
           type& schema) -> caf::result<void> {
      // TODO: Why does this allocate twice?
      auto id = get_or_compute(
        self->state.metrics_id_map, std::pair{op_index, metric_index}, [&] {
          TENZIR_WARN(
            "load_balancer allocates new metrics for {}/{} with {} ({})",
            op_index, metric_index, schema, schema.make_fingerprint());
          auto id = self->state.next_metrics_id;
          self->state.next_metrics_id += 1;
          return id;
        });
      return self->delegate(self->state.metrics, self->state.operator_index, id,
                            schema);
    },
    [self](uint64_t op_index, uint64_t metric_index,
           record& metric) -> caf::result<void> {
      auto id
        = self->state.metrics_id_map.find(std::pair{op_index, metric_index});
      TENZIR_ASSERT(id != self->state.metrics_id_map.end());
      return self->delegate(self->state.metrics, self->state.operator_index,
                            id->second, std::move(metric));
    },
    [](const operator_metric& op_metric) -> caf::result<void> {
      // There currently is no way to have subpipeline metrics.
      TENZIR_UNUSED(op_metric);
      return {};
    },
    [self](diagnostic& diagnostic) -> caf::result<void> {
      TENZIR_ASSERT(diagnostic.severity != severity::error);
      self->state.diagnostics.emit(std::move(diagnostic));
      return {};
    },
  };
}

class load_balance final : public crtp_operator<load_balance> {
public:
  load_balance() = default;

  explicit load_balance(std::vector<pipeline> pipes)
    : pipes_{std::move(pipes)} {
  }

  auto name() const -> std::string override {
    return "load_balance";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    // TODO: Handle empty list.
    // TODO: Re-batch before?
    auto load_balancer = scope_linked{ctrl.self().spawn<caf::linked>(
      make_load_balancer, pipes_, ctrl.shared_diagnostics(),
      ctrl.metrics_receiver(), ctrl.operator_index(), ctrl.is_hidden(),
      ctrl.node())};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // TODO: Is this form of back pressure what we want?
      ctrl.set_waiting(true);
      ctrl.self()
        .request(load_balancer.get(), caf::infinite, atom::write_v,
                 std::move(slice))
        .then(
          [&]() {
            TENZIR_WARN("successfully sent events to load_balancer");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error("failed to write data to load balancer")
              .note("reason: {}", err)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    // TODO: What if the generator is destroyed from the outside?
    // Then we will only call destructors. These destructors should make sure
    // that the inner executors terminate.
    TENZIR_WARN("waiting for termination of load_balancer");
    ctrl.set_waiting(true);
    load_balancer.get()->attach_functor([&] {
      // TODO: Do we know that `ctrl` still lives here?
      ctrl.set_waiting(false);
    });
    // TODO: Is this what we want?
    caf::anon_send_exit(load_balancer.get(), caf::exit_reason::user_shutdown);
    co_yield {};
    TENZIR_WARN("load_balance (probably) terminated");
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_balance& x) -> bool {
    return f.apply(x.pipes_);
  }

private:
  std::vector<pipeline> pipes_;
};

class plugin final : public virtual operator_plugin2<load_balance> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipes = std::vector<pipeline>{};
    for (auto& arg : inv.args) {
      auto pipe = std::get_if<ast::pipeline_expr>(&*arg.kind);
      TENZIR_ASSERT(pipe);
      TRY(auto compiled, compile(std::move(pipe->inner), ctx));
      auto output = compiled.infer_type<table_slice>();
      if (not output) {
        diagnostic::error("pipeline must take events as input")
          .primary(pipe->begin)
          .emit(ctx);
        return failure::promise();
      }
      if (not output->is<void>()) {
        diagnostic::error("pipeline must currently end with a sink")
          .primary(pipe->end)
          .emit(ctx);
        return failure::promise();
      }
      pipes.push_back(std::move(compiled));
    }
    return std::make_unique<load_balance>(std::move(pipes));
  }
};

} // namespace

} // namespace tenzir::plugins::load_balance

TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_balance::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::operator_inspection_plugin<
                       tenzir::plugins::load_balance::load_balance_source>)
