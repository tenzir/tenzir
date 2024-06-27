//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/execution_node.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/detail/weak_handle.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/table_slice.hpp"

#include <arrow/config.h>
#include <arrow/util/byte_size.h>
#include <caf/downstream.hpp>
#include <caf/exit_reason.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

namespace tenzir {

namespace {

using namespace std::chrono_literals;
using namespace si_literals;

template <class Element = void>
struct defaults {
  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_batch_size = 1;

  /// Defines the upper bound for the inbound buffer of the execution node.
  inline static constexpr uint64_t max_buffered = 0;

  /// Defines the time interval for sending metrics of the currently running
  /// pipeline operator.
  inline static constexpr auto metrics_interval = 1000ms;
};

template <>
struct defaults<table_slice> : defaults<> {
  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_batch_size = 8_Ki;

  /// Defines the upper bound for the inbound buffer of the execution node.
  inline static constexpr uint64_t max_buffered = 254_Ki;
};

template <>
struct defaults<chunk_ptr> : defaults<> {
  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_batch_size = 128_Ki;

  /// Defines the upper bound for the inbound buffer of the execution node.
  inline static constexpr uint64_t max_buffered = 4_Mi;
};

} // namespace

namespace {

template <class... Duration>
  requires(std::is_same_v<Duration, duration> && ...)
auto make_timer_guard(Duration&... elapsed) {
  return caf::detail::make_scope_guard(
    [&, start_time = std::chrono::steady_clock::now()] {
      const auto delta = std::chrono::steady_clock::now() - start_time;
      ((void)(elapsed += delta, true), ...);
    });
}

// Return an underestimate for the total number of referenced bytes for a vector
// of table slices, excluding the schema and disregarding any overlap or custom
// information from extension types.
auto approx_bytes(const table_slice& events) -> uint64_t {
  if (events.rows() == 0)
    return 0;
  auto record_batch = to_record_batch(events);
  TENZIR_ASSERT(record_batch);
  // Note that this function can sometimes fail. Because we ultimately want to
  // return an underestimate for the value of bytes, we silently fall back to
  // a value of zero if the referenced buffer size cannot be measured.
  //
  // As a consequence, the result of this function can be off by a large
  // margin. It never overestimates, but sometimes the result is a lot smaller
  // than you would think and also a lot smaller than it should be.
  //
  // We opted to use the built-in Arrow solution here hoping that it will be
  // improved upon in the future upsrream, rather than us having to roll our
  // own.
  //
  // We cannot feasibly warn for failure here as that would cause a lot of
  // noise.
  return detail::narrow_cast<uint64_t>(
    arrow::util::ReferencedBufferSize(*record_batch).ValueOr(0));
}

auto approx_bytes(const chunk_ptr& bytes) -> uint64_t {
  return bytes ? bytes->size() : 0;
}

template <class Input, class Output>
struct exec_node_state;

template <class Input, class Output>
struct exec_node_diagnostic_handler final : public diagnostic_handler {
  exec_node_diagnostic_handler(
    exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self,
    receiver_actor<diagnostic> handle)
    : self{self}, handle{std::move(handle)} {
  }

  void emit(diagnostic diag) override {
    TENZIR_TRACE("{} {} emits diagnostic: {:?}", *self, self->state.op->name(),
                 diag);
    if (diag.severity == severity::error) {
      throw std::move(diag);
    }
    self->send(handle, std::move(diag));
  }

  exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self = {};
  receiver_actor<diagnostic> handle = {};
};

template <class Input, class Output>
struct exec_node_control_plane final : public operator_control_plane {
  exec_node_control_plane(
    exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self,
    receiver_actor<diagnostic> diagnostic_handler, bool has_terminal)
    : state{self->state},
      diagnostic_handler{
        std::make_unique<exec_node_diagnostic_handler<Input, Output>>(
          self, std::move(diagnostic_handler))},
      has_terminal_{has_terminal} {
  }

  auto self() noexcept -> exec_node_actor::base& override {
    return *state.self;
  }

  auto node() noexcept -> node_actor override {
    return state.weak_node.lock();
  }

  auto diagnostics() noexcept -> diagnostic_handler& override {
    return *diagnostic_handler;
  }

  auto metrics() noexcept -> metric_handler& override {
    return metric_handler;
  }

  auto no_location_overrides() const noexcept -> bool override {
    return caf::get_or(content(state.self->config()),
                       "tenzir.no-location-overrides", false);
  }

  auto has_terminal() const noexcept -> bool override {
    return has_terminal_;
  }

  auto set_waiting(bool value) noexcept -> void override {
    state.waiting = value;
    if (not state.waiting) {
      state.schedule_run(false);
    }
  }

  exec_node_state<Input, Output>& state;
  std::unique_ptr<exec_node_diagnostic_handler<Input, Output>> diagnostic_handler
    = {};
  metric_handler metric_handler = {};
  bool has_terminal_;
};

auto size(const table_slice& slice) -> uint64_t {
  return slice.rows();
}

auto size(const chunk_ptr& chunk) -> uint64_t {
  return chunk ? chunk->size() : 0;
}

template <class Input, class Output>
struct exec_node_state {
  static constexpr auto name = "exec-node";

  /// A pointer to the parent actor.
  exec_node_actor::pointer self = {};

  /// The operator owned by this execution node.
  operator_ptr op = {};

  /// The instance created by the operator. Must be created at most once.
  struct resumable_generator {
    generator<Output> gen = {};
    generator<Output>::iterator it = {};
  };
  std::optional<resumable_generator> instance = {};

  /// State required for keeping and sending metrics.
  std::chrono::steady_clock::time_point start_time
    = std::chrono::steady_clock::now();
  receiver_actor<metric> metrics_handler = {};
  metric metrics = {};

  /// Whether this execution node is paused, and when it was.
  std::optional<std::chrono::steady_clock::time_point> paused_at = {};

  /// Whether this execution node is currently waiting for a response.
  bool waiting = {};

  /// A handle to the previous execution node.
  exec_node_actor previous = {};

  /// The inbound buffer.
  std::vector<Input> inbound_buffer = {};
  uint64_t inbound_buffer_size = {};

  /// The currently open demand.
  struct demand {
    caf::typed_response_promise<void> rp = {};
    exec_node_sink_actor sink = {};
    uint64_t remaining = {};
  };
  std::optional<struct demand> demand = {};
  bool issue_demand_inflight = {};

  caf::typed_response_promise<void> start_rp = {};

  /// Exponential backoff for scheduling.
  static constexpr duration min_backoff = std::chrono::milliseconds{250};
  static constexpr duration max_backoff = std::chrono::minutes{1};
  static constexpr double backoff_rate = 2.0;
  duration backoff = duration::zero();
  caf::disposable backoff_disposable = {};

  /// A pointer to te operator control plane passed to this operator during
  /// execution, which acts as an escape hatch to this actor.
  std::unique_ptr<exec_node_control_plane<Input, Output>> ctrl = {};

  /// A weak handle to the node actor.
  detail::weak_handle<node_actor> weak_node = {};

  /// Whether the next run of the internal run loop for this execution node has
  /// already been scheduled.
  bool run_scheduled = {};

  /// Tracks whether the current run has produced an output and consumed an
  /// input, respectively.
  bool consumed_input = false;
  bool produced_output = false;

  ~exec_node_state() noexcept {
    TENZIR_DEBUG("{} {} shut down", *self, op->name());
    instance.reset();
    ctrl.reset();
    emit_metrics();
    if (demand and demand->rp.pending()) {
      demand->rp.deliver();
    }
    if (start_rp.pending()) {
      // TODO: This should probably never happen, as it means that we do not
      // deliver a diagnostic.
      TENZIR_WARN("reached pending `start_rp` in exec node destructor");
      start_rp.deliver(ec::silent);
    }
  }

  auto emit_metrics() -> void {
    const auto now = std::chrono::steady_clock::now();
    auto metrics_copy = metrics;
    if (paused_at) {
      metrics_copy.time_paused
        += std::chrono::duration_cast<duration>(now - *paused_at);
    }
    metrics_copy.time_total
      = std::chrono::duration_cast<duration>(now - start_time);
    metrics_copy.time_running
      = metrics_copy.time_total - metrics_copy.time_paused;
    caf::anon_send(metrics_handler, std::move(metrics_copy));
  }

  auto start(std::vector<caf::actor> all_previous) -> caf::result<void> {
    TENZIR_DEBUG("{} {} received start request", *self, op->name());
    detail::weak_run_delayed_loop(self, defaults<>::metrics_interval, [this] {
      auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
      emit_metrics();
    });
    if (instance.has_value()) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} was already started", *self));
    }
    if constexpr (std::is_same_v<Input, std::monostate>) {
      if (not all_previous.empty()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} runs a source operator and must "
                                           "not have a previous exec-node",
                                           *self));
      }
      self->set_exit_handler([this](const caf::exit_msg& msg) {
        TENZIR_DEBUG("{} {} got exit message from the next execution node or "
                     "its executor with address {}: {}",
                     *self, op->name(), msg.source, msg.reason);
        on_error(msg.reason);
      });
    } else {
      // The previous exec-node must be set when the operator is not a source.
      if (all_previous.empty()) {
        return caf::make_error(
          ec::logic_error, fmt::format("{} runs a transformation/sink operator "
                                       "and must have a previous exec-node",
                                       *self));
      }
      previous
        = caf::actor_cast<exec_node_actor>(std::move(all_previous.back()));
      all_previous.pop_back();
      self->link_to(previous);
      self->set_exit_handler([this, prev_addr = previous.address()](
                               const caf::exit_msg& msg) {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        // We got an exit message, which can mean one of four things:
        // 1. The pipeline manager quit.
        // 2. The next operator quit.
        // 3. The previous operator quit gracefully.
        // 4. The previous operator quit ungracefully.
        // In cases (1-3) we need to shut down this operator unconditionally.
        // For (4) we we need to treat the previous operator as offline.
        if (msg.source != prev_addr) {
          TENZIR_DEBUG("{} {} got exit message from the next execution node or "
                       "its executor with address {}: {}",
                       *self, op->name(), msg.source, msg.reason);
          on_error(msg.reason);
          return;
        }
        TENZIR_DEBUG("{} {} got exit message from previous execution node with "
                     "address {}: {}",
                     *self, op->name(), msg.source, msg.reason);
        if (msg.reason and msg.reason != caf::exit_reason::unreachable) {
          on_error(msg.reason);
          return;
        }
        previous = nullptr;
        schedule_run(false);
      });
    }
    // Instantiate the operator with its input type.
    {
      auto time_scheduled_guard = make_timer_guard(metrics.time_processing);
      auto output_generator = op->instantiate(make_input_adapter(), *ctrl);
      if (not output_generator) {
        TENZIR_DEBUG("{} {} failed to instantiate operator: {}", *self,
                     op->name(), output_generator.error());
        return add_context(output_generator.error(),
                           "{} {} failed to instantiate operator", *self,
                           op->name());
      }
      if (not std::holds_alternative<generator<Output>>(*output_generator)) {
        return caf::make_error(
          ec::logic_error, fmt::format("{} expected {}, but got {}", *self,
                                       operator_type_name<Output>(),
                                       operator_type_name(*output_generator)));
      }
      instance.emplace();
      instance->gen = std::get<generator<Output>>(std::move(*output_generator));
      instance->it = instance->gen.begin();
      if (self->getf(caf::abstract_actor::is_shutting_down_flag)) {
        return {};
      }
      // Emit metrics once to get started.
      emit_metrics();
      if (instance->it == instance->gen.end()) {
        TENZIR_TRACE("{} {} finished without yielding", *self, op->name());
        if (previous) {
          // If a transformation or sink operator finishes without yielding,
          // preceding operators effectively dangle because they are set up but
          // never receive any demand. We need to explicitly shut them down to
          // avoid a hang.
          self->send_exit(previous, caf::exit_reason::unreachable);
        }
        self->quit();
        return {};
      }
    }
    if constexpr (detail::are_same_v<std::monostate, Input, Output>) {
      schedule_run(false);
      return {};
    }
    if constexpr (std::is_same_v<Output, std::monostate>) {
      start_rp = self->make_response_promise<void>();
      self
        ->request(previous, caf::infinite, atom::start_v,
                  std::move(all_previous))
        .then(
          [this]() {
            auto time_starting_guard
              = make_timer_guard(metrics.time_scheduled, metrics.time_starting);
            TENZIR_TRACE("{} {} schedules run after successful startup of all "
                         "operators",
                         *self, op->name());
            schedule_run(false);
            start_rp.deliver();
          },
          [this](const caf::error& error) {
            auto time_starting_guard
              = make_timer_guard(metrics.time_scheduled, metrics.time_starting);
            TENZIR_DEBUG("{} {} forwards error during startup: {}", *self,
                         op->name(), error);
            start_rp.deliver(error);
          });
      return start_rp;
    }
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      TENZIR_DEBUG("{} {} delegates start to {}", *self, op->name(), previous);
      return self->delegate(previous, atom::start_v, std::move(all_previous));
    }
    return {};
  }

  auto pause() -> caf::result<void> {
    if (paused_at) {
      return {};
    }
    TENZIR_DEBUG("{} {} pauses execution", *self, op->name());
    paused_at = std::chrono::steady_clock::now();
    return {};
  }

  auto resume() -> caf::result<void> {
    if (not paused_at) {
      return {};
    }
    TENZIR_DEBUG("{} {} resumes execution", *self, op->name());
    metrics.time_paused += std::chrono::duration_cast<duration>(
      std::chrono::steady_clock::now() - *paused_at);
    paused_at.reset();
    schedule_run(false);
    return {};
  }

  auto advance_generator() -> void {
    auto time_processing_guard = make_timer_guard(metrics.time_processing);
    if constexpr (std::is_same_v<Output, std::monostate>) {
      // We never issue demand to the sink, so we cannot be at the end of the
      // generator here.
      TENZIR_ASSERT(instance->it != instance->gen.end());
      TENZIR_TRACE("{} {} processes", *self, op->name());
      ++instance->it;
      if (self->getf(caf::abstract_actor::is_shutting_down_flag)) {
        return;
      }
      if (instance->it == instance->gen.end()) {
        TENZIR_DEBUG("{} {} completes processing", *self, op->name());
        self->quit();
      }
      return;
    } else {
      if (not demand or instance->it == instance->gen.end()) {
        return;
      }
      TENZIR_ASSERT(instance);
      TENZIR_TRACE("{} {} processes", *self, op->name());
      auto output = std::move(*instance->it);
      const auto output_size = size(output);
      ++instance->it;
      if (self->getf(caf::abstract_actor::is_shutting_down_flag)) {
        return;
      }
      const auto should_quit = instance->it == instance->gen.end();
      if (output_size == 0) {
        if (should_quit) {
          self->quit();
        }
        return;
      }
      produced_output = true;
      metrics.outbound_measurement.num_elements += output_size;
      metrics.outbound_measurement.num_batches += 1;
      metrics.outbound_measurement.num_approx_bytes += approx_bytes(output);
      TENZIR_TRACE("{} {} produced and pushes {} elements", *self, op->name(),
                   output_size);
      if (demand->remaining <= output_size) {
        demand->remaining = 0;
      } else {
        // TODO: Should we make demand->remaining available in the operator
        // control plane?
        demand->remaining -= output_size;
      }
      self
        ->request(demand->sink, caf::infinite, atom::push_v, std::move(output))
        .then(
          [this, output_size, should_quit]() {
            auto time_scheduled_guard
              = make_timer_guard(metrics.time_scheduled);
            TENZIR_TRACE("{} {} pushed {} elements", *self, op->name(),
                         output_size);
            if (demand and demand->remaining == 0) {
              demand->rp.deliver();
              demand.reset();
            }
            if (should_quit) {
              TENZIR_TRACE("{} {} completes processing", *self, op->name());
              if (demand and demand->rp.pending()) {
                demand->rp.deliver();
              }
              self->quit();
              return;
            }
            schedule_run(false);
          },
          [this, output_size](const caf::error& err) {
            TENZIR_DEBUG("{} {} failed to push {} elements", *self, op->name(),
                         output_size);
            auto time_scheduled_guard
              = make_timer_guard(metrics.time_scheduled);
            if (err == caf::sec::request_receiver_down) {
              if (demand and demand->rp.pending()) {
                demand->rp.deliver();
              }
              self->quit();
              return;
            }
            diagnostic::error(err)
              .note("{} {} failed to push to next execution node", *self,
                    op->name())
              .emit(ctrl->diagnostics());
          });
    }
  }

  auto make_input_adapter() -> std::monostate
    requires std::is_same_v<Input, std::monostate>
  {
    return {};
  }

  auto make_input_adapter() -> generator<Input>
    requires(not std::is_same_v<Input, std::monostate>)
  {
    while (previous or not inbound_buffer.empty()) {
      if (inbound_buffer.empty()) {
        co_yield {};
        continue;
      }
      consumed_input = true;
      auto input = std::move(inbound_buffer.front());
      inbound_buffer.erase(inbound_buffer.begin());
      const auto input_size = size(input);
      inbound_buffer_size -= input_size;
      TENZIR_TRACE("{} {} uses {} elements", *self, op->name(), input_size);
      co_yield std::move(input);
    }
    TENZIR_DEBUG("{} {} reached end of input", *self, op->name());
  }

  auto schedule_run(bool use_backoff) -> void {
    // Edge case: If a run with backoff is currently scheduled, but we now want
    // a run without backoff, we can replace the scheduled run with a new one.
    if (not backoff_disposable.disposed() and not use_backoff) {
      backoff_disposable.dispose();
      run_scheduled = false;
    }
    // Check whether we're already scheduled to run, or are no longer allowed to
    // rum.
    if (run_scheduled) {
      return;
    }
    if (not use_backoff) {
      backoff = duration::zero();
    } else if (backoff == duration::zero()) {
      backoff = min_backoff;
    } else {
      backoff
        = std::min(std::chrono::duration_cast<duration>(backoff_rate * backoff),
                   max_backoff);
    }
    TENZIR_TRACE("{} {} schedules run with a delay of {}", *self, op->name(),
                 data{backoff});
    run_scheduled = true;
    if (backoff == duration::zero()) {
      self->send(self, atom::internal_v, atom::run_v);
    } else {
      backoff_disposable = detail::weak_run_delayed(self, backoff, [this] {
        self->send(self, atom::internal_v, atom::run_v);
      });
    }
  }

  auto internal_run() -> caf::result<void> {
    run_scheduled = false;
    run();
    return {};
  }

  auto issue_demand() -> void {
    if (not previous
        or inbound_buffer_size + defaults<Input>::min_batch_size
             > defaults<Input>::max_buffered
        or issue_demand_inflight) {
      return;
    }
    const auto demand = defaults<Input>::max_buffered - inbound_buffer_size;
    TENZIR_TRACE("{} {} issues demand for up to {} elements", *self, op->name(),
                 demand);
    issue_demand_inflight = true;
    self
      ->request(previous, caf::infinite, atom::pull_v,
                static_cast<exec_node_sink_actor>(self),
                detail::narrow_cast<uint64_t>(demand))
      .then(
        [this] {
          auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
          TENZIR_TRACE("{} {} had its demand fulfilled", *self, op->name());
          issue_demand_inflight = false;
          schedule_run(false);
        },
        [this](const caf::error& err) {
          auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
          TENZIR_DEBUG("{} {} failed to get its demand fulfilled: {}", *self,
                       op->name(), err);
          issue_demand_inflight = false;
          if (err != caf::sec::request_receiver_down) {
            diagnostic::error(err)
              .note("{} {} failed to pull from previous execution node", *self,
                    op->name())
              .emit(ctrl->diagnostics());
          } else {
            schedule_run(false);
          }
        });
  }

  auto run() -> void {
    if (waiting or paused_at or not instance) {
      return;
    }
    TENZIR_TRACE("{} {} enters run loop", *self, op->name());
    // If the inbound buffer is below its capacity, we must issue demand
    // upstream.
    issue_demand();
    // Advance the operator's generator.
    advance_generator();
    // We can continue execution under the following circumstances:
    // 1. The operator's generator is not yet completed.
    // 2. The operator did not signal that we're supposed to wait.
    // 3. The operator has one of the four following reasons to do work:
    //   a. The upstream operator has completed.
    //   b. The operator has downstream demand and can produce output
    //      independently from receiving input.
    //   c. The operator has input it can consume.
    //   d. The operator is a command, i.e., has both a source and a sink.
    const auto has_demand
      = demand.has_value() or std::is_same_v<Output, std::monostate>;
    const auto should_continue
      = instance->it != instance->gen.end()                         // (1)
        and not waiting                                             // (2)
        and (not previous                                           // (3a)
             or (has_demand and op->input_independent())            // (3b)
             or not inbound_buffer.empty()                          // (3c)
             or detail::are_same_v<std::monostate, Input, Output>); // (3d)
    if (should_continue) {
      schedule_run(false);
    } else if (not waiting and has_demand) {
      // If we shouldn't continue, but there is an upstream demand, then we may
      // be in a situation where the operator has internally buffered events and
      // needs to be polled until some operator-internal timeout expires before
      // it yields the results. We use exponential backoff for this with 25%
      // increments.
      schedule_run(true);
    } else {
      TENZIR_TRACE("{} {} idles", *self, op->name());
    }
    metrics.num_runs += 1;
    metrics.num_runs_processing += consumed_input or produced_output ? 1 : 0;
    metrics.num_runs_processing_input += consumed_input ? 1 : 0;
    metrics.num_runs_processing_output += produced_output ? 1 : 0;
    consumed_input = false;
    produced_output = false;
  }

  auto pull(exec_node_sink_actor sink, uint64_t batch_size) -> caf::result<void>
    requires(not std::is_same_v<Output, std::monostate>)
  {
    TENZIR_TRACE("{} {} received downstream demand", *self, op->name());
    if (demand) {
      demand->rp.deliver();
    }
    if (instance->it == instance->gen.end()) {
      return {};
    }
    schedule_run(false);
    auto& pr = demand.emplace(self->make_response_promise<void>(),
                              std::move(sink), batch_size);
    return pr.rp;
  }

  auto push(Input input) -> caf::result<void>
    requires(not std::is_same_v<Input, std::monostate>)
  {
    const auto input_size = size(input);
    TENZIR_TRACE("{} {} received {} elements from upstream", *self, op->name(),
                 input_size);
    metrics.inbound_measurement.num_elements += input_size;
    metrics.inbound_measurement.num_batches += 1;
    metrics.inbound_measurement.num_approx_bytes += approx_bytes(input);
    inbound_buffer_size += input_size;
    inbound_buffer.push_back(std::move(input));
    schedule_run(false);
    return {};
  }

  void on_error(caf::error error) {
    if (start_rp.pending()) {
      start_rp.deliver(std::move(error));
      self->quit(ec::silent);
      return;
    }
    self->quit(std::move(error));
  }
};

template <class Input, class Output>
auto exec_node(
  exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self,
  operator_ptr op, node_actor node,
  receiver_actor<diagnostic> diagnostic_handler,
  receiver_actor<metric> metrics_handler, int index, bool has_terminal)
  -> exec_node_actor::behavior_type {
  if (self->getf(caf::scheduled_actor::is_detached_flag)) {
    const auto name = fmt::format("tenzir.exec-node.{}", op->name());
    caf::detail::set_thread_name(name.c_str());
  }
  self->state.self = self;
  self->state.op = std::move(op);
  auto time_starting_guard = make_timer_guard(
    self->state.metrics.time_scheduled, self->state.metrics.time_starting);
  self->state.metrics_handler = std::move(metrics_handler);
  self->state.metrics.operator_index = index;
  self->state.metrics.operator_name = self->state.op->name();
  self->state.metrics.inbound_measurement.unit = operator_type_name<Input>();
  self->state.metrics.outbound_measurement.unit = operator_type_name<Output>();
  // We make an exception here for transformations, which are always considered
  // internal as they cannot transport data outside of the pipeline.
  self->state.metrics.internal
    = self->state.op->internal()
      and (std::is_same_v<Input, std::monostate>
           or std::is_same_v<Output, std::monostate>);
  self->state.ctrl = std::make_unique<exec_node_control_plane<Input, Output>>(
    self, std::move(diagnostic_handler), has_terminal);
  // The node actor must be set when the operator is not a source.
  if (self->state.op->location() == operator_location::remote and not node) {
    self->state.on_error(caf::make_error(
      ec::logic_error,
      fmt::format("{} runs a remote operator and must have a node", *self)));
    return exec_node_actor::behavior_type::make_empty_behavior();
  }
  self->state.weak_node = node;
  self->set_exception_handler(
    [self](std::exception_ptr exception) -> caf::error {
      auto error = std::invoke([&] {
        try {
          std::rethrow_exception(exception);
        } catch (diagnostic diag) {
          return std::move(diag).to_error();
        } catch (const std::exception& err) {
          return diagnostic::error("{}", err.what())
            .note("unhandled exception in {} {}", *self, self->state.op->name())
            .to_error();
        } catch (...) {
          return diagnostic::error("unhandled exception in {} {}", *self,
                                   self->state.op->name())
            .to_error();
        }
      });
      if (self->state.start_rp.pending()) {
        self->state.start_rp.deliver(std::move(error));
        return ec::silent;
      }
      return error;
    });
  return {
    [self](atom::internal, atom::run) -> caf::result<void> {
      auto time_scheduled_guard
        = make_timer_guard(self->state.metrics.time_scheduled);
      return self->state.internal_run();
    },
    [self](atom::start,
           std::vector<caf::actor>& all_previous) -> caf::result<void> {
      auto time_scheduled_guard = make_timer_guard(
        self->state.metrics.time_scheduled, self->state.metrics.time_starting);
      return self->state.start(std::move(all_previous));
    },
    [self](atom::pause) -> caf::result<void> {
      auto time_scheduled_guard
        = make_timer_guard(self->state.metrics.time_scheduled);
      return self->state.pause();
    },
    [self](atom::resume) -> caf::result<void> {
      auto time_scheduled_guard
        = make_timer_guard(self->state.metrics.time_scheduled);
      return self->state.resume();
    },
    [self](diagnostic& diag) -> caf::result<void> {
      auto time_scheduled_guard
        = make_timer_guard(self->state.metrics.time_scheduled);
      self->state.ctrl->diagnostics().emit(std::move(diag));
      return {};
    },
    [self](atom::push, table_slice& events) -> caf::result<void> {
      auto time_scheduled_guard
        = make_timer_guard(self->state.metrics.time_scheduled);
      if constexpr (std::is_same_v<Input, table_slice>) {
        return self->state.push(std::move(events));
      } else {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} does not accept events as input",
                                           *self));
      }
    },
    [self](atom::push, chunk_ptr& bytes) -> caf::result<void> {
      auto time_scheduled_guard
        = make_timer_guard(self->state.metrics.time_scheduled);
      if constexpr (std::is_same_v<Input, chunk_ptr>) {
        return self->state.push(std::move(bytes));
      } else {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} does not accept bytes as input",
                                           *self));
      }
    },
    [self](atom::pull, exec_node_sink_actor& sink,
           uint64_t batch_size) -> caf::result<void> {
      auto time_scheduled_guard
        = make_timer_guard(self->state.metrics.time_scheduled);
      if constexpr (not std::is_same_v<Output, std::monostate>) {
        return self->state.pull(std::move(sink), batch_size);
      } else {
        return caf::make_error(
          ec::logic_error,
          fmt::format("{} is a sink and must not be pulled from", *self));
      }
    },
  };
}

} // namespace

auto spawn_exec_node(caf::scheduled_actor* self, operator_ptr op,
                     operator_type input_type, node_actor node,
                     receiver_actor<diagnostic> diagnostic_handler,
                     receiver_actor<metric> metrics_handler, int index,
                     bool has_terminal)
  -> caf::expected<std::pair<exec_node_actor, operator_type>> {
  TENZIR_ASSERT(self);
  TENZIR_ASSERT(op != nullptr);
  TENZIR_ASSERT(node != nullptr
                or not(op->location() == operator_location::remote));
  TENZIR_ASSERT(diagnostic_handler != nullptr);
  auto output_type = op->infer_type(input_type);
  if (not output_type) {
    return caf::make_error(ec::logic_error,
                           fmt::format("failed to spawn exec-node for '{}': {}",
                                       op->name(), output_type.error()));
  }
  auto f = [&]<caf::spawn_options SpawnOptions>() {
    return [&]<class Input, class Output>(tag<Input>,
                                          tag<Output>) -> exec_node_actor {
      using input_type
        = std::conditional_t<std::is_void_v<Input>, std::monostate, Input>;
      using output_type
        = std::conditional_t<std::is_void_v<Output>, std::monostate, Output>;
      auto result = self->spawn<SpawnOptions>(
        exec_node<input_type, output_type>, std::move(op), std::move(node),
        std::move(diagnostic_handler), std::move(metrics_handler), index,
        has_terminal);
      return result;
    };
  };
  return std::pair{
    op->detached() ? std::visit(f.template operator()<caf::detached>(),
                                input_type, *output_type)
                   : std::visit(f.template operator()<caf::no_spawn_options>(),
                                input_type, *output_type),
    *output_type,
  };
};

} // namespace tenzir
