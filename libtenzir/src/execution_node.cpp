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

#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

namespace tenzir {

namespace {

using namespace std::chrono_literals;
using namespace si_literals;

template <class Element = void>
struct defaults {
  /// Defines the upper bound for the batch timeout used when requesting a batch
  /// from the the previous execution node in the pipeline.
  inline static constexpr duration max_batch_timeout = 250ms;

  /// Defines the upper bound for how often an operator's generator may be
  /// advanced within one run before yielding to the scheduler.
  // TODO: Setting this to a higher value than 1 breaks request/await for
  // operators.
  inline static constexpr int max_advances_per_run = 1;

  /// Defines the time interval for sending metrics of the currently running
  /// pipeline operator.
  inline static constexpr auto metrics_interval
    = std::chrono::milliseconds{1000};
};

template <>
struct defaults<table_slice> : defaults<> {
  /// Defines the upper bound for the batch size used when requesting a batch
  /// from the the previous execution node in the pipeline.
  inline static constexpr uint64_t max_batch_size = 64_Ki;

  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_batch_size = 8_Ki;

  /// Defines the upper bound for the inbound and outbound buffer of the
  /// execution node.
  inline static constexpr uint64_t max_buffered = 254_Ki;
};

template <>
struct defaults<chunk_ptr> : defaults<> {
  /// Defines the upper bound for the batch size used when requesting a batch
  /// from the the previous execution node in the pipeline.
  inline static constexpr uint64_t max_batch_size = 1_Mi;

  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_batch_size = 128_Ki;

  /// Defines the upper bound for the inbound and outbound buffer of the
  /// execution node.
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

template <class Input, class Output>
struct exec_node_state;

template <class Input, class Output>
class exec_node_diagnostic_handler final : public diagnostic_handler {
public:
  exec_node_diagnostic_handler(
    exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self,
    receiver_actor<diagnostic> diagnostic_handler)
    : self_{self}, diagnostic_handler_{std::move(diagnostic_handler)} {
  }

  void emit(diagnostic d) override {
    TENZIR_VERBOSE("emitting diagnostic: {}", d);
    self_->request(diagnostic_handler_, caf::infinite, std::move(d))
      .then([]() {},
            [](caf::error& e) {
              TENZIR_WARN("failed to send diagnostic: {}", e);
            });
    if (d.severity == severity::error and not has_seen_error_) {
      has_seen_error_ = true;
      self_->state.ctrl->abort(ec::silent);
    }
  }

  auto has_seen_error() const -> bool override {
    return has_seen_error_;
  }

private:
  exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self_ = {};
  receiver_actor<diagnostic> diagnostic_handler_ = {};
  bool has_seen_error_ = {};
};

template <class Input, class Output>
class exec_node_control_plane final : public operator_control_plane {
public:
  exec_node_control_plane(
    exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self,
    receiver_actor<diagnostic> diagnostic_handler, bool has_terminal)
    : state_{self->state},
      diagnostic_handler_{
        std::make_unique<exec_node_diagnostic_handler<Input, Output>>(
          self, std::move(diagnostic_handler))},
      has_terminal_{has_terminal} {
  }

  auto self() noexcept -> exec_node_actor::base& override {
    return *state_.self;
  }

  auto node() noexcept -> node_actor override {
    return state_.weak_node.lock();
  }

  auto abort(caf::error error) noexcept -> void override {
    TENZIR_ASSERT(error != caf::none);
    if (error != ec::silent) {
      diagnostic::error("{}", error)
        .note("from `{}`", state_.op->to_string())
        .emit(diagnostics());
    }
    if (not state_.abort) {
      TENZIR_VERBOSE("setting abort flag of `{}`", state_.op->name());
      state_.abort = caf::make_error(ec::silent, fmt::to_string(error));
    } else {
      TENZIR_VERBOSE("abort flag of `{}` was already set", state_.op->name());
    }
  }

  auto warn(caf::error error) noexcept -> void override {
    if (error != ec::silent) {
      diagnostic::warning("{}", error)
        .note("from `{}`", state_.op->to_string())
        .emit(diagnostics());
    }
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    return tenzir::modules::schemas();
  }

  auto concepts() const noexcept -> const concepts_map& override {
    return tenzir::modules::concepts();
  }

  auto diagnostics() noexcept -> diagnostic_handler& override {
    return *diagnostic_handler_;
  }

  auto allow_unsafe_pipelines() const noexcept -> bool override {
    return caf::get_or(content(state_.self->config()),
                       "tenzir.allow-unsafe-pipelines", false);
  }

  auto has_terminal() const noexcept -> bool override {
    return has_terminal_;
  }

private:
  exec_node_state<Input, Output>& state_;
  std::unique_ptr<exec_node_diagnostic_handler<Input, Output>> diagnostic_handler_
    = {};
  bool has_terminal_;
};

auto size(const table_slice& slice) -> uint64_t {
  return slice.rows();
}

auto size(const chunk_ptr& chunk) -> uint64_t {
  return chunk ? chunk->size() : 0;
}

auto split(const chunk_ptr& chunk, size_t partition_point)
  -> std::pair<chunk_ptr, chunk_ptr> {
  if (partition_point == 0)
    return {{}, chunk};
  if (partition_point >= size(chunk))
    return {chunk, {}};
  return {
    chunk->slice(0, partition_point),
    chunk->slice(partition_point, size(chunk) - partition_point),
  };
}

auto split(std::vector<chunk_ptr> chunks, uint64_t partition_point)
  -> std::pair<std::vector<chunk_ptr>, std::vector<chunk_ptr>> {
  auto it = chunks.begin();
  for (; it != chunks.end(); ++it) {
    if (partition_point == size(*it)) {
      return {
        {chunks.begin(), it + 1},
        {it + 1, chunks.end()},
      };
    }
    if (partition_point < size(*it)) {
      auto lhs = std::vector<chunk_ptr>{};
      auto rhs = std::vector<chunk_ptr>{};
      lhs.reserve(std::distance(chunks.begin(), it + 1));
      rhs.reserve(std::distance(it, chunks.end()));
      lhs.insert(lhs.end(), std::make_move_iterator(chunks.begin()),
                 std::make_move_iterator(it));
      auto [split_lhs, split_rhs] = split(*it, partition_point);
      lhs.push_back(std::move(split_lhs));
      rhs.push_back(std::move(split_rhs));
      rhs.insert(rhs.end(), std::make_move_iterator(it + 1),
                 std::make_move_iterator(chunks.end()));
      return {
        std::move(lhs),
        std::move(rhs),
      };
    }
    partition_point -= size(*it);
  }
  return {
    std::move(chunks),
    {},
  };
}

template <class Input>
struct inbound_state_mixin {
  /// A handle to the previous execution node.
  exec_node_actor previous = {};
  bool signaled_demand = {};

  std::vector<Input> inbound_buffer = {};
  uint64_t inbound_buffer_size = {};
};

template <>
struct inbound_state_mixin<std::monostate> {};

template <class Output>
struct outbound_state_mixin {
  /// The outbound buffer of the operator contains elements ready to be
  /// transported to the next operator's execution node.
  std::vector<Output> outbound_buffer = {};
  uint64_t outbound_buffer_size = {};

  /// The currently open demand.
  struct demand {
    caf::typed_response_promise<void> rp = {};
    exec_node_sink_actor sink = {};
    const uint64_t batch_size = {};
    const std::chrono::steady_clock::time_point batch_timeout = {};
    bool ongoing = {};
  };
  std::optional<demand> current_demand = {};
  bool reject_demand = {};
};

template <>
struct outbound_state_mixin<std::monostate> {};

template <class Input, class Output>
struct exec_node_state : inbound_state_mixin<Input>,
                         outbound_state_mixin<Output> {
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

  // Metrics that track the total number of inbound and outbound elements that
  // passed through this operator.
  std::chrono::steady_clock::time_point start_time
    = std::chrono::steady_clock::now();
  pipeline_op_metrics current_metrics = {};

  receiver_actor<pipeline_op_metrics> metrics_handler = {};

  // Indicates whether the operator has stalled, i.e., the generator should not
  // be advanced.
  bool stalled = {};

  /// A pointer to te operator control plane passed to this operator during
  /// execution, which acts as an escape hatch to this actor.
  std::unique_ptr<exec_node_control_plane<Input, Output>> ctrl = {};

  /// A weak handle to the node actor.
  detail::weak_handle<node_actor> weak_node = {};

  /// The next run of this actor's internal run loop.
  bool run_scheduled = {};

  /// Set by `ctrl.abort(...)`, to be checked by `start()` and `run()`.
  caf::error abort;

  auto start(std::vector<caf::actor> previous) -> caf::result<void> {
    auto time_starting_guard = make_timer_guard(current_metrics.time_scheduled,
                                                current_metrics.time_starting);
    TENZIR_DEBUG("{} received start request for `{}`", *self, op->to_string());
    detail::weak_run_delayed_loop(self, defaults<>::metrics_interval, [this] {
      emit_metrics();
    });
    if (instance.has_value()) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} was already started", *self));
    }
    if constexpr (std::is_same_v<Input, std::monostate>) {
      if (not previous.empty()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} runs a source operator and must "
                                           "not have a previous exec-node",
                                           *self));
      }
    } else {
      // The previous exec-node must be set when the operator is not a source.
      if (previous.empty()) {
        return caf::make_error(
          ec::logic_error, fmt::format("{} runs a transformation/sink operator "
                                       "and must have a previous exec-node",
                                       *self));
      }
      this->previous
        = caf::actor_cast<exec_node_actor>(std::move(previous.back()));
      previous.pop_back();
      self->monitor(this->previous);
      self->set_down_handler([this](const caf::down_msg& msg) {
        auto time_scheduled_guard
          = make_timer_guard(current_metrics.time_scheduled);
        if (msg.source != this->previous.address()) {
          TENZIR_DEBUG("ignores down msg `{}` from unknown source: {}",
                       msg.reason, msg.source);
          return;
        }
        TENZIR_DEBUG("{} got down from previous execution node: {}", op->name(),
                     msg.reason);
        this->previous = nullptr;
        // We empirically noticed that sometimes, we get a down message from a
        // previous execution node in a different actor system, but do not get
        // an error response to our demand request. To be able to shutdown
        // correctly, we must set `signaled_demand` to false as a workaround.
        this->signaled_demand = false;
        schedule_run();
        if (msg.reason) {
          auto category
            = msg.reason == ec::silent ? ec::silent : ec::unspecified;
          ctrl->abort(caf::make_error(
            category, fmt::format("{} shuts down because of irregular "
                                  "exit of previous operator: {}",
                                  op, msg.reason)));
        }
      });
    }
    // Instantiate the operator with its input type.
    {
      auto time_scheduled_guard
        = make_timer_guard(current_metrics.time_running);
      auto output_generator = op->instantiate(make_input_adapter(), *ctrl);
      if (not output_generator) {
        TENZIR_VERBOSE("{} could not instantiate operator: {}", *self,
                       output_generator.error());
        return add_context(output_generator.error(),
                           "{} failed to instantiate operator", *self);
      }
      if (not std::holds_alternative<generator<Output>>(*output_generator)) {
        return caf::make_error(
          ec::logic_error, fmt::format("{} expected {}, but got {}", *self,
                                       operator_type_name<Output>(),
                                       operator_type_name(*output_generator)));
      }
      instance.emplace();
      instance->gen = std::get<generator<Output>>(std::move(*output_generator));
      TENZIR_TRACE("{} calls begin on instantiated operator", *self);
      instance->it = instance->gen.begin();
      if (abort) {
        TENZIR_DEBUG("{} was aborted during begin: {}", *self, op->to_string(),
                     abort);
        return abort;
      }
    }
    if constexpr (std::is_same_v<Output, std::monostate>) {
      TENZIR_TRACE("{} is the sink and requests start from {}", *self,
                   this->previous);
      auto rp = self->make_response_promise<void>();
      self
        ->request(this->previous, caf::infinite, atom::start_v,
                  std::move(previous))
        .then(
          [this, rp]() mutable {
            auto time_starting_guard = make_timer_guard(
              current_metrics.time_scheduled, current_metrics.time_starting);
            TENZIR_DEBUG("{} schedules run of sink after successful startup",
                         *self);
            schedule_run();
            rp.deliver();
          },
          [this, rp](caf::error& error) mutable {
            auto time_starting_guard = make_timer_guard(
              current_metrics.time_scheduled, current_metrics.time_starting);
            TENZIR_DEBUG("{} forwards error during startup: {}", *self, error);
            rp.deliver(std::move(error));
          });
      return rp;
    }
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      TENZIR_DEBUG("{} delegates start to {}", *self, this->previous);
      return self->delegate(this->previous, atom::start_v, std::move(previous));
    }
    return {};
  }

  auto request_more_input() -> void
    requires(not std::is_same_v<Input, std::monostate>)
  {
    // There are a few reasons why we would not be able to request more input:
    // 1. The space in our inbound buffer is below the minimum batch size.
    // 2. The previous execution node is down.
    // 3. We already have an open request for more input.
    TENZIR_ASSERT(this->inbound_buffer_size <= defaults<Input>::max_buffered);
    const auto batch_size
      = std::min(defaults<Input>::max_buffered - this->inbound_buffer_size,
                 defaults<Input>::max_batch_size);
    if (not this->previous or this->signaled_demand
        or batch_size < defaults<Input>::min_batch_size) {
      return;
    }
    /// Issue the actual request. If the inbound buffer is empty, we await the
    /// response, causing this actor to be suspended until the events have
    /// arrived.
    auto handle_result = [this]() mutable {
      TENZIR_TRACE("pull from {} was successful", op->name());
      auto time_scheduled_guard
        = make_timer_guard(current_metrics.time_scheduled);
      this->signaled_demand = false;
      schedule_run();
    };
    auto handle_error = [this](caf::error& error) {
      TENZIR_TRACE("pull from {} failed: {}", op->name(), error);
      auto time_scheduled_guard
        = make_timer_guard(current_metrics.time_scheduled);
      this->signaled_demand = false;
      schedule_run();
      // TODO: We currently have to use `caf::exit_reason::kill` in
      // `pipeline_executor.cpp` to work around a CAF bug. However, this implies
      // that we might receive a `caf::sec::broken_promise` error here.
      if (error == caf::sec::request_receiver_down
          || error == caf::sec::broken_promise) {
        this->previous = nullptr;
        return;
      }
      // We failed to get results from the previous; let's emit a diagnostic
      // instead.
      if (this->previous) {
        diagnostic::warning("{}", error)
          .note("`{}` failed to pull from previous execution node",
                op->to_string())
          .emit(ctrl->diagnostics());
      }
    };
    this->signaled_demand = true;
    TENZIR_TRACE("sending pull from {}", op->name());
    auto response_handle
      = self->request(this->previous, caf::infinite, atom::pull_v,
                      static_cast<exec_node_sink_actor>(self), batch_size,
                      defaults<Input>::max_batch_timeout);
    std::move(response_handle)
      .then(std::move(handle_result), std::move(handle_error));
  }

  auto advance_generator() -> bool {
    auto time_running_guard = make_timer_guard(current_metrics.time_running);
    TENZIR_ASSERT(instance);
    TENZIR_ASSERT(instance->it != instance->gen.end());
    bool empty = false;
    if constexpr (not std::is_same_v<Output, std::monostate>) {
      if (this->outbound_buffer_size >= defaults<Output>::max_buffered) {
        return false;
      }
      auto next = std::move(*instance->it);
      ++instance->it;
      if (size(next) > 0) {
        empty = true;
        this->outbound_buffer_size += size(next);
        this->outbound_buffer.push_back(std::move(next));
      }
    } else {
      ++instance->it;
    }
    if (abort) {
      self->quit(abort);
      return false;
    }
    return not empty and instance->it != instance->gen.end();
  }

  auto make_input_adapter() -> std::monostate
    requires std::is_same_v<Input, std::monostate>
  {
    return {};
  }

  auto make_input_adapter() -> generator<Input>
    requires(not std::is_same_v<Input, std::monostate>)
  {
    auto stall_guard = caf::detail::make_scope_guard([this] {
      stalled = false;
    });
    while (this->previous or this->inbound_buffer_size > 0
           or this->signaled_demand) {
      if (this->inbound_buffer_size == 0) {
        TENZIR_ASSERT(this->inbound_buffer.empty());
        stalled = true;
        co_yield {};
        continue;
      }
      while (not this->inbound_buffer.empty()) {
        auto next = std::move(this->inbound_buffer.front());
        TENZIR_ASSERT(size(next) != 0);
        this->inbound_buffer_size -= size(next);
        this->inbound_buffer.erase(this->inbound_buffer.begin());
        stalled = false;
        co_yield std::move(next);
      }
    }
  }

  auto schedule_run() -> void {
    if (not instance or run_scheduled) {
      return;
    }
    run_scheduled = true;
    // We *always* use the delayed variant here instead of scheduling
    // immediately as that has two distinct advantages:
    // - It allows for using a weak actor pointer on the click, i.e., it does
    //   not prohibit shutdown.
    // - It does not get run immediately, which would conflict with operators
    //   using `ctrl.self().request(...).await(...)`.
    auto action = [this] {
      auto time_scheduled_guard
        = make_timer_guard(current_metrics.time_scheduled);
      TENZIR_ASSERT(run_scheduled);
      run_scheduled = false;
      run();
    };
    self->clock().schedule(self->clock().now(),
                           caf::make_action(std::move(action),
                                            caf::action::state::waiting),
                           caf::weak_actor_ptr{self->ctrl()});
  }

  auto deliver_batches(std::chrono::steady_clock::time_point now, bool force)
    -> void
    requires(not std::is_same_v<Output, std::monostate>)
  {
    if (not this->current_demand or this->current_demand->ongoing) {
      return;
    }
    TENZIR_ASSERT(instance);
    if (not force
        and ((instance->it == instance->gen.end()
              or this->outbound_buffer_size < this->current_demand->batch_size)
             and (this->current_demand->batch_timeout > now))) {
      return;
    }
    this->current_demand->ongoing = true;
    const auto capped_demand
      = std::min(this->outbound_buffer_size, this->current_demand->batch_size);
    if (capped_demand == 0) {
      TENZIR_DEBUG("{} short-circuits delivery of zero batches", op->name());
      this->current_demand->rp.deliver();
      this->current_demand.reset();
      schedule_run();
      return;
    }
    auto [lhs, _] = split(this->outbound_buffer, capped_demand);
    auto handle_result = [this, capped_demand]() {
      auto time_scheduled_guard
        = make_timer_guard(current_metrics.time_scheduled);
      TENZIR_TRACE("{} pushed successfully", op->name());
      current_metrics.outbound_total += capped_demand;
      auto [lhs, rhs] = split(this->outbound_buffer, capped_demand);
      current_metrics.outbound_num_batches += lhs.size();
      this->outbound_buffer = std::move(rhs);
      this->outbound_buffer_size
        = std::transform_reduce(this->outbound_buffer.begin(),
                                this->outbound_buffer.end(), uint64_t{},
                                std::plus{}, [](const Output& x) {
                                  return size(x);
                                });
      this->current_demand->rp.deliver();
      this->current_demand.reset();
      schedule_run();
    };
    auto handle_error = [this](caf::error& error) {
      auto time_scheduled_guard
        = make_timer_guard(current_metrics.time_scheduled);
      TENZIR_DEBUG("{} failed to push", op->name());
      this->current_demand->rp.deliver(std::move(error));
      this->current_demand.reset();
      schedule_run();
    };
    auto response_handle = self->request(
      this->current_demand->sink, caf::infinite, atom::push_v, std::move(lhs));
    if (force or this->outbound_buffer_size >= defaults<Output>::max_buffered) {
      TENZIR_TRACE("{} pushes {}/{} buffered elements and suspends execution",
                   op->name(), capped_demand, this->outbound_buffer_size);
      std::move(response_handle)
        .await(std::move(handle_result), std::move(handle_error));
    } else {
      TENZIR_TRACE("{} pushes {}/{} buffered elements", op->name(),
                   capped_demand, this->outbound_buffer_size);
      std::move(response_handle)
        .then(std::move(handle_result), std::move(handle_error));
    }
  };

  auto emit_metrics() -> void {
    current_metrics.time_elapsed = std::chrono::duration_cast<duration>(
      std::chrono::steady_clock::now() - start_time);
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      const auto total = static_cast<double>(current_metrics.inbound_total);
      current_metrics.inbound_rate_per_second
        = total
          / std::chrono::duration_cast<
              std::chrono::duration<double, std::chrono::seconds::period>>(
              current_metrics.time_elapsed)
              .count();
    }
    if constexpr (not std::is_same_v<Output, std::monostate>) {
      const auto total = static_cast<double>(current_metrics.outbound_total);
      current_metrics.outbound_rate_per_second
        = total
          / std::chrono::duration_cast<
              std::chrono::duration<double, std::chrono::seconds::period>>(
              current_metrics.time_elapsed)
              .count();
    }
    self->request(metrics_handler, caf::infinite, current_metrics)
      .then([]() {},
            [](caf::error& e) {
              TENZIR_WARN("failed to send metrics: {}", e);
            });
  }

  auto print_metrics() -> void {
    auto percentage = [](auto num, auto den) {
      return std::chrono::duration<double, std::chrono::seconds::period>(num)
               .count()
             / std::chrono::duration<double, std::chrono::seconds::period>(den)
                 .count()
             * 100.0;
    };
    TENZIR_VERBOSE("{} was scheduled for {:.2g}% of total runtime", op->name(),
                   percentage(current_metrics.time_scheduled,
                              current_metrics.time_elapsed));
    TENZIR_VERBOSE("{} spent {:.2g}% of scheduled time starting", op->name(),
                   percentage(current_metrics.time_starting,
                              current_metrics.time_scheduled));
    TENZIR_VERBOSE("{} spent {:.2g}% of scheduled time running", op->name(),
                   percentage(current_metrics.time_running,
                              current_metrics.time_scheduled));
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      constexpr auto inbound_unit
        = std::is_same_v<Input, chunk_ptr> ? "B" : "events";
      TENZIR_VERBOSE("{} inbound {} {} in {} rate = {:.2f} {}/s avg batch "
                     "size = {:.2f} "
                     "{}",
                     op->name(), current_metrics.inbound_total, inbound_unit,
                     data{current_metrics.time_elapsed},
                     current_metrics.inbound_rate_per_second, inbound_unit,
                     static_cast<double>(current_metrics.inbound_total)
                       / current_metrics.inbound_num_batches,
                     inbound_unit);
    }
    if constexpr (not std::is_same_v<Output, std::monostate>) {
      constexpr auto outbound_unit
        = std::is_same_v<Output, chunk_ptr> ? "B" : "events";
      TENZIR_VERBOSE("{} outbound {} {} in {} rate = {:.2f} {}/s avg batch "
                     "size = "
                     "{:.2f} {}",
                     op->name(), current_metrics.outbound_total, outbound_unit,
                     data{current_metrics.time_elapsed},
                     current_metrics.outbound_rate_per_second, outbound_unit,
                     static_cast<double>(current_metrics.outbound_total)
                       / current_metrics.outbound_num_batches,
                     outbound_unit);
    }
  }

  auto run() -> void {
    TENZIR_TRACE("{} enters run loop", op->name());
    TENZIR_ASSERT(instance);
    const auto now = std::chrono::steady_clock::now();
    // Check if we're done.
    if (instance->it == instance->gen.end()) {
      TENZIR_DEBUG("{} is at the end of its generator", op->name());
      // Shut down the previous execution node immediately if we're done.
      // We send an unreachable error here slightly before this execution
      // node shuts down. This is merely an optimization; we call self->quit
      // a tiny bit later anyways, which would send the same exit reason
      // upstream implicitly. However, doing this early is nice because we
      // can prevent the upstream operators from running unnecessarily.
      if constexpr (not std::is_same_v<Input, std::monostate>) {
        if (this->previous) {
          TENZIR_DEBUG("{} shuts down previous operator", op->name());
          self->send_exit(this->previous, caf::exit_reason::normal);
        }
      }
      // When we're done, we must make sure that we have delivered all results
      // to the next operator. This has the following pre-requisites:
      // - The generator must be completed (already checked here).
      // - There must not be any outstanding demand.
      // - There must not be anything remaining in the buffer.
      if constexpr (not std::is_same_v<Output, std::monostate>) {
        if (this->current_demand and this->outbound_buffer_size == 0) {
          TENZIR_DEBUG("{} rejects further demand from next operator",
                       op->name());
          this->reject_demand = true;
        }
        if (this->current_demand or this->outbound_buffer_size > 0) {
          TENZIR_DEBUG("{} forcibly delivers batches", op->name());
          deliver_batches(now, true);
          schedule_run();
          return;
        }
        TENZIR_ASSERT(not this->current_demand);
        TENZIR_ASSERT(this->outbound_buffer_size == 0);
      }
      TENZIR_VERBOSE("{} is done", op);
      emit_metrics();
      print_metrics();
      self->quit();
      return;
    }
    // Try to deliver.
    if constexpr (not std::is_same_v<Output, std::monostate>) {
      deliver_batches(now, false);
    }
    // Request more input if there's more to be retrieved.
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      request_more_input();
    }
    // Produce more output if there's more to be produced, then schedule the
    // next run. For sinks, this happens delayed when there is no input. For
    // everything else, it needs to happen only when there's enough space in the
    // outbound buffer.
    for (auto i = 0; i < defaults<Output>::max_advances_per_run; ++i) {
      if (not advance_generator()) {
        break;
      }
    }
    if constexpr (std::is_same_v<Output, std::monostate>) {
      if (not stalled) {
        schedule_run();
      } else {
        TENZIR_ASSERT(this->signaled_demand);
      }
    } else if constexpr (std::is_same_v<Input, std::monostate>) {
      if (not stalled
          and (this->current_demand
               or (this->outbound_buffer_size < defaults<Output>::max_buffered
                   and instance->it != instance->gen.end()))) {
        schedule_run();
      }
    } else {
      auto can_generate
        = this->outbound_buffer_size < defaults<Output>::max_buffered
          and instance->it != instance->gen.end();
      auto should_produce = this->current_demand.has_value();
      auto is_previous_dead = not this->previous;
      if (is_previous_dead
          or (not stalled and (should_produce or can_generate))) {
        schedule_run();
      }
    }
  }

  auto
  pull(exec_node_sink_actor sink, uint64_t batch_size, duration batch_timeout)
    -> caf::result<void>
    requires(not std::is_same_v<Output, std::monostate>)
  {
    auto time_scheduled_guard
      = make_timer_guard(current_metrics.time_scheduled);
    if (this->reject_demand) {
      auto rp = self->make_response_promise<void>();
      detail::weak_run_delayed(self, batch_timeout, [rp]() mutable {
        rp.deliver();
      });
      return {};
    }
    schedule_run();
    if (this->current_demand) {
      return caf::make_error(ec::logic_error, "concurrent pull");
    }
    auto& pr = this->current_demand.emplace(
      self->make_response_promise<void>(), std::move(sink), batch_size,
      std::chrono::steady_clock::now() + batch_timeout);
    return pr.rp;
  }

  auto push(std::vector<Input> input) -> caf::result<void>
    requires(not std::is_same_v<Input, std::monostate>)
  {
    auto time_scheduled_guard
      = make_timer_guard(current_metrics.time_scheduled);
    schedule_run();
    const auto input_size = std::transform_reduce(
      input.begin(), input.end(), uint64_t{}, std::plus{}, [](const Input& x) {
        return size(x);
      });
    current_metrics.inbound_num_batches += input.size();
    if (input_size == 0) {
      return caf::make_error(ec::logic_error, "received empty batch");
    }
    if (this->inbound_buffer_size + input_size
        > defaults<Input>::max_buffered) {
      return caf::make_error(ec::logic_error, "inbound buffer full");
    }
    this->inbound_buffer.insert(this->inbound_buffer.end(),
                                std::make_move_iterator(input.begin()),
                                std::make_move_iterator(input.end()));
    this->inbound_buffer_size += input_size;
    current_metrics.inbound_total += input_size;
    return {};
  }
};

template <class Input, class Output>
auto exec_node(
  exec_node_actor::stateful_pointer<exec_node_state<Input, Output>> self,
  operator_ptr op, node_actor node,
  receiver_actor<diagnostic> diagnostic_handler,
  receiver_actor<pipeline_op_metrics> metrics_handler, int index,
  bool has_terminal) -> exec_node_actor::behavior_type {
  self->state.self = self;
  self->state.op = std::move(op);
  self->state.metrics_handler = std::move(metrics_handler);
  self->state.current_metrics.index = index;
  self->state.current_metrics.in_unit = operator_type_name<Input>();
  self->state.current_metrics.out_unit = operator_type_name<Output>();
  self->state.ctrl = std::make_unique<exec_node_control_plane<Input, Output>>(
    self, std::move(diagnostic_handler), has_terminal);
  // The node actor must be set when the operator is not a source.
  if (self->state.op->location() == operator_location::remote and not node) {
    self->quit(caf::make_error(
      ec::logic_error,
      fmt::format("{} runs a remote operator and must have a node", *self)));
    return exec_node_actor::behavior_type::make_empty_behavior();
  }
  self->state.weak_node = node;
  self->attach_functor([name = self->state.op->name()] {
    TENZIR_DEBUG("exec-node for {} shut down", name);
  });
  return {
    [self](atom::start,
           std::vector<caf::actor>& previous) -> caf::result<void> {
      return self->state.start(std::move(previous));
    },
    [self](atom::push, std::vector<table_slice>& events) -> caf::result<void> {
      if constexpr (std::is_same_v<Input, table_slice>) {
        return self->state.push(std::move(events));
      } else {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} does not accept events as input",
                                           *self));
      }
    },
    [self](atom::push, std::vector<chunk_ptr>& bytes) -> caf::result<void> {
      if constexpr (std::is_same_v<Input, chunk_ptr>) {
        return self->state.push(std::move(bytes));
      } else {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} does not accept bytes as input",
                                           *self));
      }
    },
    [self](atom::pull, exec_node_sink_actor& sink, uint64_t batch_size,
           duration batch_timeout) -> caf::result<void> {
      if constexpr (not std::is_same_v<Output, std::monostate>) {
        return self->state.pull(std::move(sink), batch_size, batch_timeout);
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
                     receiver_actor<diagnostic> diagnostics_handler,
                     receiver_actor<pipeline_op_metrics> metrics_handler,
                     int index, bool has_terminal)
  -> caf::expected<std::pair<exec_node_actor, operator_type>> {
  TENZIR_ASSERT(self);
  TENZIR_ASSERT(op != nullptr);
  TENZIR_ASSERT(node != nullptr
                or not(op->location() == operator_location::remote));
  TENZIR_ASSERT(diagnostics_handler != nullptr);
  auto output_type = op->infer_type(input_type);
  if (not output_type) {
    return caf::make_error(ec::logic_error,
                           fmt::format("failed to spawn exec-node for '{}': {}",
                                       op->to_string(), output_type.error()));
  }
  auto f = [&]<caf::spawn_options SpawnOptions>() {
    return [&]<class Input, class Output>(tag<Input>,
                                          tag<Output>) -> exec_node_actor {
      using input_type
        = std::conditional_t<std::is_void_v<Input>, std::monostate, Input>;
      using output_type
        = std::conditional_t<std::is_void_v<Output>, std::monostate, Output>;
      if constexpr (std::is_void_v<Input> and std::is_void_v<Output>) {
        die("unimplemented");
      } else {
        auto result = self->spawn<SpawnOptions>(
          exec_node<input_type, output_type>, std::move(op), std::move(node),
          std::move(diagnostics_handler), std::move(metrics_handler), index,
          has_terminal);
        return result;
      }
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
