//    _   _____   _________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/shutdown.hpp"

#include <tenzir/argument_parser2.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/multi_series.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/exit_reason.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_response_promise.hpp>
#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <queue>

namespace tenzir::plugins::window {

namespace {

template <class T>
class response_promise_queue {
public:
  response_promise_queue(caf::scheduled_actor* self, size_t capacity)
    : self_{self}, capacity_{capacity} {
  }

  response_promise_queue(response_promise_queue&&) = default;
  response_promise_queue& operator=(response_promise_queue&&) = default;
  response_promise_queue(const response_promise_queue&) = delete;
  response_promise_queue& operator=(const response_promise_queue&) = delete;

  template <class U = T>
  auto push(U&& value) -> caf::result<void> {
    if (not pull_rps_.empty()) {
      TENZIR_ASSERT(buffer_.empty());
      pull_rps_.front().deliver(std::forward<U>(value));
      pull_rps_.pop();
      return {};
    }
    buffer_.push(std::forward<U>(value));
    if (buffer_.size() >= capacity_) {
      return push_rps_.emplace(self_->make_response_promise<void>());
    }
    return {};
  }

  template <class U = T>
  auto force_push(U&& value) -> void {
    if (not pull_rps_.empty()) {
      TENZIR_ASSERT(buffer_.empty());
      pull_rps_.front().deliver(std::forward<U>(value));
      pull_rps_.pop();
      return;
    }
    buffer_.push(std::forward<U>(value));
  }

  auto pull() -> caf::result<T> {
    if (buffer_.empty()) {
      return pull_rps_.emplace(self_->make_response_promise<T>());
    }
    auto value = std::move(buffer_.front());
    buffer_.pop();
    if (not push_rps_.empty() and buffer_.size() < capacity_) {
      push_rps_.front().deliver();
      push_rps_.pop();
    }
    return value;
  }

private:
  caf::scheduled_actor* self_;
  size_t capacity_;
  std::queue<T> buffer_;
  std::queue<caf::typed_response_promise<void>> push_rps_;
  std::queue<caf::typed_response_promise<T>> pull_rps_;
};

struct window_args {
  std::optional<located<uint64_t>> window_size;
  std::optional<located<duration>> timeout;
  std::optional<located<duration>> idle_timeout;
  std::optional<located<uint64_t>> parallel;
  std::optional<located<bool>> nonblocking;
  located<pipeline> pipe;

  friend auto inspect(auto& f, window_args& x) -> bool {
    return f.object(x).fields(
      f.field("window_size", x.window_size), f.field("timeout", x.timeout),
      f.field("idle_timeout", x.idle_timeout), f.field("parallel", x.parallel),
      f.field("nonblocking", x.nonblocking), f.field("pipe", x.pipe));
  }
};

struct resolved_window_args {
  static auto make(window_args args, session ctx)
    -> failure_or<resolved_window_args> {
    auto failed = false;
    if (args.window_size and args.window_size->inner == 0) {
      diagnostic::error("window size must be at least 1")
        .primary(*args.window_size)
        .emit(ctx);
      failed = true;
    }
    if (args.timeout and args.timeout->inner <= duration::zero()) {
      diagnostic::error("timeout must be greater than zero")
        .primary(*args.timeout)
        .emit(ctx);
      failed = true;
    }
    if (args.idle_timeout and args.idle_timeout->inner <= duration::zero()) {
      diagnostic::error("idle timeout must be greater than zero")
        .primary(*args.idle_timeout)
        .emit(ctx);
      failed = true;
    }
    if (args.timeout and args.idle_timeout
        and args.timeout->inner <= args.idle_timeout->inner) {
      diagnostic::error("timeout must be greater than idle timeout")
        .primary(*args.timeout)
        .primary(*args.idle_timeout)
        .emit(ctx);
      failed = true;
    }
    if (args.parallel and args.parallel->inner == 0) {
      diagnostic::error("parallel level must be at least 1")
        .primary(*args.parallel)
        .emit(ctx);
      failed = true;
    }
    const auto output = args.pipe.inner.infer_type(tag_v<table_slice>);
    if (not output) {
      diagnostic::error("pipeline must accept `events`")
        .primary(args.pipe)
        .emit(ctx);
      failed = true;
    } else if (not output->is_any<void, table_slice>()) {
      diagnostic::error("pipeline must return `events` or `void`")
        .primary(args.pipe, "returns `{}`", operator_type_name(*output))
        .emit(ctx);
      failed = true;
    }
    if (not args.window_size and not args.timeout and not args.idle_timeout) {
      diagnostic::error("at least one of `window_size`, `timeout`, or "
                        "`idle_timeout` must be specified")
        .emit(ctx);
      failed = true;
    }
    if (failed) {
      return failure::promise();
    }
    auto result = resolved_window_args{};
    if (args.window_size) {
      result.window_size = args.window_size->inner;
    }
    if (args.timeout) {
      result.timeout = args.timeout->inner;
    }
    if (args.idle_timeout) {
      result.idle_timeout = args.idle_timeout->inner;
    }
    if (args.parallel) {
      result.parallel = args.parallel->inner;
    }
    if (args.nonblocking) {
      result.nonblocking = args.nonblocking->inner;
    }
    result.pipe = std::move(args.pipe);
    return result;
  }

  uint64_t window_size = std::numeric_limits<uint64_t>::max();
  std::optional<duration> timeout;
  std::optional<duration> idle_timeout;
  uint64_t parallel = 1;
  bool nonblocking = false;
  located<pipeline> pipe;

  friend auto inspect(auto& f, resolved_window_args& x) -> bool {
    return f.object(x).fields(
      f.field("window_size", x.window_size), f.field("timeout", x.timeout),
      f.field("idle_timeout", x.idle_timeout), f.field("parallel", x.parallel),
      f.field("nonblocking", x.nonblocking), f.field("pipe", x.pipe));
  }
};

struct window_actor_traits {
  using signatures = caf::type_list<
    // Push events from the parent pipeline into the window pipeline.
    auto(atom::push, table_slice input)->caf::result<void>,
    // Pull evaluated events into the current window.
    auto(atom::internal, atom::pull, uint64_t id)->caf::result<table_slice>,
    // Push events from the window pipeline into the parent.
    auto(atom::internal, atom::push, uint64_t id, table_slice output)
      ->caf::result<void>,
    // Get resulting events from the window pipeline into the parent pipeline.
    auto(atom::pull)->caf::result<table_slice>>
    // Support the diagnostic receiver interface for the branch pipelines.
    ::append_from<receiver_actor<diagnostic>::signatures>
    // Support the metrics receiver interface for the branch pipelines.
    ::append_from<metrics_receiver_actor::signatures>;
};

using window_actor = caf::typed_actor<window_actor_traits>;

/// The source operator used within branches of the `if` statement.
class internal_window_source_operator final
  : public crtp_operator<internal_window_source_operator> {
public:
  internal_window_source_operator() = default;

  internal_window_source_operator(window_actor window, tenzir::location source,
                                  uint64_t id)
    : window_{std::move(window)}, source_{source}, id_{id} {
  }

  auto name() const -> std::string override {
    return "internal-window-source";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    return {filter, order, this->copy()};
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto done = false;
    auto result = table_slice{};
    while (not done) {
      ctrl.self()
        .mail(atom::internal_v, atom::pull_v, id_)
        .request(window_, caf::infinite)
        .then(
          [&](table_slice input) {
            done = input.rows() == 0;
            result = std::move(input);
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to pull events into window")
              .primary(source_)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      co_yield std::move(result);
    }
  }

  friend auto inspect(auto& f, internal_window_source_operator& x) -> bool {
    return f.object(x).fields(f.field("branch", x.window_),
                              f.field("source", x.source_),
                              f.field("id", x.id_));
  }

private:
  window_actor window_;
  tenzir::location source_;
  uint64_t id_ = {};
};

/// The sink operator used within branches of the `window` operator if the
/// window had no sink of its own.
class internal_window_sink_operator final
  : public crtp_operator<internal_window_sink_operator> {
public:
  internal_window_sink_operator() = default;

  explicit internal_window_sink_operator(window_actor window,
                                         tenzir::location source, uint64_t id)
    : window_{std::move(window)}, source_{source}, id_{id} {
  }

  auto name() const -> std::string override {
    return "internal-window-sink";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    return {filter, order, this->copy()};
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    for (auto events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.self()
        .mail(atom::internal_v, atom::push_v, id_, std::move(events))
        .request(window_, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to push events from window")
              .primary(source_)
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
  }

  friend auto inspect(auto& f, internal_window_sink_operator& x) -> bool {
    return f.object(x).fields(f.field("branch", x.window_),
                              f.field("source", x.source_),
                              f.field("id", x.id_));
  }

private:
  window_actor window_;
  tenzir::location source_;
  uint64_t id_ = {};
};

/// An actor managing the nested pipelines of an `window` statement.
/// Windows can close for four reasons:
/// 1. The window stops on its own.
/// 2. The window is closed because it has reached its maximum number of events.
/// 3. The window is closed because it has reached its create timeout.
/// 4. The window is closed because it has reached its write timeout.
/// When a window closes, a new one must be opened immediately, _unless_ a
/// create timeout exists, in which case the window must be re-opened only
/// through the create timeout.
class window {
public:
  window(window_actor::pointer self, std::string definition, node_actor node,
         shared_diagnostic_handler dh, metrics_receiver_actor metrics_receiver,
         uint64_t operator_index, bool has_terminal, bool is_hidden,
         resolved_window_args args)
    : self_{self},
      definition_{std::move(definition)},
      node_{std::move(node)},
      dh_{std::move(dh)},
      metrics_receiver_{std::move(metrics_receiver)},
      operator_index_{operator_index},
      has_terminal_{has_terminal},
      is_hidden_{is_hidden},
      args_{std::move(args)},
      outputs_{self_, max_queued} {
  }

  auto make_behavior() -> window_actor::behavior_type {
    self_->set_exception_handler([this](const std::exception_ptr& exception)
                                   -> caf::error {
      try {
        std::rethrow_exception(exception);
      } catch (diagnostic& diag) {
        return std::move(diag).to_error();
      } catch (panic_exception& panic) {
        const auto has_node
          = self_->system().registry().get("tenzir.node") != nullptr;
        const auto diagnostic = to_diagnostic(panic);
        if (has_node) {
          auto buffer = std::stringstream{};
          buffer << "internal error in `window` operator\n";
          auto printer = make_diagnostic_printer(std::nullopt,
                                                 color_diagnostics::no, buffer);
          printer->emit(diagnostic);
          auto string = std::move(buffer).str();
          if (not string.empty() and string.back() == '\n') {
            string.pop_back();
          }
          TENZIR_ERROR(string);
        }
        return std::move(diagnostic).to_error();
      } catch (const std::exception& err) {
        return diagnostic::error("{}", err.what())
          .note("unhandled exception in {}", *self_)
          .to_error();
      } catch (...) {
        return diagnostic::error("unhandled exception in {}", *self_).to_error();
      }
    });
    rotate_window();
    return {
      [this](atom::push, const table_slice& input) -> caf::result<void> {
        return from_outer(input);
      },
      [this](atom::internal, atom::pull,
             uint64_t id) -> caf::result<table_slice> {
        return to_inner(id);
      },
      [this](atom::internal, atom::push, uint64_t id,
             table_slice output) -> caf::result<void> {
        return from_inner(id, std::move(output));
      },
      [this](atom::pull) -> caf::result<table_slice> {
        return to_outer();
      },
      [this](diagnostic diag) {
        return handle_diagnostic(std::move(diag));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             type schema) {
        return register_metrics(nested_operator_index, nested_metrics_id,
                                std::move(schema));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             record metrics) {
        return handle_metrics(nested_operator_index, nested_metrics_id,
                              std::move(metrics));
      },
      [](const operator_metric& metrics) {
        // We deliberately ignore operator metrics. There's no good way to
        // forward them from nested pipelines, and nowadays operator metrics
        // are really only relevant for generating pipeline metrics. If
        // there's a sink in the then-branch we're unfortunately losing its
        // egress metrics at the moment.
        TENZIR_UNUSED(metrics);
      },
      [&](caf::exit_msg msg) {
        auto handles = std::vector<caf::actor>{};
        for (const auto& window : inner_) {
          handles.push_back(caf::actor_cast<caf::actor>(window.handle));
        }
        shutdown<policy::parallel>(self_, handles, std::move(msg.reason));
      },
    };
  }

private:
  // -- utils ------------------------------------------------------------------

  auto rotate_window() -> void {
    // Close the last window, if there is one.
    if (not inner_.empty()) {
      auto& window = inner_.front();
      switch (window.state) {
        case state::starting:
        case state::running: {
          window.state = state::stopping;
          window.inputs.force_push(table_slice{});
          window.timeout.dispose();
          window.idle_timeout.dispose();
          break;
        }
        case state::stopping: {
          TENZIR_ASSERT(window.timeout.disposed());
          TENZIR_ASSERT(window.idle_timeout.disposed());
          break;
        }
      }
    }
    // If we're done then we won't create any new windows.
    if (outer_done_ and blocked_inputs_.empty()) {
      outputs_.force_push({});
      return;
    }
    // In blocking mode, we silently "advance" the start time to the current time.
    const auto now = std::chrono::steady_clock::now();
    if (not args_.nonblocking) {
      next_start_ = std::min(now, next_start_);
    }
    // If there's a create timeout, then we might need to delay the start until
    // we're supposed to start the next window, and just drop events that
    // arrive in the meantime.
    if (args_.timeout and next_start_ > now) {
      auto do_rotate_window = [this]() mutable {
        rotate_window();
      };
      self_->delay_until_fn(next_start_, std::move(do_rotate_window));
      return;
    }
    // If there are more pipelines currently waiting for shutdown than are
    // allowed to exist in parallel, then we must delay the start of the next
    // window until one of them does shut down.
    if (inner_.size() >= args_.parallel) {
      ++retry_after_window_done_;
      return;
    }
    // Let's start the next window.
    auto& window = inner_.emplace_front(this);
    // Unblock inputs, if they were waiting.
    while (not blocked_inputs_.empty()) {
      auto [head, tail] = split(blocked_inputs_.front(), window.remaining);
      window.remaining -= head.rows();
      window.inputs.force_push(std::move(head));
      if (tail.rows() > 0) {
        blocked_inputs_.front() = std::move(tail);
      } else {
        blocked_inputs_.pop();
      }
      if (window.remaining == 0) {
        break;
      }
    }
    if (blocked_inputs_.empty() and blocked_inputs_rp_.pending()) {
      blocked_inputs_rp_.deliver();
    }
    // Set up timeouts, if there are any.
    if (args_.timeout) {
      next_start_ += *args_.timeout;
      window.timeout = self_->delay_until_fn(next_start_, [this] {
        rotate_window();
      });
    }
    if (args_.idle_timeout) {
      window.idle_timeout = self_->delay_for_fn(*args_.idle_timeout, [this] {
        rotate_window();
      });
    }
    // Now, actually spawn, start, and monitor the window.
    auto pipe = args_.pipe.inner;
    pipe.prepend(std::make_unique<internal_window_source_operator>(
      window_actor{self_}, args_.pipe.source, window.id));
    if (not pipe.is_closed()) {
      pipe.append(std::make_unique<internal_window_sink_operator>(
        window_actor{self_}, args_.pipe.source, window.id));
      TENZIR_ASSERT(pipe.is_closed());
    }
    window.handle = self_->spawn(pipeline_executor, pipe.optimize_if_closed(),
                                 definition_, receiver_actor<diagnostic>{self_},
                                 metrics_receiver_actor{self_}, node_,
                                 has_terminal_, is_hidden_);
    self_->mail(atom::start_v)
      .request(window.handle, caf::infinite)
      .then(
        [this, id = window.id]() {
          TENZIR_ASSERT(not inner_.empty());
          const auto window = std::ranges::find(inner_, id, &inner::id);
          TENZIR_ASSERT(window != inner_.end());
          // There's the unlikely case that starting took longer than the write
          // or create timeouts, in which case we must not set the state back to
          // running.
          if (window->state == state::starting) {
            window->state = state::running;
          }
        },
        [this](caf::error err) {
          self_->quit(diagnostic::error(std::move(err))
                        .primary(args_.pipe, "failed to start")
                        .to_error());
        });
    self_->monitor(window.handle, [this, id = window.id](caf::error err) {
      if (err) {
        self_->quit(std::move(err));
        return;
      }
      const auto window = std::ranges::find(inner_, id, &inner::id);
      TENZIR_ASSERT(window != inner_.end());
      inner_.erase(window);
      // If we delayed window creation because we exceeded the number of
      // windows, then we must retry here.
      if (retry_after_window_done_ > 0) {
        --retry_after_window_done_;
        rotate_window();
      }
      if (not args_.timeout) {
        rotate_window();
      }
    });
  }

  // -- event forwarding -------------------------------------------------------

  auto from_outer(const table_slice& input) -> caf::result<void> {
    // The outer pipeline is done if it sends us a sentinel value.
    TENZIR_ASSERT(not outer_done_);
    outer_done_ = input.rows() == 0;
    if (outer_done_) {
      if (blocked_inputs_.empty()) {
        return outputs_.push({});
      }
      rotate_window();
      return {};
    }
    // The active window must be the first one. If it is done, then we can just
    // route the data into the void.
    if (inner_.empty() or inner_.front().state == state::stopping) {
      if (args_.nonblocking) {
        return {};
      }
      TENZIR_ASSERT(not blocked_inputs_rp_.pending());
      blocked_inputs_.push(input);
      blocked_inputs_rp_ = self_->make_response_promise<void>();
      return blocked_inputs_rp_;
    }
    auto& window = inner_.front();
    // Route the data to the active window, and if its size was exceeded, start
    // a new one.
    auto [head, tail] = split(input, window.remaining);
    window.remaining -= head.rows();
    auto result = window.inputs.push(std::move(head));
    if (args_.idle_timeout) {
      window.idle_timeout.dispose();
      window.idle_timeout = self_->delay_for_fn(*args_.idle_timeout, [this] {
        rotate_window();
      });
    }
    if (window.remaining == 0) {
      if (tail.rows() > 0) {
        blocked_inputs_.push(std::move(tail));
      }
      rotate_window();
    }
    return result;
  }

  auto to_inner(uint64_t id) -> caf::result<table_slice> {
    const auto window = std::ranges::find(inner_, id, &inner::id);
    TENZIR_ASSERT(window != inner_.end());
    return window->inputs.pull();
  }

  auto from_inner(uint64_t id, table_slice output) -> caf::result<void> {
    const auto window = std::ranges::find(inner_, id, &inner::id);
    TENZIR_ASSERT(window != inner_.end());
    return outputs_.push(std::move(output));
  }

  auto to_outer() -> caf::result<table_slice> {
    return outputs_.pull();
  }

  // -- metrics handling -------------------------------------------------------

  auto handle_diagnostic(diagnostic diag) -> caf::result<void> {
    dh_.emit(std::move(diag));
    return {};
  }

  auto register_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                        type schema) -> caf::result<void> {
    auto& id = registered_metrics[nested_operator_index][nested_metrics_id];
    id = uuid::random();
    return self_->mail(operator_index_, id, std::move(schema))
      .delegate(metrics_receiver_);
  }

  auto handle_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                      record metrics) -> caf::result<void> {
    const auto& id
      = registered_metrics[nested_operator_index][nested_metrics_id];
    return self_->mail(operator_index_, id, std::move(metrics))
      .delegate(metrics_receiver_);
  }

  enum class state {
    starting,
    running,
    stopping,
  };

  struct inner {
    explicit inner(window* window)
      : window{window},
        id{window->next_id_++},
        remaining{window->args_.window_size},
        inputs{window->self_, max_queued + window->args_.parallel - 1} {
    }

    class window* window;
    uint64_t id;
    uint64_t remaining;
    pipeline_executor_actor handle;
    caf::disposable timeout;
    caf::disposable idle_timeout;
    enum state state = state::starting;
    response_promise_queue<table_slice> inputs;
  };

  static constexpr size_t max_queued = 10;

  window_actor::pointer self_;
  std::string definition_;
  node_actor node_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor metrics_receiver_;
  uint64_t operator_index_ = 0;
  detail::flat_map<uint64_t, detail::flat_map<uuid, uuid>> registered_metrics;
  bool has_terminal_;
  bool is_hidden_;
  resolved_window_args args_;
  std::deque<inner> inner_;
  uint64_t next_id_ = 0;
  std::chrono::steady_clock::time_point next_start_
    = std::chrono::steady_clock::now();
  bool outer_done_ = false;
  response_promise_queue<table_slice> outputs_;

  uint64_t retry_after_window_done_ = 0;
  std::queue<table_slice> blocked_inputs_;
  caf::typed_response_promise<void> blocked_inputs_rp_;
};

/// The left half of the `window` operator.
class internal_window_operator final
  : public crtp_operator<internal_window_operator> {
public:
  internal_window_operator() = default;

  internal_window_operator(uuid id) : id_{id} {
  }

  auto name() const -> std::string override {
    return "internal-window";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    return {filter, order, this->copy()};
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto window = ctrl.self().system().registry().get<window_actor>(
      fmt::format("tenzir.window.{}.{}", id_, ctrl.run_id()));
    TENZIR_ASSERT(window);
    ctrl.self().system().registry().erase(window->id());
    for (auto events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.self()
        .mail(atom::push_v, std::move(events))
        .request(window, caf::infinite)
        .then(
          [&] {
            ctrl.set_waiting(false);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to push events to window")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
    }
    ctrl.self()
      .mail(atom::push_v, table_slice{})
      .request(window, caf::infinite)
      .then(
        [&] {
          ctrl.set_waiting(false);
        },
        [&](caf::error err) {
          diagnostic::error(std::move(err))
            .note("failed to push sentinel to window")
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
  }

  friend auto inspect(auto& f, internal_window_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_));
  }

private:
  uuid id_ = {};
};

/// The right half of the `window` operator.
class internal_endwindow_operator final
  : public crtp_operator<internal_endwindow_operator> {
public:
  internal_endwindow_operator() = default;

  internal_endwindow_operator(uuid id, resolved_window_args args)
    : id_{id}, args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "internal-endwindow";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    if (check(args_.pipe.inner.infer_type(tag_v<table_slice>)).is<void>()) {
      return do_not_optimize(*this);
    }
    auto result = args_.pipe.inner.optimize(filter, order);
    auto replacement = std::make_unique<internal_endwindow_operator>(*this);
    replacement->args_.pipe.inner = pipeline{};
    replacement->args_.pipe.inner.append(std::move(result.replacement));
    result.replacement = std::move(replacement);
    return result;
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // We spawn pipelines from right-to-left, so we can safely spawn this
    // operator in the internal-endwindow operator before and store it in the
    // registry as long as we do it before yielding for the first time.
    auto window = scope_linked{ctrl.self().spawn<caf::linked>(
      caf::actor_from_state<class window>, std::string{ctrl.definition()},
      ctrl.node(), ctrl.shared_diagnostics(), ctrl.metrics_receiver(),
      ctrl.operator_index(), ctrl.has_terminal(), ctrl.is_hidden(), args_)};
    ctrl.self().system().registry().put(
      fmt::format("tenzir.window.{}.{}", id_, ctrl.run_id()), window.get());
    co_yield {};
    auto output = table_slice{};
    auto done = false;
    while (not done) {
      if (auto stub = input.next()) {
        // The actual input is coming from a side-channel, so we're only
        // getting stub batchs here.
        TENZIR_ASSERT(stub->rows() == 0);
      }
      ctrl.self()
        .mail(atom::pull_v)
        .request(window.get(), caf::infinite)
        .then(
          [&](table_slice events) {
            ctrl.set_waiting(false);
            done = events.rows() == 0;
            output = std::move(events);
          },
          [&](caf::error err) {
            diagnostic::error(std::move(err))
              .note("failed to pull events from window")
              .emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      co_yield std::move(output);
    }
  }

  auto location() const -> operator_location override {
    // We pass in `ctrl.node()` to the branch actor, so if any of the nested
    // operators have a remote location, then we probably want to run the
    // `internal-endwindow` operator remotely as well.
    const auto requires_node = [](const auto& ops) {
      return std::ranges::find(ops, operator_location::remote,
                               &operator_base::location)
             != ops.end();
    };
    const auto should_be_remote = requires_node(args_.pipe.inner.operators());
    return should_be_remote ? operator_location::remote
                            : operator_location::anywhere;
  }

  friend auto inspect(auto& f, internal_endwindow_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_), f.field("args", x.args_));
  }

private:
  uuid id_ = {};
  resolved_window_args args_;
};

class window_plugin final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "window";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = window_args{};
    auto pipe_arg = std::optional<located<pipeline>>{};
    auto parser = argument_parser2::operator_(name());
    parser.named("window_size", args.window_size);
    parser.named("timeout", args.timeout);
    parser.named("idle_timeout", args.idle_timeout);
    parser.named("parallel", args.parallel);
    parser.named("_nonblocking", args.nonblocking);
    parser.positional("pipe", pipe_arg);
    TRY(parser.parse(inv, ctx));
    if (not pipe_arg) {
      // The argument parser has a bug that makes it impossible to specify a
      // required positional pipeline argument after optional named arguments.
      // We work around this by making the pipeline an optional positional
      // argument, and then manually checking if it was provided.
      diagnostic::error("missing required `pipe` argument")
        .docs(parser.docs())
        .usage(parser.usage())
        .emit(ctx);
      return failure::promise();
    }
    args.pipe = std::move(*pipe_arg);
    TRY(auto resolved_args, resolved_window_args::make(std::move(args), ctx));
    const auto id = uuid::random();
    auto result = std::make_unique<pipeline>();
    result->append(std::make_unique<internal_window_operator>(id));
    result->append(std::make_unique<internal_endwindow_operator>(
      id, std::move(resolved_args)));
    // To make the implementation of the `window` operator easier, we add
    // `discard` implicitly if the nested pipeline has a sink.
    if (check(resolved_args.pipe.inner.infer_type(tag_v<table_slice>))
          .is<void>()) {
      const auto* discard_op
        = plugins::find<operator_factory_plugin>("discard");
      TENZIR_ASSERT(discard_op);
      TRY(auto discard_pipe,
          discard_op->make({.self = inv.self, .args = {}}, ctx));
      result->append(std::move(discard_pipe));
    }
    return result;
  }
};

using internal_window_source_plugin
  = operator_inspection_plugin<internal_window_source_operator>;
using internal_window_sink_plugin
  = operator_inspection_plugin<internal_window_sink_operator>;
using internal_window_plugin
  = operator_inspection_plugin<internal_window_operator>;
using internal_endwindow_plugin
  = operator_inspection_plugin<internal_endwindow_operator>;

} // namespace

} // namespace tenzir::plugins::window

TENZIR_REGISTER_PLUGIN(tenzir::plugins::window::window_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::window::internal_window_source_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::window::internal_window_sink_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::window::internal_window_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::window::internal_endwindow_plugin)
