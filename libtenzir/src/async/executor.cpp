//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/executor.hpp"

#include "tenzir/async/fetch_node.hpp"
#include "tenzir/async/mail.hpp"
#include "tenzir/async/queue_scope.hpp"
#include "tenzir/async/unbounded_queue.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/connect_to_node.hpp"
#include "tenzir/connector.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"

#include <boost/unordered/unordered_flat_map.hpp>
#include <caf/actor_registry.hpp>
#include <folly/Executor.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

// TODO: Why does this not report line numbers correctly?
#undef TENZIR_UNREACHABLE
#define TENZIR_UNREACHABLE()                                                   \
  TENZIR_ERROR("unreachable");                                                 \
  tenzir::panic("unreachable")

namespace tenzir {

// Forward declaration to avoid including registry.hpp.
auto global_registry() -> std::shared_ptr<const registry>;

class Pass final : public Operator<table_slice, table_slice> {
public:
  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    co_await push(std::move(input));
  }
};

enum class TryRecvError {
  empty,
  closed,
};

/// Data shared between senders and receivers.
template <class T>
struct SenderReceiverShared {
  explicit SenderReceiverShared(size_t capacity)
    : data{detail::narrow<uint32_t>(capacity)} {
  }

  /// The actual data, where `None` is used to signal to receivers that the
  /// channel is closed because the last sender got destroyed.
  folly::coro::BoundedQueue<Option<T>> data;
  /// Number of remaining senders. Channel is closed when this drops to 0.
  std::atomic<size_t> senders;

  bool is_closed() const {
    return senders.load() == 0;
  }
};

/// Handle to the sending end of a channel.
///
/// Dropping the last sender closes the channel.
template <class T>
class Sender {
public:
  explicit Sender(std::shared_ptr<SenderReceiverShared<T>> shared)
    : shared_{std::move(shared)} {
    TENZIR_ASSERT(shared_);
    shared_->senders += 1;
  }

  ~Sender() {
    if (shared_) {
      auto previous = shared_->senders.fetch_sub(1);
      TENZIR_ASSERT(previous > 0);
      if (previous == 1) {
        // FIXME: If this happens while the channel is full, then we can't push
        // anything and need a different way to signal it.
        auto success = shared_->data.try_enqueue(None{});
        if (not success) {
          TENZIR_TODO();
        }
      }
    }
  }

  // TODO: Make this copyable.
  Sender(const Sender&) = delete;
  Sender& operator=(const Sender&) = delete;
  Sender(Sender&&) = default;
  Sender& operator=(Sender&&) = default;

  /// Sends a value to the channel, waiting for capacity.
  auto send(T x) -> Task<void> {
    TENZIR_ASSERT(shared_);
    co_await shared_->data.enqueue(std::move(x));
  }

  /// Sends a value if the channel is not full.
  auto try_send(T x) -> Result<void, T> {
    auto success = shared_->data.try_enqueue(std::move(x));
    if (not success) {
      // Unlike our own function, `try_enqueue` does not actually move the value
      // out if it fails to enqueue, so we can just "move it again" here.
      return Err{std::move(x)};
    }
    return {};
  }

private:
  std::shared_ptr<SenderReceiverShared<T>> shared_;
};

/// Handle to the receiving end of a channel.
///
/// Unlike in Rust, dropping the receiver does not close the channel. The sender
/// might thus eventually block. The outer system needs to be designed such that
/// a dropped receiver eventually leads to cancellation of the sender. This is
/// not an oversight, but a conscious choice.
template <class T>
class Receiver {
public:
  explicit Receiver(std::shared_ptr<SenderReceiverShared<T>> shared)
    : shared_{std::move(shared)} {
    TENZIR_ASSERT(shared_);
  }

  /// Returns `None` if channel is closed.
  auto recv() -> Task<Option<T>> {
    TENZIR_ASSERT(shared_);
    auto result = co_await shared_->data.dequeue();
    if (not result) {
      TENZIR_ASSERT(shared_->is_closed());
      // Channel is closed and we just popped an element. There must be space.
      auto success = shared_->data.try_enqueue(None{});
      TENZIR_ASSERT(success);
      co_return None{};
    }
    co_return std::move(*result);
  }

  /// Returns immediately, indicating whether the channel is empty or closed.
  auto try_recv() -> Task<Result<T, TryRecvError>> {
    TENZIR_ASSERT(shared_);
    auto result = shared_->data.try_dequeue();
    if (not result) {
      // The queue is emtpy, but maybe a different receiver took the marker that
      // the channel is closed. We thus have to check for this.
      if (shared_->is_closed()) {
        return TryRecvError::closed;
      }
      return TryRecvError::empty;
    }
    if (not *result) {
      TENZIR_ASSERT(shared_->is_closed());
      // Make sure we put back the marker.
      auto success = shared_->data.try_enqueue(None{});
      // The queue is empty now, so this must succeed.
      TENZIR_ASSERT(success);
      return TryRecvError::closed;
    }
    return std::move(**result);
  }

  auto into_generator() && -> AsyncGenerator<T> {
    return folly::coro::co_invoke(
      [self = std::move(*this)] mutable -> AsyncGenerator<T> {
        while (auto result = co_await self.recv()) {
          co_yield std::move(*result);
        }
      });
  }

private:
  std::shared_ptr<SenderReceiverShared<T>> shared_;
};

template <class T>
struct SenderReceiver {
  Sender<T> sender;
  Receiver<T> receiver;
};

template <class T>
auto bounded_channel(size_t capacity) -> SenderReceiver<T> {
  auto shared = std::make_shared<SenderReceiverShared<T>>(capacity);
  return {Sender<T>{shared}, Receiver<T>{shared}};
}

/// Transforms a `Push<OperatorMsg<T>>` into a `Push<T>`.
template <class T>
class OpPushWrapper final : public Push<T> {
public:
  explicit OpPushWrapper(Push<OperatorMsg<T>>& push) : push_{push} {
  }

  virtual auto operator()(T output) -> Task<void> override {
    return push_(std::move(output));
  }

private:
  Push<OperatorMsg<T>>& push_;
};

template <class T>
OpPushWrapper(Box<Push<OperatorMsg<T>>>&) -> OpPushWrapper<T>;

/// A type-erased upstream message: either data or a signal.
using AnyOperatorMsg = variant<table_slice, chunk_ptr, Signal>;

/// Type-erased upstream pull.
using AnyOpPull
  = variant<Box<Pull<OperatorMsg<void>>>, Box<Pull<OperatorMsg<chunk_ptr>>>,
            Box<Pull<OperatorMsg<table_slice>>>>;

/// Type-erased downstream push.
using AnyOpPush
  = variant<Box<Push<OperatorMsg<void>>>, Box<Push<OperatorMsg<chunk_ptr>>>,
            Box<Push<OperatorMsg<table_slice>>>>;

// Wraps an `Any` but without the implicit construction from values.
struct ExplicitAny {
  explicit ExplicitAny(Any value) : value{std::move(value)} {
  }

  Any value;
};

template <class Input>
struct SubPipeline {
  SubPipeline(Box<Push<OperatorMsg<Input>>> push,
              Sender<FromControl> from_control)
    : push{std::move(push)}, from_control{std::move(from_control)} {
  }

  Box<Push<OperatorMsg<Input>>> push;
  Sender<FromControl> from_control;
};

class AnySubpipeline : public variant<SubPipeline<void>, SubPipeline<chunk_ptr>,
                                      SubPipeline<table_slice>> {
public:
  using variant::variant;

  auto send(Signal signal) -> Task<void> {
    return co_match(*this, [&]<class Input>(SubPipeline<Input>& self) {
      return self.push(std::move(signal));
    });
  }

  auto send(FromControl from_control) -> Task<void> {
    return co_match(*this, [&]<class Input>(SubPipeline<Input>& self) {
      return self.from_control.send(std::move(from_control));
    });
  }

  /// Destroy the push handle to close the subpipeline's upstream channel.
  void close_push() {
    co_match(*this, [&]<class Input>(SubPipeline<Input>& self) {
      auto _ = std::move(self.push);
    });
  }
};

struct SubPipelineInfo {
  /// The handle is given out to the inner implementation.
  AnySubpipeline handle;
  /// Save the instantiated IR because we need that to restore.
  ir::pipeline ir;
  /// Also need the input type it was spawned with to recreate it.
  element_type_tag input;
  /// True if this subpipeline received the last checkpoint, requiring a commit.
  bool wants_commit = false;
  /// True if this subpipeline has sent EndOfData back to us.
  bool finished = false;
  /// True if the subpipeline runner task has fully terminated.
  bool terminated = false;
};

struct Terminated {};
struct SubPipelineFinished {};
struct SubPipelineReadyForShutdown {};
struct SubPipelineTerminated {};

struct SubPipelineEvent {
  data key;
  variant<SubPipelineFinished, SubPipelineReadyForShutdown,
          SubPipelineTerminated>
    event;
};

class MutexDiagnosticHandler final : public diagnostic_handler {
public:
  explicit MutexDiagnosticHandler(diagnostic_handler& dh) : dh_{dh} {
  }

  auto emit(diagnostic diag) -> void override {
    auto lock = std::scoped_lock{mut_};
    dh_.emit(std::move(diag));
  }

private:
  diagnostic_handler& dh_;
  std::mutex mut_;
};

class CensoringDiagHandler : public DiagHandler {
public:
  explicit CensoringDiagHandler(DiagHandler& dh) : dh_{dh} {
  }

  auto emit(diagnostic diag) -> void override {
    dh_->emit(censor_.censor(diag));
  }

  auto failure() -> failure_or<void> override {
    return dh_->failure();
  }

  auto censor() -> secret_censor& {
    return censor_;
  }

private:
  Ref<DiagHandler> dh_;
  secret_censor censor_;
};

class Runner final : public OpCtx {
public:
  Runner(AnyOperator op, AnyOpPull pull_upstream, AnyOpPush push_downstream,
         Receiver<FromControl> from_control, Sender<ToControl> to_control,
         OpId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh,
         std::shared_ptr<pipeline_metrics> metrics)
    : op_{std::move(op)},
      pull_upstream_{std::move(pull_upstream)},
      push_downstream_{std::move(push_downstream)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      id_{std::move(id)},
      exec_ctx_{exec_ctx},
      dh_{dh},
      sys_{sys},
      input_is_void_{
        match(op_,
              []<class In, class Out>(const Box<Operator<In, Out>>&) {
                return std::same_as<In, void>;
              })},
      output_is_void_{
        match(op_,
              []<class In, class Out>(const Box<Operator<In, Out>>&) {
                return std::same_as<Out, void>;
              })},
      metrics_{std::move(metrics)} {
  }

  Runner(Runner&&) = delete;
  Runner& operator=(Runner&&) = delete;
  Runner(const Runner&) = delete;
  Runner& operator=(const Runner&) = delete;
  ~Runner() override = default;

  auto run_to_completion() && -> Task<void> {
    auto guard = detail::scope_guard{[&] noexcept {
      LOGW("returning from operator runner {}", id_);
    }};
    LOGV("creating runner scope");
    co_await queue_.activate([&] -> Task<void> {
      // TODO: Figure out where exactly the operator scope is and move this.
      LOGV("creating operator scope");
      co_await async_scope([&](AsyncScope& operator_scope) -> Task<void> {
        TENZIR_ASSERT(not operator_scope_);
        operator_scope_ = &operator_scope;
        auto guard = detail::scope_guard{[&] noexcept {
          operator_scope_ = nullptr;
        }};
        co_await run();
        // Cancel operator-spawned tasks (e.g., background IO coroutines) so
        // the scope join does not block on them.
        operator_scope.cancel();
      });
    });
  }

private:
  auto actor_system() -> caf::actor_system& override {
    return sys_;
  }

  auto reg() -> const registry& override {
    return *reg_;
  }

  auto save_checkpoint(chunk_ptr chunk) -> Task<void> override {
    TENZIR_UNUSED(chunk);
    co_return;
  }

  auto load_checkpoint() -> Task<chunk_ptr> override {
    co_return {};
  }

  auto flush() -> Task<void> override {
    co_return;
  }

  auto dh() -> diagnostic_handler& override {
    return dh_;
  }

  auto make_counter(metrics_label label, metrics_direction direction,
                    metrics_visibility visibility) -> metrics_counter override {
    if (not metrics_) {
      return {};
    }
    return metrics_->make_counter(std::move(label), direction, visibility);
  }

  auto metrics() const -> std::shared_ptr<pipeline_metrics> const& override {
    return metrics_;
  }

  auto resolve_secrets(std::vector<secret_request> requests)
    -> Task<failure_or<void>> override {
    // All secrets that must be resolved by the node/platform.
    auto requested_secrets = request_map_t();
    // The finishers (operations) that must be performed after the value is known.
    auto finishers = std::vector<secret_finisher>{};
    // First, iterate all requests we got to see if any need to be looked up
    // remotely and to build the finisher list.
    for (auto& req : requests) {
      const auto collect = detail::overload{
        [](const fbs::data::SecretLiteral&) -> void {
          ; /* noop */
        },
        [&requested_secrets,
         loc = req.location](const fbs::data::SecretName& n) -> void {
          requested_secrets.try_emplace(detail::secrets::deref(n.value()).str(),
                                        loc);
        },
        [](this const auto& self,
           const fbs::data::SecretConcatenation& concat) -> void {
          for (const auto* p : detail::secrets::deref(concat.secrets())) {
            match(detail::secrets::deref(p), self);
          }
        },
        [](this const auto& self,
           const fbs::data::SecretTransformed& trafo) -> void {
          match(detail::secrets::deref(trafo.secret()), self);
        },
      };
      match(req.secret, collect);
      finishers.emplace_back(std::move(req.secret), std::move(req.callback),
                             req.location);
    }
    if (requested_secrets.empty()) {
      /// Finish all secrets via the respective finisher.
      auto success = true;
      for (const auto& f : finishers) {
        success
          &= static_cast<bool>(f.finish(requested_secrets, dh_.censor(), dh_));
      }
      if (not success) {
        co_return failure::promise();
      }
      co_return {};
    }
    auto node = co_await fetch_node(sys_, dh_);
    if (not node or not *node) {
      co_return failure::promise();
    }
    // All futures for requests we send to the node
    auto futures
      = std::vector<folly::SemiFuture<caf::expected<secret_resolution_result>>>{};
    // Key pairs used for the request encryption per request
    auto keys = std::vector<ecc::string_keypair>{};
    for (auto& [name, out] : requested_secrets) {
      auto key_pair = ecc::generate_keypair();
      TENZIR_ASSERT(key_pair);
      auto public_key = key_pair->public_key;
      futures.push_back(
        async_mail(atom::resolve_v, name, public_key).request(*node));
      keys.push_back(std::move(*key_pair));
    }
    const auto results = co_await folly::collectAll(std::move(futures));
    // We use a bool here to be able to validate all secrets instead of early
    // exiting.
    auto success = true;
    for (auto&& [result, key, secret] :
         std::views::zip(results, std::as_const(keys), requested_secrets)) {
      auto& [name, out] = secret;
      auto& expected = result.value();
      if (not expected) {
        diagnostic::error(expected.error())
          .primary(out.loc, "secret `{}` failed", name)
          .emit(dh_);
        success = false;
        continue;
      }
      match(
        *expected,
        [&](const encrypted_secret_value& v) {
          auto decrypted = ecc::decrypt(v.value, key);
          if (not decrypted) {
            diagnostic::error("failed to decrypt secret: {}", decrypted.error())
              .primary(out.loc, "secret `{}` failed", name)
              .emit(dh_);
            success = false;
            return;
          }
          out.value = std::move(*decrypted);
        },
        [&](const secret_resolution_error& e) {
          diagnostic::error("could not get secret value: {}", e.message)
            .primary(out.loc, "secret `{}` failed", name)
            .emit(dh_);
          success = false;
        });
    }
    if (not success) {
      co_return failure::promise();
    }
    /// Finish all secrets via the respective finisher.
    for (const auto& f : finishers) {
      success
        &= static_cast<bool>(f.finish(requested_secrets, dh_.censor(), dh_));
    }
    if (not success) {
      co_return failure::promise();
    }
    co_return {};
  }

  auto spawn_sub(SubKey key, ir::pipeline pipe, element_type_tag input)
    -> Task<AnyOpenPipeline> override {
    auto sub_id = id_.sub(next_subpipeline_id_);
    next_subpipeline_id_ += 1;
    auto spawned = std::move(pipe).spawn(input);
    // FIXME: Currently only subpipelines that return table slice are allowed.
    auto chain = match(
      input,
      [&]<class In>(tag<In>) -> variant<OperatorChain<void, table_slice>,
                                        OperatorChain<table_slice, table_slice>,
                                        OperatorChain<chunk_ptr, table_slice>> {
        auto result
          = OperatorChain<In, table_slice>::try_from(std::move(spawned));
        TENZIR_ASSERT(result);
        return std::move(*result);
      });
    auto [push_downstream, pull_downstream]
      = exec_ctx_.make<table_slice>(id_.to(sub_id.op(0)));
    auto [from_control_sender, from_control_receiver]
      = bounded_channel<FromControl>(16);
    auto [to_control_sender, to_control_receiver]
      = bounded_channel<ToControl>(16);
    auto [runner, push_upstream] = co_match(
      std::move(chain),
      [&]<class In>(OperatorChain<In, table_slice> chain)
        -> std::pair<Task<void>, variant<Box<Push<OperatorMsg<void>>>,
                                         Box<Push<OperatorMsg<table_slice>>>,
                                         Box<Push<OperatorMsg<chunk_ptr>>>>> {
        auto [push_upstream, pull_upstream]
          = exec_ctx_.make<In>(sub_id.op(chain.size() - 1).to(id_));
        return {
          run_chain(std::move(chain), std::move(pull_upstream),
                    std::move(push_downstream),
                    std::move(from_control_receiver),
                    std::move(to_control_sender), std::move(sub_id), exec_ctx_,
                    sys_, dh_, metrics_),
          std::move(push_upstream),
        };
      });
    auto end_of_data = std::make_shared<Notify>();
    queue_.scope().spawn(
      [this, key, end_of_data,
       pull_downstream = std::move(pull_downstream)] mutable -> Task<void> {
        // Pulling from the subpipeline needs to happen independently from the
        // main operator logic. This is because we might block when pushing to
        // the subpipeline due to backpressure, but in order to alleviate the
        // backpressure, we need to pull from it.
        while (true) {
          auto output = co_await pull_downstream();
          if (not output) {
            break;
          }
          co_await co_match(
            std::move(*output),
            [&](table_slice output) -> Task<void> {
              co_await call_process_sub(make_view(key), std::move(output));
            },
            [&](Signal signal) -> Task<void> {
              co_await co_match(
                signal,
                [&](EndOfData) -> Task<void> {
                  co_await queue_.insert(
                    SubPipelineEvent{key, SubPipelineFinished{}});
                  // TODO: Can we do this?
                  end_of_data->notify_one();
                },
                [&](Checkpoint) -> Task<void> {
                  // TODO: Handle checkpoint.
                  co_return;
                });
            });
        }
        end_of_data->notify_one();
      });
    // TODO: Should this even be a concurrent task?
    auto control_handle
      = queue_.scope().spawn([this, key,
                              to_control_receiver = std::move(
                                to_control_receiver)] mutable -> Task<void> {
          while (true) {
            auto to_control = co_await to_control_receiver.recv();
            if (not to_control) {
              LOGW("control channel from subpipeline got closed");
              co_return;
            }
            switch (*to_control) {
              case ToControl::no_more_input:
                // TODO: How do we inform the operator that the subpipeline
                // doesn't want anymore input?
                continue;
              case ToControl::ready_for_shutdown:
                co_await queue_.insert(
                  SubPipelineEvent{key, SubPipelineReadyForShutdown{}});
                co_return;
              case ToControl::checkpoint_begin:
              case ToControl::checkpoint_done:
                TENZIR_TODO();
            }
            TENZIR_UNREACHABLE();
          }
        });
    // Spawn the wired-up chain runner, adding a `SubPipelineTerminated`
    // event into the queue once it's done.
    queue_.spawn(
      [key, control_handle, end_of_data,
       runner = std::move(runner)]() mutable -> Task<SubPipelineEvent> {
        co_await std::move(runner);
        // Also wait for both control and output messages to guarantee order in
        // the queue.
        co_await control_handle.join();
        co_await end_of_data->wait();
        co_return SubPipelineEvent{key, SubPipelineTerminated{}};
      });
    // Insert the resulting subpipeline into our internal state.
    co_return co_match(push_upstream,
                       [&]<class In>(Box<Push<OperatorMsg<In>>>& push_upstream)
                         -> AnyOpenPipeline {
                         auto [it, inserted] = subpipelines_.try_emplace(
                           std::move(key), SubPipeline{
                                             std::move(push_upstream),
                                             std::move(from_control_sender),
                                           });
                         if (not inserted) {
                           panic("already have a subpipeline for that key");
                         }
                         auto& subpipe = as<SubPipeline<In>>(it->second.handle);
                         return OpenPipeline<In>{*subpipe.push};
                       });
  }

  auto get_sub(SubKeyView key) -> std::optional<AnyOpenPipeline> override {
    // TODO: This is bad.
    auto it = subpipelines_.find(materialize(key));
    if (it == subpipelines_.end()) {
      return std::nullopt;
    }
    auto& sub = it->second;
    return co_match(
      sub.handle,
      []<class In>(SubPipeline<In>& subpipeline) -> AnyOpenPipeline {
        return OpenPipeline<In>{*subpipeline.push};
      });
  }

  auto io_executor() -> folly::Executor::KeepAlive<> override {
    if (not io_executor_) {
      io_executor_ = exec_ctx_.make_io_executor(id_);
    }
    return io_executor_;
  }

  auto spawn_task(Task<void> task) -> AsyncHandle<void> override {
    TENZIR_ASSERT(operator_scope_);
    return operator_scope_->spawn(std::move(task));
  }

  /// Access the OperatorBase interface.
  auto base_op() -> OperatorBase& {
    return match(op_, [](auto& op) -> OperatorBase& {
      return *op;
    });
  }

  /// Pull one message from upstream, converting to AnyOperatorMsg.
  auto pull_upstream() -> Task<Option<AnyOperatorMsg>> {
    return co_match(
      pull_upstream_, [](auto& pull) -> Task<Option<AnyOperatorMsg>> {
        auto result = co_await (*pull)();
        if (not result) {
          co_return {};
        }
        co_return match(std::move(*result), [](auto x) -> AnyOperatorMsg {
          return std::move(x);
        });
      });
  }

  /// Push a signal downstream, regardless of output type.
  auto push_signal(Signal signal) -> Task<void> {
    co_await co_match(
      push_downstream_,
      [&]<class T>(Box<Push<OperatorMsg<T>>>& push) -> Task<void> {
        co_await push(std::move(signal));
      });
  }

  /// Get the operator's type name for logging.
  auto op_name() const -> const char* {
    return match(op_, [](const auto& op) {
      return typeid(*op).name();
    });
  }

  auto call_process_task(Any result) -> Task<void> {
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->process_task(std::move(result), *this);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->process_task(std::move(result), wrapper, *this);
        }
      });
  }

  auto call_finalize() -> Task<FinalizeBehavior> {
    co_return co_await co_match(
      op_,
      [&]<class In, class Out>(
        Box<Operator<In, Out>>& op) -> Task<FinalizeBehavior> {
        if constexpr (std::same_as<Out, void>) {
          co_return co_await op->finalize(*this);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_return co_await op->finalize(wrapper, *this);
        }
      });
  }

  auto call_process_sub(SubKeyView key, table_slice slice) -> Task<void> {
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->process_sub(key, std::move(slice), *this);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->process_sub(key, std::move(slice), wrapper, *this);
        }
      });
  }

  auto call_finish_sub(SubKeyView key) -> Task<void> {
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->finish_sub(key, *this);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->finish_sub(key, wrapper, *this);
        }
      });
  }

  template <class DataInput>
  auto call_process(DataInput input) -> Task<void> {
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<In, DataInput>) {
          if constexpr (std::same_as<Out, void>) {
            co_await op->process(input, *this);
          } else {
            auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
            auto wrapper = OpPushWrapper{push};
            co_await op->process(input, wrapper, *this);
          }
        } else {
          TENZIR_UNREACHABLE();
        }
      });
  }

  auto run() -> Task<void> {
    // co_await folly::coro::co_scope_exit(
    //   [](Runner* self) -> Task<void> {
    //     LOGW("shutting down operator {} with {} pending",
    //                 typeid(*self->op_).name(), self->queue_.pending());
    //     // TODO: Can we always do this here?
    //     co_await self->queue_.cancel_and_join();
    //     LOGW("shutdown done for {}", typeid(*self->op_).name());
    //   },
    //   this);
    try {
      LOGI("-> pre start");
      {
        // TODO: What if we don't restore? No data? Flag?
        auto data = co_await this->load_checkpoint();
        if (data) {
          auto f = caf::binary_deserializer{
            caf::const_byte_span{data->data(), data->size()}};
          auto ok = f.begin_object(caf::invalid_type_id, "");
          TENZIR_ASSERT(ok);
          auto serde = Serde{f};
          base_op().snapshot(serde);
          ok = f.end_object();
          TENZIR_ASSERT(ok);
        }
      }
      co_await base_op().start(*this);
      LOGI("-> post start");
      queue_.spawn([this] -> Task<ExplicitAny> {
        co_return ExplicitAny{co_await base_op().await_task(*this)};
      });
      queue_.spawn(pull_upstream());
      queue_.spawn(from_control_.recv());
      // Process until we got a shutdown request and the upstream channel
      // closed. The upstream closure cascades: each operator waits for its
      // predecessor to shut down and drop its push handle first.
      while (not got_shutdown_request_ or not upstream_done_) {
        co_await folly::coro::co_safe_point;
        co_await tick();
      }
    } catch (folly::OperationCancelled) {
      LOGV("shutting down operator after cancellation");
      throw;
    } catch (std::exception& e) {
      LOGE("shutting down operator after uncaught exception: {}", e.what());
      throw;
    } catch (...) {
      LOGE("shutting down operator after uncaught exception");
      throw;
    }
    LOGW("CANCELING queue");
    queue_.cancel();
  }

  auto tick() -> Task<void> {
    ticks_ += 1;
    LOGI("tick {} in {} ({})", ticks_, id_, op_name());
    switch (base_op().state()) {
      case OperatorState::done:
        co_await handle_done();
        break;
      case OperatorState::unspecified:
        break;
    }
    LOGV("waiting in {} for message", op_name());
    auto message = (co_await queue_.next()).unwrap();
    co_await co_match(std::move(message), [&](auto message) {
      return process(std::move(message));
    });
  }

  auto process(ExplicitAny message) -> Task<void> {
    // The task provided by the inner implementation completed.
    LOGV("got future result in {}", op_name());
    co_await call_process_task(std::move(message.value));
    if (base_op().state() == OperatorState::done) {
      co_await handle_done();
    } else {
      queue_.spawn([this] -> Task<ExplicitAny> {
        co_return ExplicitAny{co_await base_op().await_task(*this)};
      });
    }
    LOGV("handled future result in {}", op_name());
  }

  auto process(Option<AnyOperatorMsg> message) -> Task<void> {
    if (not message) {
      upstream_done_ = true;
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](table_slice input) -> Task<void> {
        LOGV("got input in {}", op_name());
        if (is_done_) {
          co_return;
        }
        co_await call_process(std::move(input));
      },
      [&](chunk_ptr input) -> Task<void> {
        LOGV("got input in {}", op_name());
        if (is_done_) {
          co_return;
        }
        co_await call_process(std::move(input));
      },
      [&](Signal signal) -> Task<void> {
        co_await co_match(
          signal,
          [&](EndOfData) -> Task<void> {
            LOGV("got end of data in {}", op_name());
            TENZIR_ASSERT(not input_is_void_);
            co_await handle_done();
          },
          [&](Checkpoint checkpoint) -> Task<void> {
            co_await begin_checkpoint(checkpoint);
          });
      });
    queue_.spawn(pull_upstream());
  }

  /// Checkpoint the operator and all of its subpipelines.
  ///
  /// This works as follows:
  /// - First, checkpoint the state of the operator itself.
  /// - If there are no subpipelines: We are done, send checkpoint downstream.
  /// - Otherwise, forward the checkpoint to all subpipelines.
  /// - Remember which ones exist for the commit notification later.
  /// X Block output (perhaps with buffer) of the inner operator.
  /// X Also block output subpipelines created after this point.
  /// - Start asynchronous subpipeline checkpointing and return.
  ///
  /// Asynchronous subpipeline checkpointing works as follows:
  /// X Wait for all subpipelines that got the checkpoint to return it to us.
  /// X Once a subpipeline returns its checkpoint, its output is also blocked.
  /// X When we got all checkpoints, we forward it to our actual downstream.
  /// X Then, unblock our output and blocked subpipelines (which means all).
  /// X The checkpoint is now complete.
  ///
  /// When blocking the output of a subpipeline, should we call `process_sub`?
  /// If it does not mutate the operator state, then there is no problem. If it
  /// does, but does, then we have to be careful: If we then process something
  /// from another subpipeline that is not yet blocked (i.e. checkpoint is
  /// pending), and propagate information from that with that event, then we
  /// have information from after the checkpoint in something that comes before
  /// the checkpoint. This must not happen. On the other hand, the same can
  /// happen for mutations that go through `process`. So we should not call
  /// `process_sub` for blocked subpipelines.
  ///
  /// Could we build a loop? Let's say `while foo < 42 { … }`. We would then
  /// have to forward the checkpoint, but also record incoming data from the
  /// back-edge before the checkpoint comes back. This needs to be part of the
  /// snapshot, but we already forwarded the checkpoint, so we need to inform
  /// the system that it must wait for the checkpoint of the back-edge. This
  /// then covers then in-flight data at the point in time when the original
  /// snapshot was created.
  ///
  /// Can we apply this to `process_sub`? We perform the snapshot, but then
  /// record all incoming data as it comes out of the subpipelines... Hmm...
  /// No, that doesn't sound great.
  ///
  /// When we get a commit notification, we forward it only to those
  /// subpipelines that already existed when we started the checkpoint. In the
  /// meantime, we must take care to not destroy those subpipelines.
  auto begin_checkpoint(Checkpoint checkpoint) -> Task<void> {
    LOGI("got checkpoint {} in {}", checkpoint.id, op_name());
    co_await to_control_.send(ToControl::checkpoint_begin);
    {
      auto buffer = caf::byte_buffer{};
      auto f = caf::binary_serializer{buffer};
      auto ok = f.begin_object(caf::invalid_type_id, "");
      TENZIR_ASSERT(ok);
      auto serde = Serde{f};
      base_op().snapshot(serde);
      ok = f.end_object();
      TENZIR_ASSERT(ok);
      co_await this->save_checkpoint(chunk::make(std::move(buffer)));
    }
    // TODO: Also checkpoint `subpipelines_`. What else?
    if (subpipelines_.empty()) {
      LOGI("finishing checkpoint with no subpipelines in {} ", op_name());
      co_await to_control_.send(ToControl::checkpoint_done);
      co_await push_signal(Signal{std::move(checkpoint)});
      co_return;
    }
    LOGI("checkpointing {} subpipelines in {}", subpipelines_.size(),
         op_name());
    for (auto& [_, sub] : subpipelines_) {
      co_await sub.handle.send(checkpoint);
      sub.wants_commit = true;
    }
    // TODO: Continue here?
  }

  auto process(Option<FromControl> message) -> Task<void> {
    if (not message) {
      // This indicates that control died. This should only happen if it is
      // cancelled. However, we should also be cancelled by then.
      TENZIR_UNREACHABLE();
    }
    co_await co_match(
      std::move(*message),
      [&](PostCommit) -> Task<void> {
        LOGV("got post commit in {}", op_name());
        co_await base_op().post_commit(*this);
      },
      [&](Shutdown) -> Task<void> {
        // FIXME: Cleanup on shutdown?
        LOGV("got shutdown in {}", op_name());
        got_shutdown_request_ = true;
        // TODO: Should we really just forward this here?
        for (auto& [_, sub] : subpipelines_) {
          co_await sub.handle.send(Shutdown{});
          // Close upstream push so the subpipeline's first operator
          // observes None, unless handle_done() already did this.
          sub.handle.close_push();
        }
        co_return;
      },
      [&](Stop) -> Task<void> {
        co_await handle_done();
      });
    queue_.spawn(from_control_.recv());
  }

  auto process(SubPipelineEvent message) -> Task<void> {
    co_await co_match(
      std::move(message.event),
      [&](SubPipelineFinished) -> Task<void> {
        auto maybe_cleanup_subpipeline = [this](auto it) -> Task<void> {
          // We currently do not guarantee ordering between the Finished and
          // Terminated messages. This should not happen once the subpipeline
          // implementation is cleaned up.
          if (it->second.finished and it->second.terminated) {
            subpipelines_.erase(it);
            co_await try_ready_for_shutdown();
          }
        };
        co_await call_finish_sub(make_view(message.key));
        auto it = subpipelines_.find(message.key);
        TENZIR_ASSERT(it != subpipelines_.end());
        TENZIR_ASSERT(not it->second.finished);
        it->second.finished = true;
        co_await maybe_cleanup_subpipeline(it);
        co_await try_send_end_of_data();
      },
      [&](SubPipelineReadyForShutdown) -> Task<void> {
        auto it = subpipelines_.find(message.key);
        TENZIR_ASSERT(it != subpipelines_.end());
        if (it->second.terminated) {
          co_return;
        }
        co_await it->second.handle.send(Shutdown{});
        co_return;
      },
      [&](SubPipelineTerminated) -> Task<void> {
        auto it = subpipelines_.find(message.key);
        TENZIR_ASSERT(it != subpipelines_.end());
        it->second.terminated = true;
        if (it->second.finished) {
          subpipelines_.erase(it);
          co_await try_ready_for_shutdown();
        }
      });
  }

  auto handle_done() -> Task<void> {
    // We want to run this code once.
    if (is_done_) {
      co_return;
    }
    LOGV("running done in {}", op_name());
    is_done_ = true;
    // Immediately inform control that we want no more data.
    if (not input_is_void_) {
      co_await to_control_.send(ToControl::no_more_input);
    }
    // auto behavior = co_await call_finalize_behavior();
    // Then finalize the operator, which can still produce output.
    auto b = co_await call_finalize();
    if (b == FinalizeBehavior::continue_) {
      is_done_ = false;
      co_return;
    }
    // Tell all subpipelines to shut down. Note that the previous step could
    // have still pushed data into them. The main loop continues running to
    // drain remaining subpipeline output and collect SubPipelineFinished
    // messages. `try_finish_done()` completes the shutdown once all are gone.
    // If we already got a shutdown request, the Shutdown handler already
    // closed the subpipeline push handles, so skip sending EndOfData.
    if (not got_shutdown_request_) {
      for (auto& [key, sub] : subpipelines_) {
        co_await co_match(sub.handle,
                          []<class In>(SubPipeline<In>& sub) -> Task<void> {
                            // TODO: What if this is a source?
                            if constexpr (not std::same_as<In, void>) {
                              co_await sub.push(EndOfData{});
                            }
                          });
      }
      // Close all subpipeline upstream pushes so their first operators
      // observe None after the EndOfData, enabling orderly shutdown.
      for (auto& [key, sub] : subpipelines_) {
        sub.handle.close_push();
      }
    }
    co_await try_send_end_of_data();
    co_await try_ready_for_shutdown();
  }

  auto all_subpipelines_finished() const -> bool {
    return std::ranges::all_of(subpipelines_, [](auto const& kv) {
      return kv.second.finished;
    });
  }

  auto try_send_end_of_data() -> Task<void> {
    if (not is_done_ or not all_subpipelines_finished()) {
      co_return;
    }
    if (not output_is_void_) {
      LOGW("sending end of data from {}", id_);
      co_await push_signal(EndOfData{});
    }
  }

  auto try_ready_for_shutdown() -> Task<void> {
    if (not is_done_ or not subpipelines_.empty()) {
      co_return;
    }
    LOGW("sending ready to shutdown from {}", id_);
    co_await to_control_.send(ToControl::ready_for_shutdown);
  }

  AnyOperator op_;
  AnyOpPull pull_upstream_;
  AnyOpPush push_downstream_;
  Receiver<FromControl> from_control_;
  Sender<ToControl> to_control_;
  OpId id_;
  ExecCtx& exec_ctx_;
  CensoringDiagHandler dh_;
  caf::actor_system& sys_;
  bool input_is_void_;
  bool output_is_void_;
  std::shared_ptr<pipeline_metrics> metrics_;
  std::shared_ptr<const registry> reg_ = global_registry();
  folly::Executor::KeepAlive<> io_executor_;

  size_t next_subpipeline_id_ = 0;

  // TODO: Better map type.
  std::unordered_map<data, SubPipelineInfo> subpipelines_;

  /// Scope used for tasks spawned by the inner operator implementation.
  ///
  /// This scope can be smaller than the `queue_` scope because the outer
  /// framing is even kept alive after the operator itself finished.
  AsyncScope* operator_scope_ = nullptr;

  QueueScope<variant<ExplicitAny, Option<AnyOperatorMsg>, Option<FromControl>,
                     SubPipelineEvent>>
    queue_;
  bool got_shutdown_request_ = false;
  bool upstream_done_ = false;
  bool is_done_ = false;
  // TODO: Expose this?
  std::atomic<size_t> ticks_ = 0;
};

namespace {

template <class Input, class Output>
auto run_operator(Box<Operator<Input, Output>> op,
                  Box<Pull<OperatorMsg<Input>>> pull_upstream,
                  Box<Push<OperatorMsg<Output>>> push_downstream,
                  Receiver<FromControl> from_control,
                  Sender<ToControl> to_control, OpId id, ExecCtx& exec_ctx,
                  caf::actor_system& sys, DiagHandler& dh,
                  std::shared_ptr<pipeline_metrics> metrics) -> Task<void> {
  co_await folly::coro::co_safe_point;
  co_await Runner{
    AnyOperator{std::move(op)},
    AnyOpPull{std::move(pull_upstream)},
    AnyOpPush{std::move(push_downstream)},
    std::move(from_control),
    std::move(to_control),
    std::move(id),
    exec_ctx,
    sys,
    dh,
    std::move(metrics),
  }
    .run_to_completion();
}

} // namespace

class ChainRunner {
public:
  ChainRunner(std::vector<AnyOperator> operators, AnyOpPull pull_upstream,
              AnyOpPush push_downstream, Receiver<FromControl> from_control,
              Sender<ToControl> to_control, PipeId id, ExecCtx& exec_ctx,
              caf::actor_system& sys, DiagHandler& dh,
              std::shared_ptr<pipeline_metrics> metrics)
    : operators_{std::move(operators)},
      pull_upstream_{std::move(pull_upstream)},
      push_downstream_{std::move(push_downstream)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      id_{std::move(id)},
      exec_ctx_{exec_ctx},
      sys_{sys},
      dh_{dh},
      metrics_{std::move(metrics)} {
  }

  auto run_to_completion() && -> Task<void> {
    auto guard = detail::scope_guard{[&] noexcept {
      LOGI("returning from chain runner {}", id_);
    }};
    LOGV("creating chain runner scope");
    co_await queue_.activate([&] -> Task<void> {
      LOGW("beginning chain setup of {}", id_);
      spawn_operators();
      LOGW("entering main loop of {}", id_);
      co_await run_until_shutdown();
      LOGW("cancelling queue of {}", id_);
      queue_.cancel();
    });
  }

private:
  auto spawn_operators() -> void {
    auto next_input = std::move(pull_upstream_);
    // TODO: Polish this.
    for (auto& op : operators_) {
      auto index = detail::narrow<size_t>(&op - operators_.data());
      co_match(op, [&]<class In, class Out>(Box<Operator<In, Out>>& op) {
        LOGI("got {}", typeid(*op).name());
        exec_ctx_.register_op_name(id_.op(index), typeid(*op));
        auto input = std::move(as<Box<Pull<OperatorMsg<In>>>>(next_input));
        auto last = index == operators_.size() - 1;
        auto output_sender = [&]() -> Box<Push<OperatorMsg<Out>>> {
          if (last) {
            return std::move(as<Box<Push<OperatorMsg<Out>>>>(push_downstream_));
          }
          auto [sender, receiver]
            = exec_ctx_.make<Out>(id_.op(index).to(id_.op(index + 1)));
          next_input = std::move(receiver);
          return std::move(sender);
        }();
        auto [from_control_sender, from_control_receiver]
          = bounded_channel<FromControl>(16);
        auto [to_control_sender, to_control_receiver]
          = bounded_channel<ToControl>(16);
        operator_ctrl_.push_back(std::move(from_control_sender));
        auto task = run_operator(std::move(op), std::move(input),
                                 std::move(output_sender),
                                 std::move(from_control_receiver),
                                 std::move(to_control_sender), id_.op(index),
                                 exec_ctx_, sys_, dh_, metrics_);
        auto executor = exec_ctx_.make_executor(id_.op(index));
        LOGI("spawning operator task");
        queue_.spawn([task = std::move(task), index,
                      executor = std::move(executor)] mutable
                       -> Task<std::pair<size_t, Terminated>> {
          co_await folly::coro::co_withExecutor(std::move(executor),
                                                std::move(task));
          LOGI("got termination from operator {}", index);
          co_return {index, Terminated{}};
        });
        LOGI("inserting control receiver task");
        queue_.spawn(
          [to_control_receiver = std::move(to_control_receiver), index] mutable
            -> AsyncGenerator<std::pair<size_t, Option<ToControl>>> {
            auto stop = false;
            while (not stop) {
              auto to_control = co_await to_control_receiver.recv();
              stop = to_control.is_none();
              co_yield {index, to_control};
            }
          });
        LOGI("done with operator");
      });
    }
  }

  auto run_until_shutdown() -> Task<void> {
    auto ready_for_shutdown = size_t{0};
    auto runners_left = operators_.size();
    auto controls_left = operators_.size();
    queue_.spawn(from_control_.recv());
    while (true) {
      if (runners_left == 0 and controls_left == 0) {
        break;
      }
      LOGW("waiting for next info in chain runner");
      auto next = co_await queue_.next();
      // We should never be done here...
      // TODO: Cancellation?
      TENZIR_ASSERT(next, "unexpected end of queue");
      co_await co_match(
        *next,
        [&](Option<FromControl> from_control) -> Task<void> {
          if (not from_control) {
            // Our controller should not die without cancelling us.
            TENZIR_UNREACHABLE();
          }
          co_await co_match(
            *from_control,
            [&](PostCommit) -> Task<void> {
              for (auto& ctrl : operator_ctrl_) {
                co_await ctrl.send(PostCommit{});
              }
            },
            [&](Shutdown) -> Task<void> {
              if (ready_for_shutdown == operators_.size()) {
                LOGW("got shutdown after request in {}", id_);
              } else {
                LOGW("got shutdown without request in {}", id_);
              }
              for (auto& sender : operator_ctrl_) {
                co_await sender.send(Shutdown{});
              }
            },
            [&](Stop) -> Task<void> {
              // TODO: Is this correct?
              for (auto& ctrl : operator_ctrl_) {
                co_await ctrl.send(Stop{});
              }
            });
        },
        [&](std::pair<size_t, variant<Terminated, Option<ToControl>>> next)
          -> Task<void> {
          auto [index, kind] = std::move(next);
          co_await co_match(
            kind,
            [&](Terminated) -> Task<void> {
              // TODO: What if we didn't send shutdown signal?
              TENZIR_ASSERT(runners_left > 0);
              runners_left -= 1;
              LOGW("got shutdown from {} ({} remaining)", id_.op(index),
                   operators_.size() - runners_left);
              co_return;
            },
            [&](Option<ToControl> to_control) -> Task<void> {
              if (not to_control) {
                TENZIR_ASSERT(controls_left > 0);
                controls_left -= 1;
                co_return;
              }
              LOGW("got control message from operator {}: {}", id_.op(index),
                   *to_control);
              switch (*to_control) {
                case ToControl::ready_for_shutdown:
                  TENZIR_ASSERT(ready_for_shutdown < operators_.size());
                  ready_for_shutdown += 1;
                  if (ready_for_shutdown == operators_.size()) {
                    // Once we are here, we got a request to shutdown from all
                    // operators. However, since we might be running in a
                    // subpipeline that is not ready to shutdown yet, we first
                    // have to ask control whether we are allowed to.
                    co_await to_control_.send(ToControl::ready_for_shutdown);
                  }
                  co_return;
                case ToControl::no_more_input:
                  // TODO: Inform the preceding operator that we don't need
                  // any more input.
                  if (index > 0) {
                    co_await operator_ctrl_[index - 1].send(Stop{});
                  } else {
                    // TODO: What if we don't host the preceding operator?
                    // Then we need to notify OUR input!
                    co_await to_control_.send(ToControl::no_more_input);
                  }
                  co_return;
                case ToControl::checkpoint_begin:
                case ToControl::checkpoint_done:
                  LOGI("chain got {} from operator {}", to_control,
                       id_.op(index));
                  co_return;
              }
              TENZIR_UNREACHABLE();
            });
          co_return;
        });
    }
    LOGW("left main loop of {}", id_);
    TENZIR_ASSERT(ready_for_shutdown == operators_.size());
    TENZIR_ASSERT(runners_left == 0);
    TENZIR_ASSERT(controls_left == 0);
  }

  std::vector<AnyOperator> operators_;
  AnyOpPull pull_upstream_;
  AnyOpPush push_downstream_;
  Receiver<FromControl> from_control_;
  Sender<ToControl> to_control_;
  PipeId id_;
  ExecCtx& exec_ctx_;
  caf::actor_system& sys_;
  DiagHandler& dh_;
  std::shared_ptr<pipeline_metrics> metrics_;

  std::vector<Sender<FromControl>> operator_ctrl_;

  QueueScope<variant<
    // Message from our controller.
    Option<FromControl>,
    // Message from one of the operators.
    std::pair<
      // Index of the operator where the message came from.
      size_t,
      // Message content.
      variant<
        // Signal that the operator task finished.
        Terminated,
        // Control message from one of the operators.
        Option<ToControl>>>>>
    queue_;
};

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> pull_upstream,
               Box<Push<OperatorMsg<Output>>> push_downstream,
               Receiver<FromControl> from_control, Sender<ToControl> to_control,
               PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys,
               DiagHandler& dh, std::shared_ptr<pipeline_metrics> metrics)
  -> Task<void> {
  co_await folly::coro::co_safe_point;
  co_await ChainRunner{
    std::move(chain).unwrap(),
    AnyOpPull{std::move(pull_upstream)},
    AnyOpPush{std::move(push_downstream)},
    std::move(from_control),
    std::move(to_control),
    std::move(id),
    exec_ctx,
    sys,
    dh,
    std::move(metrics),
  }
    .run_to_completion();
}

template auto
run_chain(OperatorChain<void, table_slice> chain,
          Box<Pull<OperatorMsg<void>>> pull_upstream,
          Box<Push<OperatorMsg<table_slice>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

template auto
run_chain(OperatorChain<chunk_ptr, table_slice> chain,
          Box<Pull<OperatorMsg<chunk_ptr>>> pull_upstream,
          Box<Push<OperatorMsg<table_slice>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

template auto
run_chain(OperatorChain<table_slice, table_slice> chain,
          Box<Pull<OperatorMsg<table_slice>>> pull_upstream,
          Box<Push<OperatorMsg<table_slice>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

/// Run a potentially-open pipeline without external control.
template <class Output>
  requires(not std::same_as<Output, void>)
auto run_open_pipeline(OperatorChain<void, Output> pipeline,
                       caf::actor_system& sys, DiagHandler& dh)
  -> AsyncGenerator<Output> {
  TENZIR_UNUSED(pipeline, sys, dh);
  TENZIR_TODO();
}

namespace {

auto new_pipe_id() -> PipeId {
  static auto next = std::atomic<size_t>{0};
  auto id = next.fetch_add(1, std::memory_order::relaxed);
  return PipeId{fmt::to_string(id)};
}

} // namespace

auto run_pipeline(OperatorChain<void, void> pipeline, ExecCtx& exec_ctx,
                  caf::actor_system& sys, DiagHandler& dh,
                  metrics_callback emit_fn) -> Task<void> {
  auto id = new_pipe_id();
  auto [push_input, pull_input]
    = exec_ctx.make<void>(ChannelId::first(id.op(0)));
  auto [push_output, pull_output]
    = exec_ctx.make<void>(ChannelId::last(id.op(pipeline.size() - 1)));
  auto metrics = std::make_shared<pipeline_metrics>();
  try {
    auto [from_control_sender, from_control_receiver]
      = bounded_channel<FromControl>(16);
    auto [to_control_sender, to_control_receiver]
      = bounded_channel<ToControl>(16);
    auto queue = QueueScope<
      variant<std::monostate, Option<ToControl>, Option<OperatorMsg<void>>>>{};
    LOGV("creating pipeline queue scope");
    co_await queue.activate([&] -> Task<void> {
      // Spawn periodic metrics emission if a callback was provided.
      if (emit_fn) {
        queue.scope().spawn([metrics, emit_fn] -> Task<void> {
          while (true) {
            co_await folly::coro::sleep(
              std::chrono::duration_cast<folly::HighResDuration>(
                defaults::metrics_interval));
            auto snapshot = metrics->take_snapshot();
            emit_fn(snapshot);
          }
        });
      }
      queue.spawn([&] -> Task<std::monostate> {
        co_await run_chain(std::move(pipeline), std::move(pull_input),
                           std::move(push_output),
                           std::move(from_control_receiver),
                           std::move(to_control_sender), id, exec_ctx, sys, dh,
                           std::move(metrics));
        co_return std::monostate{};
      });
      // TODO: We just have this right now to simulate checkpointing.
#if 0
      queue.spawn([&] -> Task<std::monostate> {
        while (true) {
          auto checkpoint = Checkpoint{uuid::random()};
          LOGI("injecting checkpoint {} into pipeline", checkpoint.id);
          co_await push_input(std::move(checkpoint));
          co_await folly::coro::sleep(std::chrono::seconds{1});
        }
      });
#endif
      queue.spawn(pull_output());
      queue.spawn(to_control_receiver.recv());
      auto chain_running = true;
      auto control_open = true;
      while (chain_running or control_open) {
        auto next = co_await queue.next();
        TENZIR_ASSERT(next);
        co_await co_match(
          std::move(*next),
          [&](std::monostate) -> Task<void> {
            // TODO: The pipeline terminated?
            LOGI("run_pipeline got info that chain terminated");
            chain_running = false;
            co_return;
          },
          [&](Option<ToControl> to_control) -> Task<void> {
            if (not to_control) {
              LOGI("outermost control channel was closed");
              control_open = false;
              co_return;
            }
            // TODO
            TENZIR_ASSERT(to_control == ToControl::ready_for_shutdown);
            LOGI("got shutdown request from outermost subpipeline");
            co_await from_control_sender.send(Shutdown{});
            // Close the input channel so that operator 0 observes None from
            // its upstream pull, enabling orderly sequential shutdown.
            {
              auto _ = std::move(push_input);
            }
            queue.spawn(to_control_receiver.recv());
          },
          [&](Option<OperatorMsg<void>> msg) -> Task<void> {
            if (not msg) {
              co_return;
            }
            co_match(*msg, [&](Signal signal) {
              co_match(
                signal,
                [&](EndOfData) {
                  LOGI("end of data is leaving pipeline");
                },
                [&](Checkpoint checkpoint) {
                  LOGI("checkpoint {} is leaving pipeline", checkpoint.id);
                });
            });
            queue.spawn(pull_output());
            co_return;
          });
      }
      queue.cancel();
    });
  } catch (folly::OperationCancelled) {
    // TODO: ?
    throw;
  } catch (panic_exception& e) {
    dh.emit(to_diagnostic(e));
    // TODO: Return failure?
    co_return;
  } catch (std::exception& e) {
    diagnostic::error("uncaught exception in pipeline: {}", e.what()).emit(dh);
    // TODO: Return failure?
    co_return;
  } catch (...) {
    diagnostic::error("uncaught exception in pipeline").emit(dh);
    // TODO: Return failure?
    co_return;
  }
}

} // namespace tenzir
