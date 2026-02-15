//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include "tenzir/async/queue_scope.hpp"
#include "tenzir/async/unbounded_queue.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/connect_to_node.hpp"
#include "tenzir/connector.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/tql2/eval.hpp"

#include <boost/unordered/unordered_flat_map.hpp>
#include <caf/actor_registry.hpp>
#include <folly/Executor.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

// TODO: Why does this not report line numbers correctly?
#undef TENZIR_UNREACHABLE
#define TENZIR_UNREACHABLE()                                                   \
  TENZIR_ERROR("unreachable");                                                 \
  tenzir::panic("unreachable")

namespace tenzir {

class Pass final : public Operator<table_slice, table_slice> {
public:
  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    co_await push(std::move(input));
  }
};

auto filter2(const table_slice& slice, const ast::expression& expr,
             diagnostic_handler& dh, bool warn) -> std::vector<table_slice> {
  auto results = std::vector<table_slice>{};
  auto offset = int64_t{0};
  for (auto& filter : eval(expr, slice, dh)) {
    auto array = try_as<arrow::BooleanArray>(&*filter.array);
    if (not array) {
      diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
        .primary(expr)
        .emit(dh);
      offset += filter.array->length();
      continue;
    }
    if (array->true_count() == array->length()) {
      results.push_back(subslice(slice, offset, offset + array->length()));
      offset += array->length();
      continue;
    }
    if (warn) {
      diagnostic::warning("assertion failure").primary(expr).emit(dh);
    }
    auto length = array->length();
    auto current_value = array->Value(0);
    auto current_begin = int64_t{0};
    // We add an artificial `false` at index `length` to flush.
    for (auto i = int64_t{1}; i < length + 1; ++i) {
      const auto next = i != length && array->IsValid(i) && array->Value(i);
      if (current_value == next) {
        continue;
      }
      if (current_value) {
        results.push_back(subslice(slice, offset + current_begin, offset + i));
      }
      current_value = next;
      current_begin = i;
    }
    offset += length;
  }
  return results;
}

template <class T>
class Receiver {
public:
  explicit Receiver(std::shared_ptr<UnboundedQueue<T>> queue)
    : queue_{std::move(queue)} {
    TENZIR_ASSERT(queue_);
  }

  auto receive() -> Task<T> {
    auto guard = detail::scope_guard{[] noexcept {
      LOGD("CANCELLED");
    }};
    folly::CancellationToken token
      = co_await folly::coro::co_current_cancellation_token;
    LOGV("waiting for queue in receiver ({}): {}", fmt::ptr(queue_.get()),
         token.isCancellationRequested());
    TENZIR_ASSERT(queue_);
    auto result = co_await queue_->dequeue();
    LOGW("got item for queue in receiver");
    guard.disable();
    co_return result;
  }

  auto into_generator() && -> AsyncGenerator<T> {
    return folly::coro::co_invoke(
      [self = std::move(*this)] mutable -> AsyncGenerator<T> {
        LOGV("starting receiver generator");
        while (true) {
          auto result = co_await self.receive();
          LOGV("got item in receiver generator");
          co_yield std::move(result);
          LOGV("continuing in result generator");
        }
      });
  }

private:
  std::shared_ptr<UnboundedQueue<T>> queue_;
};

template <class T>
class Sender {
public:
  explicit Sender(std::shared_ptr<UnboundedQueue<T>> queue)
    : queue_{std::move(queue)} {
  }

  auto send(T x) -> void {
    TENZIR_ASSERT(queue_);
    queue_->enqueue(std::move(x));
  }

private:
  std::shared_ptr<UnboundedQueue<T>> queue_;
};

template <class T>
auto make_unbounded_channel() -> std::pair<Sender<T>, Receiver<T>> {
  auto shared = std::make_shared<UnboundedQueue<T>>();
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
    return co_match(*this,
                    [&]<class Input>(SubPipeline<Input>& self) -> Task<void> {
                      return self.push(std::move(signal));
                    });
  }

  auto send(FromControl from_control) -> void {
    co_match(*this, [&]<class Input>(SubPipeline<Input>& self) {
      self.from_control.send(std::move(from_control));
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
};

struct SubPipelineFinished {
  data key;
};

class MutexDiagnosticHandler final : public diagnostic_handler {
public:
  MutexDiagnosticHandler(diagnostic_handler& dh) : dh_{dh} {
  }

  auto emit(diagnostic diag) -> void override {
    auto lock = std::scoped_lock{mut_};
    dh_.emit(std::move(diag));
  }

private:
  diagnostic_handler& dh_;
  std::mutex mut_;
};

template <class Input, class Output>
class Runner final : public OpCtx {
  class DiagnosticHandler : public diagnostic_handler {
  public:
    DiagnosticHandler(diagnostic_handler& dh) : dh_{dh} {
    }
    auto emit(diagnostic diag) -> void override {
      dh_.emit(censor_.censor(diag));
    }

  private:
    friend class Runner;
    diagnostic_handler& dh_;
    secret_censor censor_;
  };

public:
  Runner(Box<Operator<Input, Output>> op,
         Box<Pull<OperatorMsg<Input>>> pull_upstream,
         Box<Push<OperatorMsg<Output>>> push_downstream,
         Receiver<FromControl> from_control, Sender<ToControl> to_control,
         OpId id, ChannelFactory& channel_factory, caf::actor_system& sys,
         diagnostic_handler& dh)
    : op_{std::move(op)},
      pull_upstream_{std::move(pull_upstream)},
      push_downstream_{std::move(push_downstream)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      id_{std::move(id)},
      channel_factory_{channel_factory},
      dh_{dh},
      sys_{sys} {
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

  auto fetch_node() -> Task<failure_or<node_actor>> override {
    // Fast path: check local registry for existing node.
    if (auto node = sys_.registry().template get<node_actor>("tenzir.node")) {
      co_return node;
    }
    static auto mut = RawMutex{};
    const auto lock = co_await mut.lock();
    if (auto node = sys_.registry().template get<node_actor>("tenzir.node")) {
      co_return node;
    }
    // Get configuration.
    const auto& opts = content(sys_.config());
    const auto node_endpoint = detail::get_node_endpoint(opts);
    if (not node_endpoint) {
      diagnostic::error("failed to get node endpoint: {}",
                        node_endpoint.error())
        .emit(dh_);
      co_return failure::promise();
    }
    const auto timeout = detail::node_connection_timeout(opts);
    const auto retry_delay = detail::get_retry_delay(opts);
    const auto deadline = detail::get_deadline(timeout);
    // Spawn connector and request connection.
    auto connector_actor = sys_.spawn(connector, retry_delay, deadline, false);
    auto request = connect_request{
      .port = node_endpoint->port->number(),
      .host = node_endpoint->host,
    };
    auto result = co_await async_mail(atom::connect_v, std::move(request))
                    .request(connector_actor);
    caf::anon_send_exit(connector_actor, caf::exit_reason::user_shutdown);
    if (not result) {
      diagnostic::error("failed to connect to node: {}", result.error())
        .emit(dh_);
      co_return failure::promise();
    }
    // Put the actor into the local registry for process-wide fast pathing.
    sys_.registry().put("tenzir.node", *result);
    co_return std::move(*result);
  }

  auto dh() -> diagnostic_handler& override {
    return dh_;
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
          &= static_cast<bool>(f.finish(requested_secrets, dh_.censor_, dh_));
      }
      if (not success) {
        co_return failure::promise();
      }
      co_return {};
    }
    auto node = co_await fetch_node();
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
      futures.push_back(mail(atom::resolve_v, name, public_key).request(*node));
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
        &= static_cast<bool>(f.finish(requested_secrets, dh_.censor_, dh_));
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
    // TODO: Run chain in async scope?
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
      = channel_factory_.make<table_slice>(id_.to(sub_id.op(0)));
    auto [from_control_sender, from_control_receiver]
      = make_unbounded_channel<FromControl>();
    auto [to_control_sender, to_control_receiver]
      = make_unbounded_channel<ToControl>();
    auto push_upstream = co_match(
      std::move(chain),
      [&]<class In>(OperatorChain<In, table_slice> chain)
        -> variant<Box<Push<OperatorMsg<void>>>,
                   Box<Push<OperatorMsg<table_slice>>>,
                   Box<Push<OperatorMsg<chunk_ptr>>>> {
        auto [push_upstream, pull_upstream]
          = channel_factory_.make<In>(sub_id.op(chain.size()).to(id_));
        auto runner = run_chain(std::move(chain), std::move(pull_upstream),
                                std::move(push_downstream),
                                std::move(from_control_receiver),
                                std::move(to_control_sender), std::move(sub_id),
                                channel_factory_, sys_, dh_);
        // TODO: What do we do here? Do we need to react to it
        // finishing?
        auto handle = queue_.scope().spawn(std::move(runner));
        return std::move(push_upstream);
      });
    // co_await handle.join();
    queue_.scope().spawn(
      [this, key,
       pull_downstream = std::move(pull_downstream)] mutable -> Task<void> {
        // Pulling from the subpipeline needs to happen independently from the
        // main operator logic. This is because we might block when pushing to
        // the subpipeline due to backpressure, but in order to alleviate the
        // backpressure, we need to pull from it.
        while (true) {
          auto output = co_await pull_downstream();
          co_await co_match(
            std::move(output),
            [&](table_slice output) -> Task<void> {
              if constexpr (std::same_as<Output, void>) {
                co_await op_->process_sub(make_view(key), std::move(output),
                                          *this);
              } else {
                auto push = OpPushWrapper{push_downstream_};
                co_await op_->process_sub(make_view(key), std::move(output),
                                          push, *this);
              }
            },
            [&](Signal signal) -> Task<void> {
              co_await co_match(
                signal,
                [&](EndOfData) -> Task<void> {
                  queue_.spawn([key] -> Task<SubPipelineFinished> {
                    co_return SubPipelineFinished{key};
                  });
                  co_return;
                },
                [&](Checkpoint) -> Task<void> {
                  // TODO: Handle checkpoint.
                  co_return;
                });
            });
        }
      });
    queue_.scope().spawn([to_control_receiver = std::move(
                            to_control_receiver)] mutable -> Task<void> {
      while (true) {
        auto to_control = co_await to_control_receiver.receive();
        switch (to_control) {
          case ToControl::no_more_input:
            // FIXME: How do we inform the operator that the subpipeline
            // doesn't want anymore input?
            co_return;
          case ToControl::ready_for_shutdown:
            // FIXME: Send shutdown signal and then remove it from the
            // pipelines list once the task terminated.
          case ToControl::checkpoint_begin:
          case ToControl::checkpoint_done:
            TENZIR_TODO();
        }
        TENZIR_UNREACHABLE();
      }
    });
    // TODO: Cleanup.
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

  auto spawn_task(Task<void> task) -> AsyncHandle<void> override {
    TENZIR_ASSERT(operator_scope_);
    return operator_scope_->spawn(std::move(task));
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
      if constexpr (std::same_as<Output, void>) {
        co_await op_->start(*this);
      } else {
        co_await op_->start(*this);
      }
      LOGI("-> post start");
      queue_.spawn([this] -> Task<ExplicitAny> {
        co_return ExplicitAny{co_await op_->await_task(*this)};
      });
      queue_.spawn(pull_upstream_());
      queue_.spawn(from_control_.receive());
      while (not got_shutdown_request_) {
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
    LOGI("tick {} in {} ({})", ticks_, id_, typeid(*op_).name());
    switch (op_->state()) {
      case OperatorState::done:
        co_await handle_done();
        break;
      case OperatorState::unspecified:
        break;
    }
    LOGV("waiting in {} for message", typeid(*op_).name());
    auto message = check(co_await queue_.next());
    co_await co_match(std::move(message), [&](auto message) {
      return process(std::move(message));
    });
  }

  auto process(ExplicitAny message) -> Task<void> {
    // The task provided by the inner implementation completed.
    LOGV("got future result in {}", typeid(*op_).name());
    if constexpr (std::same_as<Output, void>) {
      co_await op_->process_task(std::move(message.value), *this);
    } else {
      auto push = OpPushWrapper{push_downstream_};
      co_await op_->process_task(std::move(message.value), push, *this);
    }
    if (op_->state() == OperatorState::done) {
      co_await handle_done();
    } else {
      queue_.spawn([this] -> Task<ExplicitAny> {
        co_return ExplicitAny{co_await op_->await_task(*this)};
      });
    }
    LOGV("handled future result in {}", typeid(*op_).name());
  }

  auto process(OperatorMsg<Input> message) -> Task<void> {
    co_await co_match(
      std::move(message),
      // The template indirection is necessary to prevent a `void` parameter.
      [&]<std::same_as<Input> Input2>(Input2 input) -> Task<void> {
        if constexpr (std::same_as<Input, void>) {
          TENZIR_UNREACHABLE();
        } else {
          LOGV("got input in {}", typeid(*op_).name());
          if (is_done_) {
            // No need to forward the input.
            co_return;
          }
          if constexpr (std::same_as<Output, void>) {
            co_await op_->process(input, *this);
          } else {
            auto push = OpPushWrapper{push_downstream_};
            co_await op_->process(input, push, *this);
          }
        }
      },
      [&](Signal signal) -> Task<void> {
        co_await co_match(
          signal,
          [&](EndOfData) -> Task<void> {
            LOGV("got end of data in {}", typeid(*op_).name());
            if constexpr (std::same_as<Input, void>) {
              TENZIR_UNREACHABLE();
            } else {
              // TODO: The default behavior is to transition to done?
              co_await handle_done();
            }
            co_return;
          },
          [&](Checkpoint checkpoint) -> Task<void> {
            co_await begin_checkpoint(checkpoint);
            co_return;
          });
      });
    queue_.spawn(pull_upstream_());
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
    LOGI("got checkpoint {} in {}", checkpoint.id, typeid(*op_).name());
    to_control_.send(ToControl::checkpoint_begin);
    co_await op_->checkpoint(*this);
    // TODO: Also checkpoint `subpipelines_`. What else?
    if (subpipelines_.empty()) {
      LOGI("finishing checkpoint with no subpipelines in {} ",
           typeid(*op_).name());
      to_control_.send(ToControl::checkpoint_done);
      co_await push_downstream_(std::move(checkpoint));
      co_return;
    }
    LOGI("checkpointing {} subpipelines in {}", subpipelines_.size(),
         typeid(*op_).name());
    for (auto& [_, sub] : subpipelines_) {
      co_await sub.handle.send(checkpoint);
      sub.wants_commit = true;
    }
    // TODO: Continue here?
  }

  auto process(FromControl message) -> Task<void> {
    co_await co_match(
      std::move(message),
      [&](PostCommit) -> Task<void> {
        LOGV("got post commit in {}", typeid(*op_).name());
        co_await op_->post_commit();
      },
      [&](Shutdown) -> Task<void> {
        // FIXME: Cleanup on shutdown?
        LOGV("got shutdown in {}", typeid(*op_).name());
        got_shutdown_request_ = true;
        // TODO: Should we really just forward this here?
        for (auto& [_, sub] : subpipelines_) {
          sub.handle.send(Shutdown{});
        }
        co_return;
      },
      [&](Stop) -> Task<void> {
        co_await handle_done();
      });
    queue_.spawn(from_control_.receive());
  }

  auto process(SubPipelineFinished message) -> Task<void> {
    if constexpr (std::same_as<Output, void>) {
      co_await op_->finish_sub(make_view(message.key), *this);
    } else {
      auto push = OpPushWrapper{push_downstream_};
      co_await op_->finish_sub(make_view(message.key), push, *this);
    }
    subpipelines_.erase(message.key);
    co_await try_finish_done();
  }

  auto handle_done() -> Task<void> {
    // We want to run this code once.
    if (is_done_) {
      co_return;
    }
    LOGV("running done in {}", typeid(*op_).name());
    is_done_ = true;
    // Immediately inform control that we want no more data.
    if constexpr (not std::same_as<Input, void>) {
      to_control_.send(ToControl::no_more_input);
    }
    // Then finalize the operator, which can still produce output.
    if constexpr (std::same_as<Output, void>) {
      co_await op_->finalize(*this);
    } else {
      auto push = OpPushWrapper{push_downstream_};
      co_await op_->finalize(push, *this);
    }
    // Tell all subpipelines to shut down. Note that the previous step could
    // have still pushed data into them. The main loop continues running to
    // drain remaining subpipeline output and collect SubPipelineFinished
    // messages. `try_finish_done()` completes the shutdown once all are gone.
    for (auto& [key, sub] : subpipelines_) {
      co_await co_match(sub.handle,
                        []<class In>(SubPipeline<In>& sub) -> Task<void> {
                          // TODO: What if this is a source?
                          if constexpr (not std::same_as<In, void>) {
                            co_await sub.push(EndOfData{});
                          }
                        });
    }
    co_await try_finish_done();
  }

  auto try_finish_done() -> Task<void> {
    if (not is_done_ or not subpipelines_.empty()) {
      co_return;
    }
    if constexpr (not std::same_as<Output, void>) {
      co_await push_downstream_(EndOfData{});
    }
    LOGW("sending ready to shutdown from {}", id_);
    to_control_.send(ToControl::ready_for_shutdown);
  }

  Box<Operator<Input, Output>> op_;
  Box<Pull<OperatorMsg<Input>>> pull_upstream_;
  Box<Push<OperatorMsg<Output>>> push_downstream_;
  Receiver<FromControl> from_control_;
  Sender<ToControl> to_control_;
  OpId id_;
  ChannelFactory& channel_factory_;
  DiagnosticHandler dh_;
  caf::actor_system& sys_;
  std::shared_ptr<const registry> reg_ = global_registry();

  size_t next_subpipeline_id_ = 0;

  // TODO: Better map type.
  std::unordered_map<data, SubPipelineInfo> subpipelines_;

  /// Scope used for tasks spawned by the inner operator implementation.
  ///
  /// This scope can be smaller than the `queue_` scope because the outer
  /// framing is even kept alive after the operator itself finished.
  AsyncScope* operator_scope_ = nullptr;

  QueueScope<
    variant<ExplicitAny, OperatorMsg<Input>, FromControl, SubPipelineFinished>>
    queue_;
  bool got_shutdown_request_ = false;
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
                  Sender<ToControl> to_control, OpId id,
                  ChannelFactory& channel_factory, caf::actor_system& sys,
                  diagnostic_handler& dh) -> Task<void> {
  co_await folly::coro::co_safe_point;
  co_await Runner<Input, Output>{
    std::move(op),
    std::move(pull_upstream),
    std::move(push_downstream),
    std::move(from_control),
    std::move(to_control),
    std::move(id),
    channel_factory,
    sys,
    dh,
  }
    .run_to_completion();
}

} // namespace

template <class Input, class Output>
class ChainRunner {
public:
  ChainRunner(OperatorChain<Input, Output> chain,
              Box<Pull<OperatorMsg<Input>>> pull_upstream,
              Box<Push<OperatorMsg<Output>>> push_downstream,
              Receiver<FromControl> from_control, Sender<ToControl> to_control,
              PipeId id, ChannelFactory& channel_factory,
              caf::actor_system& sys, MutexDiagnosticHandler& dh)
    : operators_{std::move(chain).unwrap()},
      pull_upstream_{std::move(pull_upstream)},
      push_downstream_{std::move(push_downstream)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      id_{std::move(id)},
      channel_factory_{channel_factory},
      sys_{sys},
      dh_{dh} {
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
    auto next_input
      = variant<Box<Pull<OperatorMsg<void>>>, Box<Pull<OperatorMsg<chunk_ptr>>>,
                Box<Pull<OperatorMsg<table_slice>>>>{std::move(pull_upstream_)};
    // TODO: Polish this.
    for (auto& op : operators_) {
      auto index = detail::narrow<size_t>(&op - operators_.data());
      co_match(op, [&]<class In, class Out>(Box<Operator<In, Out>>& op) {
        LOGI("got {}", typeid(*op).name());
        auto input = as<Box<Pull<OperatorMsg<In>>>>(std::move(next_input));
        auto [output_sender, output_receiver]
          = channel_factory_.make<Out>(id_.op(index).to(id_.op(index + 1)));
        // TODO: This is a horrible hack.
        auto last = index == operators_.size() - 1;
        if (last) {
          if constexpr (std::same_as<Out, Output>) {
            output_sender = std::move(push_downstream_);
          } else {
            TENZIR_UNREACHABLE();
          }
        }
        auto [from_control_sender, from_control_receiver]
          = make_unbounded_channel<FromControl>();
        auto [to_control_sender, to_control_receiver]
          = make_unbounded_channel<ToControl>();
        operator_ctrl_.push_back(std::move(from_control_sender));
        next_input = std::move(output_receiver);
        auto task = run_operator(std::move(op), std::move(input),
                                 std::move(output_sender),
                                 std::move(from_control_receiver),
                                 std::move(to_control_sender), id_.op(index),
                                 channel_factory_, sys_, dh_);
        LOGI("spawning operator task");
        queue_.spawn([task = std::move(task),
                      index] mutable -> Task<std::pair<size_t, Shutdown>> {
          co_await std::move(task);
          LOGI("got termination from operator {}", index);
          co_return {index, Shutdown{}};
        });
        LOGI("inserting control receiver task");
        queue_.spawn(
          [to_control_receiver = std::move(to_control_receiver), index] mutable
            -> folly::coro::AsyncGenerator<std::pair<size_t, ToControl>> {
            while (true) {
              co_yield {index, co_await to_control_receiver.receive()};
            }
          });
        LOGI("done with operator");
      });
    }
  }

  auto run_until_shutdown() -> Task<void> {
    // TODO: Or do we want to continue listening for control responses during
    // shutdown? That would require some additional coordination.
    auto ready_for_shutdown = size_t{0};
    auto shutdown_count = size_t{0};
    queue_.spawn(from_control_.receive());
    auto keep_running = true;
    while (keep_running) {
      LOGW("waiting for next info in chain runner");
      auto next = co_await queue_.next();
      // We should never be done here...
      // TODO: Cancellation?
      TENZIR_ASSERT(next, "unexpected end of queue");
      co_match(
        *next,
        [&](FromControl from_control) {
          co_match(
            from_control,
            [&](PostCommit) {
              for (auto& ctrl : operator_ctrl_) {
                ctrl.send(PostCommit{});
              }
            },
            [&](Shutdown) {
              if (ready_for_shutdown == operators_.size()) {
                LOGW("got shutdown after request in {}", id_);
              } else {
                LOGW("got shutdown without request in {}", id_);
              }
              for (auto& sender : operator_ctrl_) {
                sender.send(Shutdown{});
              }
            },
            [&](Stop) {
              // TODO: Is this correct?
              for (auto& ctrl : operator_ctrl_) {
                ctrl.send(Stop{});
              }
            });
        },
        [&](std::pair<size_t, variant<Shutdown, ToControl>> next) {
          auto [index, kind] = std::move(next);
          co_match(
            kind,
            [&](Shutdown) {
              // TODO: What if we didn't send shutdown signal?
              TENZIR_ASSERT(shutdown_count < operators_.size());
              shutdown_count += 1;
              LOGW("got shutdown from {} ({} remaining)", id_.op(index),
                   operators_.size() - shutdown_count);
              if (shutdown_count == operators_.size()) {
                // All operators shut down successfully.
                keep_running = false;
              }
            },
            [&](ToControl to_control) {
              LOGW("got control message from operator {}: {}", id_.op(index),
                   to_control);
              switch (to_control) {
                case ToControl::ready_for_shutdown:
                  TENZIR_ASSERT(ready_for_shutdown < operators_.size());
                  ready_for_shutdown += 1;
                  if (ready_for_shutdown == operators_.size()) {
                    // Once we are here, we got a request to shutdown from all
                    // operators. However, since we might be running in a
                    // subpipeline that is not ready to shutdown yet, we first
                    // have to ask control whether we are allowed to.
                    to_control_.send(ToControl::ready_for_shutdown);
                  }
                  return;
                case ToControl::no_more_input:
                  // TODO: Inform the preceding operator that we don't need
                  // any more input.
                  if (index > 0) {
                    operator_ctrl_[index - 1].send(Stop{});
                  } else {
                    // TODO: What if we don't host the preceding operator?
                    // Then we need to notify OUR input!
                    to_control_.send(ToControl::no_more_input);
                  }
                  return;
                case ToControl::checkpoint_begin:
                case ToControl::checkpoint_done:
                  LOGI("chain got {} from operator {}", to_control,
                       id_.op(index));
                  return;
              }
              TENZIR_UNREACHABLE();
            });
        });
    }
    LOGW("left main loop of {}", id_);
    TENZIR_ASSERT(ready_for_shutdown == operators_.size());
    TENZIR_ASSERT(shutdown_count == operators_.size());
  }

  std::vector<AnyOperator> operators_;
  Box<Pull<OperatorMsg<Input>>> pull_upstream_;
  Box<Push<OperatorMsg<Output>>> push_downstream_;
  Receiver<FromControl> from_control_;
  Sender<ToControl> to_control_;
  PipeId id_;
  ChannelFactory& channel_factory_;
  caf::actor_system& sys_;
  MutexDiagnosticHandler& dh_;

  std::vector<Sender<FromControl>> operator_ctrl_;

  QueueScope<variant<
    // Message from our controller.
    FromControl,
    // Message from one of the operators.
    std::pair<
      // Index of the operator where the message came from.
      size_t,
      // Message content.
      variant<
        // Signal that the operator task finished.
        Shutdown,
        // Control message from one of the operators.
        ToControl>>>>
    queue_;
};

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> pull_upstream,
               Box<Push<OperatorMsg<Output>>> push_downstream,
               Receiver<FromControl> from_control, Sender<ToControl> to_control,
               PipeId id, ChannelFactory& channel_factory,
               caf::actor_system& sys, diagnostic_handler& dh) -> Task<void> {
  auto mdh = MutexDiagnosticHandler{dh};
  co_await folly::coro::co_safe_point;
  co_await ChainRunner{
    std::move(chain),
    std::move(pull_upstream),
    std::move(push_downstream),
    std::move(from_control),
    std::move(to_control),
    std::move(id),
    channel_factory,
    sys,
    mdh,
  }
    .run_to_completion();
}

/// Run a potentially-open pipeline without external control.
template <class Output>
  requires(not std::same_as<Output, void>)
auto run_open_pipeline(OperatorChain<void, Output> pipeline,
                       caf::actor_system& sys, diagnostic_handler& dh)
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

auto run_pipeline(OperatorChain<void, void> pipeline,
                  ChannelFactory& channel_factory, caf::actor_system& sys,
                  diagnostic_handler& dh) -> Task<void> {
  auto id = new_pipe_id();
  auto [push_input, pull_input]
    = channel_factory.make<void>(ChannelId::first(id.op(0)));
  auto [push_output, pull_output]
    = channel_factory.make<void>(ChannelId::last(id.op(pipeline.size() + 1)));
  try {
    auto [from_control_sender, from_control_receiver]
      = make_unbounded_channel<FromControl>();
    auto [to_control_sender, to_control_receiver]
      = make_unbounded_channel<ToControl>();
    auto queue
      = QueueScope<variant<std::monostate, ToControl, OperatorMsg<void>>>{};
    LOGV("creating pipeline queue scope");
    co_await queue.activate([&] -> Task<void> {
      queue.spawn([&] -> Task<std::monostate> {
        co_await run_chain(std::move(pipeline), std::move(pull_input),
                           std::move(push_output),
                           std::move(from_control_receiver),
                           std::move(to_control_sender), id, channel_factory,
                           sys, dh);
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
      queue.spawn(to_control_receiver.receive());
      auto is_running = true;
      while (is_running) {
        auto next = co_await queue.next();
        TENZIR_ASSERT(next);
        co_match(
          std::move(*next),
          [&](std::monostate) {
            // TODO: The pipeline terminated?
            LOGI("run_pipeline got info that chain terminated");
            is_running = false;
          },
          [&](ToControl to_control) {
            // TODO
            TENZIR_ASSERT(to_control == ToControl::ready_for_shutdown);
            LOGI("got shutdown request from outermost subpipeline");
            from_control_sender.send(Shutdown{});
            queue.spawn(to_control_receiver.receive());
          },
          [&](OperatorMsg<void> msg) {
            co_match(msg, [&](Signal signal) {
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
