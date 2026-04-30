//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/executor.hpp"

#include "tenzir/async/fetch_node.hpp"
#include "tenzir/async/join_set.hpp"
#include "tenzir/async/log.hpp"
#include "tenzir/async/mail.hpp"
#include "tenzir/async/select_set.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <folly/Demangle.h>
#include <folly/coro/BoundedQueue.h>

// TODO: Why does this not report line numbers correctly?
#undef TENZIR_UNREACHABLE
#define TENZIR_UNREACHABLE()                                                   \
  TENZIR_ERROR("unreachable");                                                 \
  tenzir::panic("unreachable")

namespace tenzir {

// Forward declaration to avoid including registry.hpp.
auto global_registry() -> std::shared_ptr<const registry>;

namespace {

auto demangle_op_type(std::type_info const& type) -> std::string;

} // namespace

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

/// A type-erased stream message: either data or a signal.
struct AnyOperatorMsg : variant<table_slice, chunk_ptr, Signal> {
  using variant::variant;

  template <class T>
  explicit(false) AnyOperatorMsg(OperatorMsg<T> msg)
    : variant{co_match(std::move(msg), [](auto value) -> AnyOperatorMsg {
        return value;
      })} {
  }
};

/// Type-erased pull.
using AnyOpPull
  = variant<Box<Pull<OperatorMsg<void>>>, Box<Pull<OperatorMsg<chunk_ptr>>>,
            Box<Pull<OperatorMsg<table_slice>>>>;

/// Type-erased push.
using AnyOpPush
  = variant<Box<Push<OperatorMsg<void>>>, Box<Push<OperatorMsg<chunk_ptr>>>,
            Box<Push<OperatorMsg<table_slice>>>>;

// Wraps an `Any` but without the implicit construction from values.
struct ExplicitAny {
  explicit ExplicitAny(Any value) : value{std::move(value)} {
  }

  Any value;
};

struct Terminated {};

/// An message transported from a subpipeline to the parent pipeline.
///
/// Since this goes over a back-edge, we need to replay it when restoring.
struct SubMessage {
  using Event = variant<Option<Checkpoint>, Option<ToControl>>;

  SubKey key;
  Event event;
};

struct SubPipeline {
  SubPipeline(AnyOpPush push, Receiver<Checkpoint> from_sub,
              Sender<FromControl> from_control_sender,
              Receiver<ToControl> to_control_receiver, element_type_tag input)
    : push{Option<AnyOpPush>{std::move(push)}},
      from_sub{std::move(from_sub)},
      from_control_sender{
        Option<Sender<FromControl>>{std::move(from_control_sender)}},
      to_control_receiver{std::move(to_control_receiver)},
      input{input},
      handle{match(input, [this]<class Input>(tag<Input>) -> AnySubHandle {
        // TODO: There surely is a better way to model this than passing `this`.
        return AnySubHandle{std::in_place_type<SubHandle<Input>>, *this};
      })} {
  }

  auto send(Signal signal) -> Task<void> {
    TENZIR_ASSERT(push);
    co_return co_await co_match(
      *push, [&]<class In>(Box<Push<OperatorMsg<In>>>& push) {
        return push(std::move(signal));
      });
  }

  /// We keep the push handle until we begin its shutdown sequence.
  Option<AnyOpPush> push;
  /// Don't send more data after `ToControl::no_more_input` or `close()`.
  bool closed_data = false;
  /// The channel from subpipeline to parent pipeline.
  Receiver<Checkpoint> from_sub;
  /// Channel to send control messages to the subpipeline chain.
  Option<Sender<FromControl>> from_control_sender;
  /// Channel to receive control messages from the subpipeline chain.
  Receiver<ToControl> to_control_receiver;
  /// Also need the input type it was spawned with to recreate it.
  element_type_tag input;
  /// True if this subpipeline received the last checkpoint, requiring a commit.
  bool wants_commit = false;
  /// Set when process(SubMessage) observes the from_sub channel drain.
  bool from_sub_done = false;
  /// Set when process(SubMessage) observes the to_control channel drain.
  bool to_control_done = false;
  /// Handle that is given out to the operator implementation.
  AnySubHandle handle;
};

template <class Input>
template <std::same_as<Input> In>
auto SubHandle<Input>::push(In input) -> Task<Result<void, In>> {
  if (self_.closed_data) {
    co_return Err{std::move(input)};
  }
  auto& push = as<Box<Push<OperatorMsg<Input>>>>(self_.push.unwrap());
  co_await push(std::move(input));
  co_return {};
}

template <class Input>
auto SubHandle<Input>::close() -> Task<void>
  requires(not std::same_as<Input, void>)
{
  if (self_.closed_data) {
    co_return;
  }
  auto& push = as<Box<Push<OperatorMsg<Input>>>>(self_.push.unwrap());
  co_await push(EndOfData{});
  self_.closed_data = true;
  co_return;
}

template class SubHandle<chunk_ptr>;
template class SubHandle<table_slice>;
// Explicit instantiation of member template `push` (not covered by template
// class).
template auto SubHandle<chunk_ptr>::push(chunk_ptr)
  -> Task<Result<void, chunk_ptr>>;
template auto SubHandle<table_slice>::push(table_slice)
  -> Task<Result<void, table_slice>>;

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

class CensoringDiagHandler final : public DiagHandler {
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

/// Decorator over any `ExecCtx` that produces fused channels and shares a
/// single executor across all operators. This gives run-to-completion
/// semantics per slice: a push blocks until the downstream operator has
/// finished processing the item and is ready for the next.
class FusedExecCtx final : public ExecCtx {
public:
  explicit FusedExecCtx(ExecCtx& inner) : inner_{inner} {
  }

  auto make_executor(OpId id, std::string name)
    -> folly::Executor::KeepAlive<> override {
    return inner_.make_executor(std::move(id), std::move(name));
  }

  auto make_io_executor(OpId id, std::string name)
    -> folly::Executor::KeepAlive<folly::IOExecutor> override {
    return inner_.make_io_executor(std::move(id), std::move(name));
  }

  auto metrics_receiver() const -> metrics_receiver_actor override {
    return inner_.metrics_receiver();
  }

  auto make_counter(MetricsLabel label, MetricsDirection direction,
                    MetricsVisibility visibility) -> MetricsCounter override {
    return inner_.make_counter(label, direction, visibility);
  }

  auto is_hidden() const -> bool override {
    return inner_.is_hidden();
  }

  auto has_terminal() const -> bool override {
    return inner_.has_terminal();
  }

protected:
  auto make_void(ChannelId id) -> PushPull<OperatorMsg<void>> override {
    return inner_.make_fused_channel<void>(std::move(id));
  }

  auto make_events(ChannelId id)
    -> PushPull<OperatorMsg<table_slice>> override {
    return inner_.make_fused_channel<table_slice>(std::move(id));
  }

  auto make_bytes(ChannelId id) -> PushPull<OperatorMsg<chunk_ptr>> override {
    return inner_.make_fused_channel<chunk_ptr>(std::move(id));
  }

  auto make_fused_void(ChannelId id) -> PushPull<OperatorMsg<void>> override {
    return inner_.make_fused_channel<void>(std::move(id));
  }

  auto make_fused_events(ChannelId id)
    -> PushPull<OperatorMsg<table_slice>> override {
    return inner_.make_fused_channel<table_slice>(std::move(id));
  }

  auto make_fused_bytes(ChannelId id)
    -> PushPull<OperatorMsg<chunk_ptr>> override {
    return inner_.make_fused_channel<chunk_ptr>(std::move(id));
  }

private:
  ExecCtx& inner_;
};

/// Run a pipeline with fused (run-to-completion) semantics.
///
/// Each input slice is fully processed through the entire operator chain
/// before the next input is pulled. Internally creates a `FusedExecCtx` that
/// replaces buffered channels with fused channels.
template <class Input, class Output>
auto run_chain_fused(OperatorChain<Input, Output> chain,
                     Box<Pull<OperatorMsg<Input>>> pull_upstream,
                     Box<Push<OperatorMsg<Output>>> push_downstream,
                     Receiver<FromControl> from_control,
                     Sender<ToControl> to_control, PipeId id, ExecCtx& exec_ctx,
                     caf::actor_system& sys, DiagHandler& dh) -> Task<void> {
  auto fused_ctx = FusedExecCtx{exec_ctx};
  co_await run_chain(std::move(chain), std::move(pull_upstream),
                     std::move(push_downstream), std::move(from_control),
                     std::move(to_control), std::move(id), fused_ctx, sys, dh);
}

/// Core execution logic for a single operator.
///
/// This is heavily inspired by "Asynchronous Barrier Snapshotting"[^1]. In the
/// following, we assume familiarity with the general paper.
//
/// The main difference is that we allow dynamic graphs as long as they are
/// tree-shaped, that is: Dynamically spawned parts of the graph must be owned
/// by a single operator. Those are our subpipelines. Since the graph is
/// dynamic, we do not use a count-based checkpoint completion detection ("all
/// 42 operators and 5 back-edges have snapshotted"), and instead rely on the
/// tree to propagate checkpoints. That is: We are done with checkpointing once
/// the final sink yields a checkpoint. Each operator is responsible to only
/// emit a checkpoint once all of its subpipelines are checkpointed.
///
/// Conceptually, the execution graph of a single operator looks like this:
///                 ┌────────────┐
///      Input      │            │     Output
/// ───────────────>│  Operator  ├───────────────>
///             ┌───┤            │<──┐
///             │   └────────────┘   │
///             │  ┌──────────────┐  │
///             ├─>│ Subpipeline 1├──┤
///             │  └──────────────┘  │
///             │  ┌──────────────┐  │
///             └─>│ Subpipeline 2├──┘
///                └──────────────┘
///
/// However, when it comes to reasoning about checkpointing, it is helpful to
/// think about it more like this:
///                 ┌────────────┐
///      Input      │            │   process()    ┌────────────┐     Output
/// ───────────────>│  Operator  ├───────────────>│  Combiner  ├───────────────>
///             ┌───┤            │<─┬─┐           └────∧─∧─────┘
///             │   └────────────┘  │ │ finish_sub()   │ │ process_sub()
///             │  ┌──────────────┐ │ │                │ │
///             ├─>│ Subpipeline 1├─┴──────────────────┘ │
///             │  └──────────────┘   │                  │
///             │  ┌──────────────┐   │                  │
///             └─>│ Subpipeline 2├───┴──────────────────┘
///                └──────────────┘
///
/// Here, `finish_sub()` is a back-edge. We will replay those calls as explained
/// in the paper. On the other hand, `process_sub()` is a forward-edge, which
/// doesn't need channel snapshotting. Thus we just forward data from
/// subpipelines. This is good because it means we don't have to capture any
/// actual in-flight data in the snapshot, which could be quite big and changes
/// all the time.
///
/// You could also make `process_sub()` a back-edge, which would be necessary to
/// make something like `while expr { … }` work. Recording incoming messages
/// after a snapshot might also be more efficient because you can then continue
/// processing other messages in the meantime. This could be considered a future
/// extension. Note that we can't do our trick above here then, since this
/// typically would lead to follow-up messages to the subpipelines, creating an
/// infinite cycle.
///
/// So checkpointing of subpipelines proceeds as follows:
/// 1) Block the input channel of the operator itself.
/// 2) Start recording future calls to `finish_sub()`.
/// 3) Perform the snapshot and submit it.
/// 4) Forward checkpoint messages to all subpipelines.
/// 5) Block the output of a subpipeline once it returns the checkpoint.
/// 6) Wait for all subpipelines to return their checkpoints.
/// 7) Wait for confirmation of our snapshot.
/// 8) Forward the checkpoint to the downstream operator.
/// 9) Unblock all channels and continue normal execution.
///
/// If there are no subpipelines, this becomes:
/// 1) Perform the snapshot and submit it.
/// 2) Wait for confirmation of our snapshot.
/// 3) Forward the checkpoint to the downstream operator.
///
/// While we are waiting for subpipeline checkpointing, we also don't process
/// results of `await_task()` with `process_task()`, as this runs in the main
/// loop and it could push into the subpipelines which would eventually block
/// because we stop reading from them after a checkpoint.
///
/// Besides subpipelines, another complication is supporting operators such as
/// `head` that terminate without consuming their entire input. In this case,
/// the `head` operator emits a `ToControl::no_more_input` message. This will
/// lead to a `FromControl::Stop` message being send to all preceding operators.
/// Upon receiving it (which can take some time because we might be blocked in a
/// processing function), all active tasks of the operator implementation are
/// cancelled and then the implementation is destroyed. Afterwards, we ignore
/// all data messages arriving and only relay checkpoint messages.
///
/// Shutdown is sequenced as follows: Operators report when they would be "ready
/// to shutdown", which is the case when they only forward checkpoints and don't
/// do any computation anymore. Once all operators have reported that, control
/// closes both input and control channel, which causes the operator runners to
/// terminate since their drivers will become empty.
///
/// [^1]: https://arxiv.org/pdf/1506.08603
class Runner final : public OpCtx {
public:
  Runner(AnyOperator op, AnyOpPull pull_upstream, AnyOpPush push_downstream,
         Receiver<FromControl> from_control, Sender<ToControl> to_control,
         OpId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
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
        match(op_, []<class In, class Out>(const Box<Operator<In, Out>>&) {
          return std::same_as<Out, void>;
        })} {
  }

  Runner(Runner&&) = delete;
  Runner& operator=(Runner&&) = delete;
  Runner(const Runner&) = delete;
  Runner& operator=(const Runner&) = delete;
  ~Runner() override = default;

  auto run_to_completion() && -> Task<void> {
    auto cancellation_token
      = co_await folly::coro::co_current_cancellation_token;
    auto guard = detail::scope_guard{[&] noexcept {
      LOGW("returning from operator runner {} (cancelled = {})", id_,
           cancellation_token.isCancellationRequested());
    }};
    LOGV("creating runner scope");
    co_await driver_.activate([&] -> Task<void> {
      // TODO: Figure out where exactly the operator scope is and move this.
      LOGV("creating operator scope");
      co_await async_scope([&](AsyncScope& operator_scope) -> Task<void> {
        TENZIR_ASSERT(not operator_scope_);
        operator_scope_ = &operator_scope;
        co_await run();
        // Cancel operator-spawned tasks (e.g., background IO coroutines) so
        // the scope join does not block on them.
        operator_scope.cancel();
      });
      // We reset the `operator_scope_` outside as concurrent tasks can still
      // spawn new tasks until they actually terminated due to cancellation.
      // This does not protect against concurrent access from tasks that are not
      // spawned within the `operator_scope_`, but that's UB anyway.
      operator_scope_ = nullptr;
    });
  }

private:
  enum class Phase {
    /// The operator still accepts and processes normal upstream input.
    running,
    /// Graceful shutdown is in progress and may still finalize buffered work.
    stopping_gracefully,
    /// Forced shutdown is in progress and must not emit more regular output.
    stopping_forced,
    /// Shutdown is complete from the operator's perspective.
    stopped,
  };

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

  auto make_counter(MetricsLabel label, MetricsDirection direction,
                    MetricsVisibility visibility) -> MetricsCounter override {
    return exec_ctx_.make_counter(label, direction, visibility);
  }

  auto metrics_receiver() const -> metrics_receiver_actor override {
    return exec_ctx_.metrics_receiver();
  }

  auto is_hidden() const -> bool override {
    return exec_ctx_.is_hidden();
  }

  auto has_terminal() const -> bool override {
    return exec_ctx_.has_terminal();
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
    -> Task<AnySubHandle&> override {
    return spawn_sub_impl(std::move(key), std::move(pipe), input, false);
  }

  auto spawn_sub_fused(SubKey key, ir::pipeline pipe, element_type_tag input)
    -> Task<AnySubHandle&> override {
    return spawn_sub_impl(std::move(key), std::move(pipe), input, true);
  }

  auto spawn_sub_impl(SubKey key, ir::pipeline pipe, element_type_tag input,
                      bool fused) -> Task<AnySubHandle&> {
    auto sub_id = id_.sub(next_subpipeline_id_);
    if (not fused) {
      // For the parallel operator, which is currently the only user of the
      // fused execution mode, we want to associate all subpipelines with the
      // same executor such that all legs contribute to the same metrics.
      next_subpipeline_id_ += 1;
    }
    // Instantiate for the case where it was not instantiated yet.
    if (not pipe.substitute(substitute_ctx{base_ctx{dh_, *reg_}, nullptr},
                            true)) {
      // We just emitted an error. Either we return some placeholder no-op
      // handle now, or we just sleep and wait for cancellation. For now, we
      // pick the simple option, but we might need to reconsider how we want to
      // handle such cases eventually.
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    // Optimize one more time in case it wasn't yet, or we just instantiated.
    auto opt
      = std::move(pipe).optimize(ir::optimize_filter{}, event_order::ordered);
    pipe = std::move(opt.replacement);
    if (not opt.filter.empty()) {
      auto offset = pipe.operators.size();
      for (auto& expr : opt.filter) {
        pipe.operators.push_back(make_where_ir(std::move(expr)));
      }
      std::rotate(pipe.operators.begin(), pipe.operators.begin() + offset,
                  pipe.operators.end());
    }
    auto output = pipe.infer_type(input, dh_);
    // The caller is responsible for passing a well-typed pipeline that
    // type-checks against `input`. And since optimizations are type-preserving,
    // we know that this cannot fail.
    TENZIR_ASSERT(output);
    TENZIR_ASSERT(*output);
    auto spawned = std::move(pipe).spawn(input);
    // TODO: Empty subpipelines need special treatment. We currently assume that
    // they don't exist. Perhaps we should simply insert `pass` if they are empty.
    TENZIR_ASSERT(not spawned.empty());
    auto [from_control_sender, from_control_receiver]
      = channel<FromControl>(16);
    auto [to_control_sender, to_control_receiver] = channel<ToControl>(16);
    auto [runner, push_sub, pull_sub] = match(
      std::tie(input, **output),
      [&]<class In, class Out>(
        tag<In>, tag<Out>) -> std::tuple<Task<void>, AnyOpPush, AnyOpPull> {
        auto chain
          = OperatorChain<In, Out>::try_from(std::move(spawned)).unwrap();
        auto [push_upstream, pull_upstream]
          = exec_ctx_.make_channel<In>(id_.to(sub_id.op(0)));
        // We already checked for non-empty chain above.
        TENZIR_ASSERT(chain.size() > 0);
        auto [push_downstream, pull_downstream]
          = exec_ctx_.make_channel<Out>(sub_id.op(chain.size() - 1).to(id_));
        auto runner
          = fused ? run_chain_fused(std::move(chain), std::move(pull_upstream),
                                    std::move(push_downstream),
                                    std::move(from_control_receiver),
                                    std::move(to_control_sender),
                                    std::move(sub_id), exec_ctx_, sys_, dh_)
                  : run_chain(std::move(chain), std::move(pull_upstream),
                              std::move(push_downstream),
                              std::move(from_control_receiver),
                              std::move(to_control_sender), std::move(sub_id),
                              exec_ctx_, sys_, dh_);
        return {
          std::move(runner),
          AnyOpPush{std::move(push_upstream)},
          AnyOpPull{std::move(pull_downstream)},
        };
      });
    auto [to_parent, from_sub] = channel<Checkpoint>(16);
    auto sub_key = key;
    // Insert the resulting subpipeline into our internal state before starting
    // its runner so shutdown paths can already observe and drain it.
    auto [it, inserted] = subpipelines_.try_emplace(
      std::move(key), std::move(push_sub), std::move(from_sub),
      std::move(from_control_sender), std::move(to_control_receiver), input);
    if (not inserted) {
      panic("already have a subpipeline for that key");
    }
    add_from_sub_recv(it);
    add_to_control_recv(it);
    // TODO: Where do we put this?
    TENZIR_ASSERT(operator_scope_);
    operator_scope_->spawn([this, key = std::move(sub_key),
                            runner = std::move(runner),
                            pull_sub = std::move(pull_sub),
                            to_parent
                            = std::move(to_parent)]() mutable -> Task<void> {
      // The main loop of the subpipeline (or combiner?).
      using Event = variant<Terminated, Option<AnyOperatorMsg>>;
      // TODO: Fully implement checkpointing with `allow_output`.
      auto allow_output = true;
      auto driver = SelectSet<Event>{};
      co_await driver.activate([&] -> Task<void> {
        auto add_pull = [&] {
          driver.add(co_match(pull_sub,
                              []<class Out>(Box<Pull<OperatorMsg<Out>>>& pull)
                                -> Task<Option<AnyOperatorMsg>> {
                                co_return Option<AnyOperatorMsg>{
                                  co_await pull()};
                              }));
        };
        add_pull();
        driver.add([runner = std::move(runner)] mutable -> Task<Terminated> {
          co_await std::move(runner);
          co_return Terminated{};
        });
        while (auto next = co_await driver.next([&](Event const& event) {
          // Block subpipeline output after receiving its checkpoint.
          return allow_output or not is<Option<AnyOperatorMsg>>(event);
        })) {
          co_await co_match(
            *next,
            [&](Option<AnyOperatorMsg> output) -> Task<void> {
              if (not output) {
                // Subpipeline closed its output channel.
                co_return;
              }
              TENZIR_ASSERT(allow_output);
              co_await co_match(
                std::move(*output),
                [&](chunk_ptr output) -> Task<void> {
                  co_await call_process_sub(make_view(key), std::move(output));
                },
                [&](table_slice output) -> Task<void> {
                  co_await call_process_sub(make_view(key), std::move(output));
                },
                [&](Signal signal) -> Task<void> {
                  co_await co_match(
                    signal,
                    [&](EndOfData) -> Task<void> {
                      // We currently only notify the parent pipeline once the
                      // pipeline terminates.
                      co_return;
                    },
                    [&](Checkpoint checkpoint) -> Task<void> {
                      // Notify our parent that we got the checkpoint.
                      co_await to_parent.send(std::move(checkpoint));
                      // Block all future output until the checkpoint of the
                      // parent operator is done.
                      allow_output = false;
                      co_return;
                    });
                });
              add_pull();
            },
            [&](Terminated) -> Task<void> {
              // We wait with informing the parent until everything is done to
              // ensure that we don't send anything anymore.
              co_return;
            });
        }
        // Now is the time to notify the parent. For now, this happens just by
        // dropping `to_parent` and `to_control_sender`, closing the channels.
      });
    });
    co_return it->second.handle;
  }

  auto add_from_sub_recv(std::unordered_map<data, SubPipeline>::iterator it)
    -> void {
    TENZIR_ASSERT(it != subpipelines_.end());
    // `std::unordered_map` guarantees reference stability on insertions, so
    // `from_sub` remains valid until the entry is erased. Erasure only happens
    // in `finish_if_closed` after this task fires and finds the channel drained.
    driver_.add([key = it->first,
                 &from_sub = it->second.from_sub] mutable -> Task<SubMessage> {
      co_return SubMessage{std::move(key), co_await from_sub.recv()};
    });
  }

  auto add_to_control_recv(std::unordered_map<data, SubPipeline>::iterator it)
    -> void {
    TENZIR_ASSERT(it != subpipelines_.end());
    driver_.add([key = it->first,
                 &to_control_receiver
                 = it->second.to_control_receiver] mutable -> Task<SubMessage> {
      co_return SubMessage{std::move(key), co_await to_control_receiver.recv()};
    });
  }

  auto get_sub(SubKeyView key) -> Option<AnySubHandle&> override {
    // TODO: The `materialize` is bad.
    auto it = subpipelines_.find(materialize(key));
    if (it == subpipelines_.end()) {
      return None{};
    }
    return it->second.handle;
  }

  auto io_executor() -> folly::Executor::KeepAlive<folly::IOExecutor> override {
    if (not io_executor_) {
      auto& op = base_op();
      io_executor_
        = exec_ctx_.make_io_executor(id_, demangle_op_type(typeid(op)));
    }
    return io_executor_;
  }

  auto spawn_task(Task<void> task) -> AsyncHandle<void> override {
    TENZIR_ASSERT(operator_scope_);
    return operator_scope_->spawn(std::move(task));
  }

  auto ensure_await_task() -> void {
    if (await_task_pending_ or base_op().state() == OperatorState::done) {
      return;
    }
    await_task_pending_ = true;
    // `await_task()` belongs to the inner operator lifecycle and must stop with
    // it, even if the outer driver keeps running to finish framing work. We
    // spawn it into `operator_scope_` immediately (while it is guaranteed
    // non-null) rather than inside the driver lambda, because the operator
    // scope is set to null before the driver finishes.
    TENZIR_ASSERT(operator_scope_);
    auto handle = operator_scope_->spawn([this] -> Task<ExplicitAny> {
      co_return ExplicitAny{co_await base_op().await_task(*this)};
    });
    driver_.add(
      [handle = std::move(handle)] mutable -> Task<Option<ExplicitAny>> {
        co_return co_await handle.try_join();
      });
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
          return x;
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
    auto& ctx_ref = static_cast<OpCtx&>(*this);
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->process_task(std::move(result), ctx_ref);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->process_task(std::move(result), wrapper, ctx_ref);
        }
      });
  }

  auto call_finalize() -> Task<FinalizeBehavior> {
    auto& ctx_ref = static_cast<OpCtx&>(*this);
    co_return co_await co_match(
      op_,
      [&]<class In, class Out>(
        Box<Operator<In, Out>>& op) -> Task<FinalizeBehavior> {
        if constexpr (std::same_as<Out, void>) {
          co_return co_await op->finalize(ctx_ref);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_return co_await op->finalize(wrapper, ctx_ref);
        }
      });
  }

  auto call_prepare_snapshot() -> Task<void> {
    auto& ctx_ref = static_cast<OpCtx&>(*this);
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->prepare_snapshot(ctx_ref);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->prepare_snapshot(wrapper, ctx_ref);
        }
      });
  }

  auto call_process_sub(SubKeyView key, table_slice slice) -> Task<void> {
    auto& ctx_ref = static_cast<OpCtx&>(*this);
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->process_sub(key, std::move(slice), ctx_ref);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->process_sub(key, std::move(slice), wrapper, ctx_ref);
        }
      });
  }

  auto call_process_sub(SubKeyView key, chunk_ptr chunk) -> Task<void> {
    auto& ctx_ref = static_cast<OpCtx&>(*this);
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->process_sub(key, std::move(chunk), ctx_ref);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->process_sub(key, std::move(chunk), wrapper, ctx_ref);
        }
      });
  }

  auto call_finish_sub(SubKeyView key) -> Task<void> {
    auto& ctx_ref = static_cast<OpCtx&>(*this);
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<Out, void>) {
          co_await op->finish_sub(key, ctx_ref);
        } else {
          auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
          auto wrapper = OpPushWrapper{push};
          co_await op->finish_sub(key, wrapper, ctx_ref);
        }
      });
  }

  template <class DataInput>
  auto call_process(DataInput input) -> Task<void> {
    auto& ctx_ref = static_cast<OpCtx&>(*this);
    co_await co_match(
      op_, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        if constexpr (std::same_as<In, DataInput>) {
          if constexpr (std::same_as<Out, void>) {
            co_await op->process(input, ctx_ref);
          } else {
            auto& push = as<Box<Push<OperatorMsg<Out>>>>(push_downstream_);
            auto wrapper = OpPushWrapper{push};
            co_await op->process(input, wrapper, ctx_ref);
          }
        } else {
          TENZIR_UNREACHABLE();
        }
      });
  }

  auto run() -> Task<void> {
    auto& cancellation_token
      = co_await folly::coro::co_current_cancellation_token;
    try {
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
      co_await folly::coro::co_safe_point;
      ensure_await_task();
      driver_.add(pull_upstream());
      driver_.add(from_control_.recv());
      co_await main_loop();
    } catch (folly::OperationCancelled const&) {
      // Sanity check: We should only propagate this if we actually got cancelled.
      auto cancelled = cancellation_token.isCancellationRequested();
      LOGV("shutting down operator after cancellation: {}", cancelled);
      TENZIR_ASSERT(cancelled);
      throw;
    } catch (std::exception& e) {
      LOGE("shutting down operator after uncaught exception: {}", e.what());
      throw;
    } catch (...) {
      LOGE("shutting down operator after uncaught exception");
      throw;
    }
    LOGW("CANCELING queue");
  }

  auto main_loop() -> Task<void> {
    while (true) {
      co_await folly::coro::co_safe_point;
      ticks_ += 1;
      LOGI("tick {} in {} ({})", ticks_, id_, op_name());
      if (phase_ == Phase::running
          and base_op().state() == OperatorState::done) {
        co_await handle_done(false);
      }
      auto message = co_await driver_.next([&](Event const& event) {
        // When there is an active checkpoint, we only allow
        // subpipeline messages.
        return not active_checkpoint_ or is<SubMessage>(event);
      });
      if (not message) {
        break;
      }
      co_await co_match(std::move(*message), [&](auto message) {
        return process(std::move(message));
      });
    }
  }

  auto process(Option<ExplicitAny> message) -> Task<void> {
    // The task provided by the inner implementation completed.
    await_task_pending_ = false;
    LOGV("got future result in {}", op_name());
    if (phase_ == Phase::stopping_forced or phase_ == Phase::stopped) {
      co_return;
    }
    // If we are not done, then the operator scope is not cancelled, which means
    // that this should always have a value.
    TENZIR_ASSERT(message);
    co_await call_process_task(std::move(message->value));
    if (base_op().state() == OperatorState::done) {
      // This is here because we need to check for completion when
      // `FinalizeBehavior::continue_` was returned. This is not really clean
      // and should be revisited eventually.
      co_await handle_done(false);
    } else {
      // This only happens if we are not in the done state, because whether or
      // not another `await_task` should be scheduled depends on what
      // `finalize()` returns, which is not ideal.
      ensure_await_task();
    }
    LOGV("handled future result in {}", op_name());
  }

  auto process(Option<AnyOperatorMsg> message) -> Task<void> {
    if (not message) {
      LOGV("got end of operator messages in {}", op_name());
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](table_slice input) -> Task<void> {
        LOGV("got input in {}", op_name());
        if (phase_ != Phase::running) {
          co_return;
        }
        co_await call_process(std::move(input));
      },
      [&](chunk_ptr input) -> Task<void> {
        LOGV("got input in {}", op_name());
        if (phase_ != Phase::running) {
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
            got_end_of_data_ = true;
            co_await handle_done(false);
          },
          [&](Checkpoint checkpoint) -> Task<void> {
            co_await begin_checkpoint(checkpoint);
          });
      });
    // We always pull from upstream, even if the operator is done, since we want
    // to continue receiving checkpoints and our shutdown logic depends on it.
    driver_.add(pull_upstream());
  }

  auto begin_checkpoint(Checkpoint checkpoint) -> Task<void> {
    // TODO: This is not fully implement yet. The approach is documented in the
    // docs of the class, but this implementation is not yet complete.
    LOGI("got checkpoint {} in {}", checkpoint.id, op_name());
    co_await to_control_.send(ToControl::checkpoint_begin);
    co_await call_prepare_snapshot();
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
      co_await sub.send(checkpoint);
      sub.wants_commit = true;
    }
    TENZIR_TODO();
  }

  auto process(Option<FromControl> message) -> Task<void> {
    if (not message) {
      LOGV("got end of FromControl in {}", op_name());
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](PostCommit) -> Task<void> {
        LOGV("got post commit in {}", op_name());
        co_await base_op().post_commit(*this);
      },
      [&](HardStop) -> Task<void> {
        if (phase_ == Phase::stopping_forced or phase_ == Phase::stopped) {
          co_return;
        }
        LOGE("got hard stop in {}", op_name());
        co_await handle_done(true);
      });
    driver_.add(from_control_.recv());
  }

  auto process(SubMessage message) -> Task<void> {
    auto it = subpipelines_.find(message.key);
    TENZIR_ASSERT(it != subpipelines_.end());
    auto& sub = it->second;
    // Erases the sub and notifies the operator once both channels have drained.
    // Uses explicit flags rather than is_drained() to avoid a race where both
    // tasks fire and sit in the driver queue simultaneously: the first would
    // see both channels drained and erase the entry, leaving the second with a
    // dangling key.
    auto finish_if_closed = [&] -> Task<void> {
      auto closed = sub.from_sub_done and sub.to_control_done;
      if (not closed) {
        co_return;
      }
      subpipelines_.erase(it);
      if (phase_ != Phase::stopping_forced) {
        co_await call_finish_sub(make_view(message.key));
      }
      co_await check_done();
    };
    co_await co_match(
      std::move(message.event),
      [&](Option<Checkpoint> checkpoint) -> Task<void> {
        if (not checkpoint) {
          sub.from_sub_done = true;
          co_await finish_if_closed();
          co_return;
        }
        TENZIR_UNUSED(checkpoint);
        TENZIR_TODO();
        add_from_sub_recv(it);
        co_return;
      },
      [&](Option<ToControl> to_control) -> Task<void> {
        if (not to_control) {
          sub.to_control_done = true;
          co_await finish_if_closed();
          co_return;
        }
        switch (*to_control) {
          case ToControl::no_more_input:
            // TODO: What exactly do we need to do here?
            sub.closed_data = true;
            break;
          case ToControl::ready_for_shutdown:
            // We immediately shut down the subpipeline once the request
            // arrives. The subpipeline will not be part of future checkpoints.
            // There is a small delay that needs to be accounted for by the
            // checkpointing logic until the shutdown fully propagates and we
            // remove the subpipeline from our bookkeeping. Note that both
            // handles are only safe to drop here because `SubHandle` may
            // only be used inside the main-loop functions, which this drop is
            // also part of. Thus, there is no concurrent use.
            // TODO: Once we implement the rest of the checkpointing logic, we
            // probably need to defer the closing of these channels until the
            // post-commit occurs, as the pipeline needs to be kept alive then.
            sub.closed_data = true;
            sub.push = None{};
            sub.from_control_sender = None{};
            break;
          case ToControl::checkpoint_begin:
          case ToControl::checkpoint_done:
            TENZIR_TODO();
        }
        add_to_control_recv(it);
      });
  }

  auto handle_done(bool force) -> Task<void> {
    if (phase_ == Phase::stopping_forced or phase_ == Phase::stopped) {
      co_return;
    }
    LOGV("running done in {}", op_name());
    auto first_time = phase_ == Phase::running;
    phase_ = force ? Phase::stopping_forced : Phase::stopping_gracefully;
    if (force) {
      // Cancel operator-scoped tasks immediately so that background IO
      // coroutines stop producing work before the subpipelines drain.
      // check_done() will also cancel, but only once subpipelines_.empty(),
      // which may be too late to prevent further output.
      TENZIR_ASSERT(operator_scope_);
      operator_scope_->cancel();
      for (auto& [_, sub] : subpipelines_) {
        sub.closed_data = true;
        sub.push = None{};
        if (sub.from_control_sender) {
          co_await sub.from_control_sender->send(HardStop{});
          sub.from_control_sender = None{};
        }
      }
      co_await check_done();
      co_return;
    }
    if (first_time) {
      // Notify control only if we are stopping before normal end-of-stream.
      if (not input_is_void_ and not got_end_of_data_) {
        co_await to_control_.send(ToControl::no_more_input);
      }
    }
    // Then finalize the operator, which can still produce output.
    auto b = co_await call_finalize();
    if (b == FinalizeBehavior::continue_) {
      // The operator is still draining and will transition to `stopped` via
      // further `await_task()` calls. Re-arm so those calls keep flowing.
      operator_draining_ = true;
      ensure_await_task();
      co_return;
    }
    // Tell all subpipelines to shut down. Note that the previous step could
    // have still pushed data into them. The main loop continues running to
    // drain remaining subpipeline output. `check_done()` completes the shutdown
    // once all are gone.
    for (auto& [key, sub] : subpipelines_) {
      // It might be that push is closed if the pipeline is already shutting down.
      if (sub.push and not sub.closed_data and not sub.input.is<void>()) {
        sub.closed_data = true;
        co_await co_match(*sub.push,
                          [&]<class In>(Box<Push<OperatorMsg<In>>>& push) {
                            return push(EndOfData{});
                          });
      }
    }
    co_await check_done();
  }

  auto check_done() -> Task<void> {
    if (phase_ == Phase::running or phase_ == Phase::stopped) {
      co_return;
    }
    if (not subpipelines_.empty()) {
      // We need to wait for all subpipelines to finish.
      co_return;
    }
    if (operator_draining_ and base_op().state() != OperatorState::done) {
      // The operator returned FinalizeBehavior::continue_ and has not
      // transitioned to done yet.
      co_return;
    }
    // If we reach this point, then we are about to fully shut down. The only
    // remaining tasks in the operator scope are from `await_task()` and
    // `ctx.spawn_task(…)`. If the operator still needs to keep them running, it
    // should not have requested the execution to continue. So we assume we can
    // just cancel them.
    TENZIR_ASSERT(operator_scope_);
    operator_scope_->cancel();
    if (phase_ == Phase::stopping_gracefully and not output_is_void_) {
      LOGW("sending end of data from {}", id_);
      co_await push_signal(EndOfData{});
    }
    LOGW("sending ready to shutdown from {}", id_);
    co_await to_control_.send(ToControl::ready_for_shutdown);
    phase_ = Phase::stopped;
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
  std::shared_ptr<const registry> reg_ = global_registry();
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;

  size_t next_subpipeline_id_ = 0;

  // TODO: Better map type (but check for UB from channel being moved).
  std::unordered_map<data, SubPipeline> subpipelines_;

  /// Scope used for tasks spawned by the inner operator implementation.
  ///
  /// This scope can be smaller than the `driver_` scope because the outer
  /// framing is even kept alive after the operator itself finished in order to
  /// propagate checkpoints.
  AsyncScope* operator_scope_ = nullptr;

  /// True when finalize() returned continue_ — the operator is still draining
  /// and will transition to done via state(). check_done() must not cancel
  /// operator_scope_ while this is set and the operator has not returned done.
  bool operator_draining_ = false;

  /// Anything the main loop reacts to.
  using Event = variant<
    // Input from our upstream.
    Option<AnyOperatorMsg>,
    // Control message.
    Option<FromControl>,
    // Result of `await_task()` or `None` if cancelled.
    Option<ExplicitAny>,
    // Anything coming from a subpipeline.
    SubMessage>;

  /// The primary stream of events that the main loop reacts to.
  SelectSet<Event> driver_;
  /// Whether we want to exclusively listen for messages from subpipelines.
  bool active_checkpoint_{false};
  /// True if there is an `await_task()` not yet processed by the mainloop.
  bool await_task_pending_{false};
  /// Whether `EndOfData` was already received from upstream.
  bool got_end_of_data_{false};
  /// Tracks the shutdown sequencing of the operator.
  Phase phase_{Phase::running};
  // TODO: Expose this as an atomic for metrics?
  size_t ticks_{0};
};

namespace {

template <class Input, class Output>
auto run_operator(Box<Operator<Input, Output>> op,
                  Box<Pull<OperatorMsg<Input>>> pull_upstream,
                  Box<Push<OperatorMsg<Output>>> push_downstream,
                  Receiver<FromControl> from_control,
                  Sender<ToControl> to_control, OpId id, ExecCtx& exec_ctx,
                  caf::actor_system& sys, DiagHandler& dh) -> Task<void> {
  co_await folly::coro::co_safe_point;
  auto runner = Runner{
    AnyOperator{std::move(op)},
    AnyOpPull{std::move(pull_upstream)},
    AnyOpPush{std::move(push_downstream)},
    std::move(from_control),
    std::move(to_control),
    std::move(id),
    exec_ctx,
    sys,
    dh,
  };
  auto result
    = co_await catch_cancellation(std::move(runner).run_to_completion());
  // Ensure that we only cancel if cancellation was requested from outside.
  co_await folly::coro::co_safe_point;
  TENZIR_ASSERT(result);
  co_return;
}

/// Demangle a C++ type name and strip the namespace prefix.
auto demangle_op_type(std::type_info const& type) -> std::string {
  auto demangled = folly::demangle(type);
  auto result = std::string{demangled};
  // Strip namespace prefix, keeping only the class name.
  // Only look before '<' to avoid matching '::' inside template arguments.
  auto tpl = result.find('<');
  auto prefix = tpl != std::string::npos ? result.substr(0, tpl) : result;
  auto pos = prefix.rfind("::");
  if (pos != std::string::npos) {
    result = result.substr(pos + 2);
  }
  return result;
}

} // namespace

class ChainRunner {
public:
  ChainRunner(std::vector<AnyOperator> operators, AnyOpPull pull_upstream,
              AnyOpPush push_downstream, Receiver<FromControl> from_control,
              Sender<ToControl> to_control, PipeId id, ExecCtx& exec_ctx,
              caf::actor_system& sys, DiagHandler& dh)
    : operators_{std::move(operators)},
      pull_upstream_{std::move(pull_upstream)},
      push_downstream_{std::move(push_downstream)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      id_{std::move(id)},
      exec_ctx_{exec_ctx},
      sys_{sys},
      dh_{dh} {
    // TODO: Validate types, just to make sure.
  }

  auto run_to_completion() && -> Task<void> {
    auto guard = detail::scope_guard{[&] noexcept {
      LOGI("returning from chain runner {}", id_);
    }};
    LOGV("creating chain runner scope");
    co_await driver_.activate([&] -> Task<void> {
      LOGW("beginning chain setup of {}", id_);
      spawn_operators();
      LOGW("entering main loop of {}", id_);
      co_await run_until_shutdown();
      LOGW("leaving driver scope of {}", id_);
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
        auto input = std::move(as<Box<Pull<OperatorMsg<In>>>>(next_input));
        auto last = index == operators_.size() - 1;
        auto output_sender = [&]() -> Box<Push<OperatorMsg<Out>>> {
          if (last) {
            return std::move(as<Box<Push<OperatorMsg<Out>>>>(push_downstream_));
          }
          auto [sender, receiver]
            = exec_ctx_.make_channel<Out>(id_.op(index).to(id_.op(index + 1)));
          next_input = std::move(receiver);
          return std::move(sender);
        }();
        auto [from_control_sender, from_control_receiver]
          = channel<FromControl>(16);
        auto [to_control_sender, to_control_receiver] = channel<ToControl>(16);
        op_controls_.push_back(OpControl{
          Option<Sender<FromControl>>{std::move(from_control_sender)},
          std::move(to_control_receiver),
        });
        auto executor = exec_ctx_.make_executor(id_.op(index),
                                                demangle_op_type(typeid(*op)));
        auto task = run_operator(std::move(op), std::move(input),
                                 std::move(output_sender),
                                 std::move(from_control_receiver),
                                 std::move(to_control_sender), id_.op(index),
                                 exec_ctx_, sys_, dh_);
        LOGI("spawning operator task");
        driver_.add([task = std::move(task), index,
                     executor = std::move(executor)] mutable
                      -> Task<std::pair<size_t, Terminated>> {
          co_await folly::coro::co_withExecutor(std::move(executor),
                                                std::move(task));
          LOGI("got termination from operator {}", index);
          co_return {index, Terminated{}};
        });
      });
    }
    for (auto index = size_t{0}; index < operators_.size(); ++index) {
      add_control_read(index);
    }
  }

  auto add_control_read(size_t index) -> void {
    driver_.add([this, index] -> Task<std::pair<size_t, Option<ToControl>>> {
      TENZIR_ASSERT(index < op_controls_.size());
      co_return {index, co_await op_controls_[index].receiver.recv()};
    });
  }

  auto run_until_shutdown() -> Task<void> {
    auto ready_for_shutdown = size_t{0};
    auto got_ready_for_shutdown = std::vector<bool>(operators_.size(), false);
    driver_.add(from_control_.recv());
    while (auto next = co_await driver_.next()) {
      co_await co_match(
        std::move(*next),
        [&](Option<FromControl> from_control) -> Task<void> {
          if (not from_control) {
            co_return;
          }
          co_await co_match(
            std::move(*from_control),
            [&](PostCommit) -> Task<void> {
              for (auto& ctrl : op_controls_) {
                if (ctrl.sender) {
                  co_await ctrl.sender->send(PostCommit{});
                }
              }
            },
            [&](HardStop) -> Task<void> {
              for (auto& ctrl : op_controls_) {
                if (ctrl.sender) {
                  co_await ctrl.sender->send(HardStop{});
                }
              }
            });
          driver_.add(from_control_.recv());
        },
        [&](std::pair<size_t, variant<Terminated, Option<ToControl>>> next)
          -> Task<void> {
          auto [index, kind] = std::move(next);
          co_await co_match(
            std::move(kind),
            [&](Terminated) -> Task<void> {
              // TODO: What if we didn't send shutdown signal?
              LOGW("got shutdown from {}", id_.op(index));
              co_return;
            },
            [&](Option<ToControl> to_control) -> Task<void> {
              if (not to_control) {
                co_return;
              }
              LOGW("got control message from operator {}: {}", id_.op(index),
                   *to_control);
              switch (*to_control) {
                case ToControl::ready_for_shutdown: {
                  TENZIR_ASSERT(index < got_ready_for_shutdown.size());
                  TENZIR_ASSERT(not got_ready_for_shutdown[index]);
                  got_ready_for_shutdown[index] = true;
                  TENZIR_ASSERT(ready_for_shutdown < operators_.size());
                  // Once an operator declared shutdown readiness, it must not
                  // receive further control messages. Closing the sender lets
                  // its pending control receive drain to `None`.
                  op_controls_[index].sender = None{};
                  ready_for_shutdown += 1;
                  if (ready_for_shutdown == operators_.size()) {
                    // Once we are here, we got a request to shutdown from all
                    // operators. However, since we might be running in a
                    // subpipeline that is not ready to shutdown yet, we first
                    // have to ask control whether we are allowed to.
                    LOGW("got ready to shutdown from all in chain");
                    co_await to_control_.send(ToControl::ready_for_shutdown);
                  }
                  break;
                }
                case ToControl::no_more_input:
                  for (auto preceding = size_t{0}; preceding < index;
                       ++preceding) {
                    if (op_controls_[preceding].sender) {
                      co_await op_controls_[preceding].sender->send(HardStop{});
                    }
                  }
                  co_await to_control_.send(ToControl::no_more_input);
                  break;
                case ToControl::checkpoint_begin:
                case ToControl::checkpoint_done:
                  LOGI("chain got {} from operator {}", to_control,
                       id_.op(index));
                  break;
              }
              add_control_read(index);
            });
        });
    }
    LOGW("left main loop of {}", id_);
    TENZIR_ASSERT(ready_for_shutdown == operators_.size());
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

  struct OpControl {
    Option<Sender<FromControl>> sender;
    Receiver<ToControl> receiver;
  };
  std::vector<OpControl> op_controls_;

  JoinSet<variant<
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
    driver_;
};

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> pull_upstream,
               Box<Push<OperatorMsg<Output>>> push_downstream,
               Receiver<FromControl> from_control, Sender<ToControl> to_control,
               PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys,
               DiagHandler& dh) -> Task<void> {
  TENZIR_ASSERT(chain.size() != 0);
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

template auto
run_chain(OperatorChain<void, void> chain,
          Box<Pull<OperatorMsg<void>>> pull_upstream,
          Box<Push<OperatorMsg<void>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

template auto
run_chain(OperatorChain<table_slice, void> chain,
          Box<Pull<OperatorMsg<table_slice>>> pull_upstream,
          Box<Push<OperatorMsg<void>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

template auto
run_chain(OperatorChain<chunk_ptr, void> chain,
          Box<Pull<OperatorMsg<chunk_ptr>>> pull_upstream,
          Box<Push<OperatorMsg<void>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

template auto
run_chain(OperatorChain<void, chunk_ptr> chain,
          Box<Pull<OperatorMsg<void>>> pull_upstream,
          Box<Push<OperatorMsg<chunk_ptr>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

template auto
run_chain(OperatorChain<chunk_ptr, chunk_ptr> chain,
          Box<Pull<OperatorMsg<chunk_ptr>>> pull_upstream,
          Box<Push<OperatorMsg<chunk_ptr>>> push_downstream,
          Receiver<FromControl> from_control, Sender<ToControl> to_control,
          PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys, DiagHandler& dh)
  -> Task<void>;

template auto
run_chain(OperatorChain<table_slice, chunk_ptr> chain,
          Box<Pull<OperatorMsg<table_slice>>> pull_upstream,
          Box<Push<OperatorMsg<chunk_ptr>>> push_downstream,
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
  static auto next = Atomic<size_t>{0};
  auto id = next.fetch_add(1, std::memory_order::relaxed);
  return PipeId{fmt::to_string(id)};
}

} // namespace

auto run_pipeline(OperatorChain<void, void> pipeline, ExecCtx& exec_ctx,
                  caf::actor_system& sys, DiagHandler& dh) -> Task<void> {
  auto id = new_pipe_id();
  auto [push_input, pull_input]
    = exec_ctx.make_channel<void>(ChannelId::first(id.op(0)));
  auto [push_output, pull_output]
    = exec_ctx.make_channel<void>(ChannelId::last(id.op(pipeline.size() - 1)));
  auto result = co_await async_try([&]() -> Task<void> {
    auto [from_control_sender, from_control_receiver]
      = channel<FromControl>(16);
    auto [to_control_sender, to_control_receiver] = channel<ToControl>(16);
    auto driver = JoinSet<
      variant<Terminated, Option<ToControl>, Option<OperatorMsg<void>>>>{};
    LOGV("creating pipeline queue scope");
    co_await driver.activate([&] -> Task<void> {
      driver.add([&] -> Task<Terminated> {
        co_await run_chain(std::move(pipeline), std::move(pull_input),
                           std::move(push_output),
                           std::move(from_control_receiver),
                           std::move(to_control_sender), id, exec_ctx, sys, dh);
        co_return Terminated{};
      });
      driver.add(pull_output());
      driver.add(to_control_receiver.recv());
#if 0
      // TODO: We just have this right now to simulate checkpointing.
      queue.scope().spawn([&] -> Task<std::monostate> {
        while (true) {
          auto checkpoint = Checkpoint{uuid::random()};
          LOGI("injecting checkpoint {} into pipeline", checkpoint.id);
          co_await push_input(std::move(checkpoint));
          co_await folly::coro::sleep(std::chrono::seconds{1});
        }
      });
#endif
      while (auto next = co_await driver.next()) {
        co_await co_match(
          std::move(*next),
          [&](Terminated) -> Task<void> {
            // TODO: The pipeline terminated?
            LOGI("run_pipeline got info that chain terminated");
            co_return;
          },
          [&](Option<ToControl> to_control) -> Task<void> {
            if (not to_control) {
              co_return;
            }
            switch (*to_control) {
              case ToControl::no_more_input:
                // We don't feed it any input either way.
                break;
              case ToControl::ready_for_shutdown:
                LOGE("got ready to shutdown in outermost subpipeline");
                {
                  // FIXME: We should not leave this dangling.
                  auto _input = std::move(push_input);
                  auto _control = std::move(from_control_sender);
                }
                break;
              case ToControl::checkpoint_begin:
              case ToControl::checkpoint_done:
                TENZIR_TODO();
            }
            driver.add(to_control_receiver.recv());
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
                  TENZIR_UNUSED(checkpoint);
                  LOGI("checkpoint {} is leaving pipeline", checkpoint.id);
                });
            });
            driver.add(pull_output());
            co_return;
          });
      }
    });
  }());
  if (result.is_ok()) {
    co_return;
  }
  auto exception = std::move(result).unwrap_err();
  if (exception.is_compatible_with<panic_exception>()) {
    dh.emit(to_diagnostic(*exception.get_exception<panic_exception>()));
    co_return;
  }
  diagnostic::error("uncaught exception in pipeline: {}", exception.what())
    .emit(dh);
}

} // namespace tenzir
