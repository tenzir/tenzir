//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <folly/Executor.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace tenzir {

class Pass final : public Operator<table_slice, table_slice> {
public:
  auto process(table_slice input, Push<table_slice>& push, AsyncCtx& ctx)
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

struct PostCommit {};
struct Shutdown {};

using FromControl = variant<PostCommit, Shutdown>;

enum class ToControl {
  /// Notify the host that we are ready to shutdown. After emitting this, the
  /// operator is no longer allowed to send data, so it should tell its previous
  /// operator to stop and its subsequent operator that it will not get any more
  /// input.
  ready_for_shutdown,
  /// Say that we do not want any more input.
  no_more_input,
};

template <class T>
using SpscQueue = folly::coro::SmallUnboundedQueue<T, true, true>;

template <class T>
class Receiver {
public:
  explicit Receiver(std::shared_ptr<SpscQueue<T>> queue)
    : queue_{std::move(queue)} {
  }

  auto receive() -> Task<T> {
    return queue_->dequeue();
  }

private:
  std::shared_ptr<SpscQueue<T>> queue_;
};

template <class T>
class Sender {
public:
  explicit Sender(std::shared_ptr<SpscQueue<T>> queue)
    : queue_{std::move(queue)} {
  }

  auto send(T x) -> void {
    queue_->enqueue(std::move(x));
  }

private:
  std::shared_ptr<SpscQueue<T>> queue_;
};

template <class T>
auto make_unbounded_channel() -> std::pair<Sender<T>, Receiver<T>> {
  auto shared = std::make_shared<SpscQueue<T>>();
  return {Sender<T>{shared}, Receiver<T>{shared}};
}

template <class T>
class OpPush final : public Push<T> {
public:
  explicit OpPush(OpSender<T>& sender) : sender_{sender} {
  }

  auto operator()(T output) -> Task<void> override {
    co_await sender_.send(std::move(output));
  }

private:
  OpSender<T>& sender_;
};

template <class Input, class Output>
  requires(not std::same_as<Input, void>)
struct NonSourceRunner {
  auto run_to_completion() -> Task<void> {
    TENZIR_INFO("entering run loop for non-source");
    while (not got_shutdown_request) {
      co_await tick();
    }
  }

  auto tick() -> Task<void> {
    TENZIR_INFO("tick non-source");
    auto ctx = AsyncCtx{sys_};
    switch (op->state()) {
      case OperatorState::no_more_input:
        handle_no_more_input();
        break;
      case OperatorState::unspecified:
        break;
    }
    // FIXME: This might not be the best approach, because we have to cancel
    // futures. We could instead keep them running.
    TENZIR_VERBOSE("waiting in non-source for message");
    auto message
      = co_await select_into_variant(input.receive(), from_control.receive());
    co_await match(
      std::move(message),
      [&](OperatorMessage<Input> message) -> Task<void> {
        co_await match(
          std::move(message),
          [&](Input input) -> Task<void> {
            TENZIR_VERBOSE("got input in non-source");
            if (sent_no_more_input) {
              // No need to forward the input.
              co_return;
            }
            if constexpr (std::same_as<Output, void>) {
              co_await op->process(input, ctx);
            } else {
              auto push = OpPush<Output>{};
              co_await op->process(input, push, ctx);
            }
          },
          [&](Signal signal) -> Task<void> {
            TENZIR_VERBOSE("got signal in non-source");
            switch (signal) {
              case Signal::end_of_data:
                TENZIR_VERBOSE("received end of data in non-source");
                if constexpr (not std::same_as<Output, void>) {
                  auto push = OpPush<Output>{output};
                  TENZIR_VERBOSE("finalizing in non-source");
                  co_await op->finalize(push, ctx);
                }
                TENZIR_VERBOSE("sending end of data from non-source");
                co_await output.send(Signal::end_of_data);
                TENZIR_WARN("sending ready to shutdown from non-source");
                to_control.send(ToControl::ready_for_shutdown);
                TENZIR_VERBOSE("finished end of data in non-source");
                co_return;
              case Signal::checkpoint:
                TENZIR_ERROR("got checkpoint in non-source");
                co_await op->checkpoint();
                co_await output.send(Signal::checkpoint);
                co_return;
            }
            TENZIR_UNREACHABLE();
          });
      },
      [&](FromControl message) -> Task<void> {
        TENZIR_VERBOSE("got control message in non-source");
        co_await match(
          std::move(message),
          [&](PostCommit) -> Task<void> {
            co_return;
          },
          [&](Shutdown) -> Task<void> {
            // We won't perform any cleanup. This might be undesirable.
            got_shutdown_request = true;
            co_return;
          });
      });
  }

  auto handle_no_more_input() -> void {
    TENZIR_VERBOSE("...");
    if (sent_no_more_input) {
      return;
    }
    to_control.send(ToControl::no_more_input);
    sent_no_more_input = true;
  }

  Box<Operator<Input, Output>> op;
  OpReceiver<Input> input;
  OpSender<Output> output;
  Receiver<FromControl> from_control;
  Sender<ToControl> to_control;
  caf::actor_system& sys_;

  bool got_shutdown_request = false;
  bool sent_no_more_input = false;
};

template <class Output>
class SourceRunner {
public:
  SourceRunner(Box<Operator<void, Output>> op, OpReceiver<void> input,
               OpSender<Output> output, Receiver<FromControl> from_control,
               Sender<ToControl> to_control, caf::actor_system& sys)
    : op{std::move(op)},
      input{std::move(input)},
      output{std::move(output)},
      from_control{std::move(from_control)},
      to_control{std::move(to_control)},
      sys_{sys} {
  }

  auto run_to_completion() -> Task<void> {
    TENZIR_INFO("entering run loop for source");
    co_await enqueue(op->next());
    co_await enqueue(input.receive());
    co_await enqueue(from_control.receive());
    while (not got_shutdown_request) {
      co_await tick();
    }
    // TODO: Do we know that we can always cancel stuff?
    co_await scope_.cancelAndJoinAsync();
  }

private:
  auto tick() -> Task<void> {
    TENZIR_INFO("tick source");
    auto ctx = AsyncCtx{sys_};
    TENZIR_VERBOSE("waiting in source for message");
    auto message = co_await queue_.dequeue();
    co_await match(
      message,
      [&](std::any message) -> Task<void> {
        TENZIR_VERBOSE("got future result in source");
        // The task provided by the inner implementation completed.
        if (message.has_value()) {
          // The source wants to continue.
          TENZIR_VERBOSE("...");
          auto push = OpPush<Output>{output};
          co_await op->process(std::move(message), push, ctx);
          co_await enqueue(op->next());
          TENZIR_VERBOSE("...");
        } else {
          // The source is done.
          TENZIR_VERBOSE("sending end of data from source");
          co_await output.send(Signal::end_of_data);
          TENZIR_WARN("sending ready to shutdown from source");
          to_control.send(ToControl::ready_for_shutdown);
          // Do must not enqueue the source again.
        }
      },
      [&](OperatorMessage<void> message) -> Task<void> {
        TENZIR_VERBOSE("got operator message in source");
        co_await match(message, [&](Signal signal) -> Task<void> {
          switch (signal) {
            case Signal::end_of_data:
              TENZIR_UNREACHABLE();
            case Signal::checkpoint:
              TENZIR_ERROR("got checkpoint in source");
              co_await op->checkpoint();
              co_await output.send(Signal::checkpoint);
              co_return;
          }
          TENZIR_UNREACHABLE();
        });
        co_await enqueue(input.receive());
      },
      [&](FromControl message) -> Task<void> {
        TENZIR_VERBOSE("got control message in source");
        co_await match(
          message,
          [&](PostCommit) -> Task<void> {
            co_await op->post_commit();
          },
          [&](Shutdown) -> Task<void> {
            got_shutdown_request = true;
            co_return;
          });
        co_await enqueue(from_control.receive());
      });
  }

  template <class T>
  auto enqueue(Task<T> task) -> Task<void> {
    auto executor = co_await folly::coro::co_current_executor;
    scope_.add(folly::coro::co_withExecutor(
      executor, folly::coro::co_invoke(
                  [this, task = std::move(task)] mutable -> Task<void> {
                    queue_.enqueue(co_await std::move(task));
                  })));
  }

  Box<Operator<void, Output>> op;
  OpReceiver<void> input;
  OpSender<Output> output;
  Receiver<FromControl> from_control;
  Sender<ToControl> to_control;
  caf::actor_system& sys_;

  bool got_shutdown_request = false;

  using QueueItem = variant<std::any, OperatorMessage<void>, FromControl>;

  // FIXME: Are those used as SPSC? No!
  SpscQueue<QueueItem> queue_;
  folly::coro::CancellableAsyncScope scope_;
};

template <class Input, class Output>
auto run_operator(Box<Operator<Input, Output>> op, OpReceiver<Input> input,
                  OpSender<Output> output, Receiver<FromControl> from_control,
                  Sender<ToControl> to_control, caf::actor_system& sys)
  -> Task<void> {
  if constexpr (std::same_as<Input, void>) {
    co_await SourceRunner<Output>{
      std::move(op),           std::move(input),      std::move(output),
      std::move(from_control), std::move(to_control), sys,
    }
      .run_to_completion();
  } else {
    co_await NonSourceRunner<Input, Output>{
      std::move(op),           std::move(input),      std::move(output),
      std::move(from_control), std::move(to_control), sys,
    }
      .run_to_completion();
  }
}

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain, OpReceiver<Input> input,
               OpSender<Output> output, caf::actor_system& sys) -> Task<void> {
  // TODO: Just pretend that we have this case.
  TENZIR_ASSERT(chain.size() == 2);
  auto operators = std::move(chain).unwrap();
  auto first = as<Box<Operator<Input, table_slice>>>(std::move(operators[0]));
  auto second = as<Box<Operator<table_slice, Output>>>(std::move(operators[1]));
  auto channel = std::make_shared<OpChannel<table_slice>>(10);
  auto sender = OpSender<table_slice>{channel};
  auto receiver = OpReceiver<table_slice>{std::move(channel)};
  // FIXME: Those are not shared.
  auto [from_control_sender1, from_control_receiver1]
    = make_unbounded_channel<FromControl>();
  auto [to_control_sender1, to_control_receiver1]
    = make_unbounded_channel<ToControl>();
  auto [from_control_sender2, from_control_receiver2]
    = make_unbounded_channel<FromControl>();
  auto [to_control_sender2, to_control_receiver2]
    = make_unbounded_channel<ToControl>();
  TENZIR_WARN("getting operator run tasks");
  auto run_1
    = run_operator(std::move(first), std::move(input), std::move(sender),
                   std::move(from_control_receiver1),
                   std::move(to_control_sender1), sys);
  auto run_2
    = run_operator(std::move(second), std::move(receiver), std::move(output),
                   std::move(from_control_receiver2),
                   std::move(to_control_sender2), sys);
  TENZIR_WARN("waiting for all run tasks to finish");
  co_await folly::coro::collectAll(std::move(run_1), std::move(run_2));
}

auto run_pipeline(OperatorChain<void, void> pipeline, OpReceiver<void> input,
                  OpSender<void> output, caf::actor_system& sys) -> Task<void> {
  return run_chain(std::move(pipeline), std::move(input), std::move(output),
                   sys);
  // auto first = Box<TransformationOperator>{};
  // auto second = Box<TransformationOperator>{};
  // auto ctrl1 = CtrlReceiver{};
  // auto ctrl2 = CtrlReceiver{};
  // // TODO
  // auto [s0, r1] = make_batch_channel(10);
  // auto [s1, r2] = make_batch_channel(10);
  // folly::coro::co_withExecutor(folly::getGlobalCPUExecutor(),
  //                              run_transformation(std::move(first),
  //                                                 std::move(r1),
  //                                                 std::move(s1),
  //                                                 std::move(ctrl1)))
  //   .start();
}

} // namespace tenzir
