//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <folly/Executor.h>
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

enum class Signal {
  /// No more data will come after this signal.
  end_of_data,
  /// Request to perform a checkpoint.
  checkpoint,
};

template <class T>
struct OperatorMessage : variant<T, Signal> {
  using variant<T, Signal>::variant;
};

template <>
struct OperatorMessage<void> : variant<Signal> {
  using variant<Signal>::variant;
};

template <class T>
static auto cost(const OperatorMessage<T>& item) -> size_t {
  return match(
    item,
    [](const table_slice& slice) -> size_t {
      return slice.rows();
    },
    [](const chunk_ptr& chunk) -> size_t {
      return chunk ? chunk->size() : 0;
    },
    [](const Signal&) {
      return size_t{1};
    });
};

/// Data channel between two operators.
template <class T>
struct OpChannel {
public:
  explicit OpChannel(size_t limit) : mutex_{Locked{limit}} {
  }

  auto send(OperatorMessage<T> x) -> Task<void> {
    auto lock = co_await mutex_.lock();
    while (cost(x) > lock->remaining) {
      lock.unlock();
      co_await remaining_increased_;
      remaining_increased_.reset();
      lock = co_await mutex_.lock();
    }
    lock->remaining -= cost(x);
    lock->queue.push_back(x);
    queue_pushed_.post();
  }

  auto receive() -> Task<OperatorMessage<T>> {
    auto lock = co_await mutex_.lock();
    while (lock->queue.empty()) {
      lock.unlock();
      co_await queue_pushed_;
      queue_pushed_.reset();
      lock = co_await mutex_.lock();
    }
    auto result = std::move(lock->queue.front());
    lock->queue.pop_front();
    lock->remaining += cost(result);
    remaining_increased_.post();
    co_return result;
  }

private:
  struct Locked {
    explicit Locked(size_t limit) : remaining{limit} {
    }

    size_t remaining;
    std::deque<OperatorMessage<T>> queue;
  };

  // TODO: This can surely be written better?
  Mutex<Locked> mutex_;
  folly::coro::Baton remaining_increased_;
  folly::coro::Baton queue_pushed_;
};

template <class T>
class OpSender {
public:
  explicit OpSender(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  auto send(OperatorMessage<T> x) -> Task<void> {
    return shared_->send(std::move(x));
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

template <class T>
class OpReceiver {
public:
  explicit OpReceiver(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  auto receive() -> Task<OperatorMessage<T>> {
    return shared_->receive();
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

struct PostCommit {};
struct ShutdownRequest {};

using FromControl = variant<PostCommit, ShutdownRequest>;

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
class Receiver {
public:
  auto receive() -> Task<T> {
    co_await std::suspend_always{};
    TENZIR_UNREACHABLE();
  }
};

template <class T>
class Sender {
public:
  auto send(T x) -> void {
    TENZIR_UNREACHABLE();
  }
};

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

/// A sequence of operators with the given input and output.
template <class Input, class Output>
class OperatorChain {
public:
  auto try_from(std::vector<OperatorPtr> operators)
    -> std::optional<OperatorChain<Input, Output>> {
    // TODO: Implement properly.
    return OperatorChain{std::move(operators)};
  }

  auto size() const -> size_t {
    return operators_.size();
  }

  auto operator[](size_t index) const -> const OperatorPtr& {
    return operators_[index];
  }

  auto unwrap() && -> std::vector<OperatorPtr> {
    return std::move(operators_);
  }

private:
  explicit OperatorChain(std::vector<OperatorPtr> operators)
    : operators_{std::move(operators)} {
  }

  std::vector<OperatorPtr> operators_;
};

template <class Input, class Output>
  requires(not std::same_as<Input, void>)
struct NonSourceRunner {
  auto run_to_completion() -> Task<void> {
    while (not got_shutdown_request) {
      co_await tick();
    }
  }

  auto tick() -> Task<void> {
    auto ctx = AsyncCtx{};
    switch (op->state()) {
      case OperatorState::no_more_input:
        handle_no_more_input();
        break;
      case OperatorState::unspecified:
        break;
    }
    // FIXME: This might not be the best approach, because we have to cancel
    // futures. We could instead keep them running.
    auto message
      = co_await select_into_variant(input.receive(), from_control.receive());
    co_await match(
      std::move(message),
      [&](OperatorMessage<Input> message) -> Task<void> {
        co_await match(
          std::move(message),
          [&](Input input) -> Task<void> {
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
            switch (signal) {
              case Signal::end_of_data:
                if constexpr (not std::same_as<Output, void>) {
                  auto push = OpPush<Output>{output};
                  co_await op->finalize(push, ctx);
                }
                co_return;
              case Signal::checkpoint:
                co_await op->checkpoint();
                co_await output.send(Signal::checkpoint);
                co_return;
            }
            TENZIR_UNREACHABLE();
          });
      },
      [&](FromControl message) -> Task<void> {
        co_await match(
          std::move(message),
          [&](PostCommit) -> Task<void> {
            co_return;
          },
          [&](ShutdownRequest) -> Task<void> {
            // We won't perform any cleanup. This might be undesirable.
            got_shutdown_request = true;
            co_return;
          });
      });
  }

  auto handle_no_more_input() -> void {
    if (sent_no_more_input) {
      return;
    }
    to_control.send(ToControl::no_more_input);
    sent_no_more_input = true;
  }

  std::unique_ptr<Operator<Input, Output>> op;
  OpReceiver<Input> input;
  OpSender<Output> output;
  Receiver<FromControl> from_control;
  Sender<ToControl> to_control;

  bool got_shutdown_request = false;
  bool sent_no_more_input = false;
};

template <class Output>
class SourceRunner {
public:
  SourceRunner(std::unique_ptr<Operator<void, Output>> op,
               OpReceiver<void> input, OpSender<Output> output,
               Receiver<FromControl> from_control, Sender<ToControl> to_control)
    : op{std::move(op)},
      input{std::move(input)},
      output{std::move(output)},
      from_control{std::move(from_control)},
      to_control{std::move(to_control)} {
  }

  auto run_to_completion() -> Task<void> {
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
    auto ctx = AsyncCtx{};
    auto message = co_await from_queue_.receive();
    co_await match(
      message,
      [&](std::any message) -> Task<void> {
        // The task provided by the inner implementation completed.
        if (message.has_value()) {
          // The source wants to continue.
          auto push = OpPush<Output>{output};
          co_await op->process(std::move(message), push, ctx);
          co_await enqueue(op->next());
        } else {
          // The source is done.
          co_await output.send(Signal::end_of_data);
          to_control.send(ToControl::ready_for_shutdown);
          // Do must not enqueue the source again.
        }
      },
      [&](OperatorMessage<void> message) -> Task<void> {
        co_await match(message, [&](Signal signal) -> Task<void> {
          switch (signal) {
            case Signal::end_of_data:
              TENZIR_UNREACHABLE();
            case Signal::checkpoint:
              co_await op->checkpoint();
              co_return;
          }
          TENZIR_UNREACHABLE();
        });
        co_await enqueue(input.receive());
      },
      [&](FromControl message) -> Task<void> {
        co_await match(
          message,
          [&](PostCommit) -> Task<void> {
            co_await op->post_commit();
          },
          [&](ShutdownRequest) -> Task<void> {
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
                    to_queue_.send(co_await std::move(task));
                  })));
  }

  std::unique_ptr<Operator<void, Output>> op;
  OpReceiver<void> input;
  OpSender<Output> output;
  Receiver<FromControl> from_control;
  Sender<ToControl> to_control;

  bool got_shutdown_request = false;

  using QueueItem = variant<std::any, OperatorMessage<void>, FromControl>;

  Receiver<QueueItem> from_queue_;
  Sender<QueueItem> to_queue_;
  folly::coro::CancellableAsyncScope scope_;
};

template <class Input, class Output>
auto run_operator(std::unique_ptr<Operator<Input, Output>> op,
                  OpReceiver<Input> input, OpSender<Output> output,
                  Receiver<FromControl> from_control,
                  Sender<ToControl> to_control) -> Task<void> {
  if constexpr (std::same_as<Input, void>) {
    co_await SourceRunner<Output>{
      std::move(op),           std::move(input),      std::move(output),
      std::move(from_control), std::move(to_control),
    }
      .run_to_completion();
  } else {
    co_await NonSourceRunner<Input, Output>{
      std::move(op),           std::move(input),      std::move(output),
      std::move(from_control), std::move(to_control),
    }
      .run_to_completion();
  }
}

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain, OpReceiver<Input> input,
               OpSender<Output> output) -> Task<void> {
#if 1
  // TODO: Just pretend that we have this case.
  TENZIR_ASSERT(chain.size() == 2);
  auto operators = std::move(chain).unwrap();
  auto first = as<std::unique_ptr<Operator<Input, table_slice>>>(
    std::move(operators[0]));
  auto second = as<std::unique_ptr<Operator<table_slice, Output>>>(
    std::move(operators[1]));
  auto channel = std::make_shared<OpChannel<table_slice>>(10);
  auto sender = OpSender<table_slice>{channel};
  auto receiver = OpReceiver<table_slice>{std::move(channel)};
  auto run_1
    = run_operator(std::move(first), std::move(input), std::move(sender),
                   Receiver<FromControl>{}, Sender<ToControl>{});
  auto run_2
    = run_operator(std::move(second), std::move(receiver), std::move(output),
                   Receiver<FromControl>{}, Sender<ToControl>{});
  co_await folly::coro::collectAll(std::move(run_1), std::move(run_2));
#endif
}

auto run_pipeline(OperatorChain<void, void> pipeline, OpReceiver<void> input,
                  OpSender<void> output) -> Task<void> {
  return run_chain(std::move(pipeline), std::move(input), std::move(output));
  // auto first = std::unique_ptr<TransformationOperator>{};
  // auto second = std::unique_ptr<TransformationOperator>{};
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
