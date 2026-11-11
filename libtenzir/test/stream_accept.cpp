//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/operator/stream_accept.hpp"

#include "tenzir/async.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator/stream_accept_legacy.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/registry.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <sys/socket.h>

#include <unistd.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <functional>

namespace tenzir {
namespace {

/// Shared state between the test body and the operators under test.
struct AcceptControl {
  /// File descriptors that the fake listener hands out as accepted connections.
  folly::coro::UnboundedQueue<int> pending;
  Atomic<size_t> accepted{0};
  Notify accepted_changed;
  /// Number of subpipelines that reached `finalize()` and wait for `release`.
  Atomic<size_t> gated{0};
  Notify gated_changed;
  Notify release;
};

/// A listener that accepts one end of socket pairs queued by the test.
struct FakeAccept {
  struct Args {
    Arc<AcceptControl> control;
    located<ir::pipeline> pipeline;
  };

  struct AcceptedInfo {
    Arc<folly::coro::Transport> transport;
  };

  struct ConnectionState {};

  explicit FakeAccept(Args args) : args_{std::move(args)} {
  }

  auto max_connections() const -> uint64_t {
    return 1;
  }

  auto prepare(OpCtx&) -> Task<bool> {
    co_return true;
  }

  auto start_listener(folly::EventBase*, OpCtx&) -> Task<bool> {
    co_return true;
  }

  auto stop_accepting(folly::EventBase*) -> void {
  }

  auto cleanup() -> void {
  }

  auto accept(folly::EventBase* evb) -> Task<Box<folly::coro::Transport>> {
    auto fd = co_await args_.control->pending.dequeue();
    auto transport = co_await folly::coro::co_withExecutor(
      evb, [evb, fd]() -> Task<Box<folly::coro::Transport>> {
        co_return Box<folly::coro::Transport>{
          std::in_place, evb,
          folly::AsyncSocket::newSocket(evb, folly::NetworkSocket::fromFd(fd))};
      }());
    args_.control->accepted.fetch_add(1, std::memory_order_relaxed);
    args_.control->accepted_changed.notify_one();
    co_return transport;
  }

  auto finish_accept(Box<folly::coro::Transport> transport,
                     folly::CancellationToken, folly::CancellationToken,
                     folly::CancellationToken, diagnostic_handler&)
    -> Task<Option<AcceptedInfo>> {
    co_return AcceptedInfo{Arc<folly::coro::Transport>{std::move(*transport)}};
  }

  auto bind(ir::pipeline&, AcceptedInfo&, OpCtx&) -> void {
  }

  auto pipeline() -> located<ir::pipeline>& {
    return args_.pipeline;
  }

  auto make_connection_state(folly::coro::Transport&, AcceptedInfo&,
                             OpCtx&) const -> ConnectionState {
    return {};
  }

  static auto record_read(ConnectionState&, size_t) -> void {
  }

  static auto close_connection_state(ConnectionState&) -> void {
  }

  auto events_metric_label() const -> MetricsLabel {
    return {"operator", "fake_accept"};
  }

  auto bytes_metric_label(AcceptedInfo const&) const -> MetricsLabel {
    return {"operator", "fake_accept"};
  }

  auto emit_accept_warning(folly::AsyncSocketException const&,
                           diagnostic_handler&) const -> void {
  }

  auto emit_read_warning(uint64_t, std::string const&,
                         diagnostic_handler&) const -> void {
  }

  auto debug_name() const -> std::string_view {
    return "fake_accept";
  }

private:
  Args args_;
};

/// Discards its input and blocks in `finalize()` until the test releases it.
template <class Output>
class GateSub final : public Operator<chunk_ptr, Output> {
public:
  explicit GateSub(Arc<AcceptControl> control) : control_{std::move(control)} {
  }

  auto process(chunk_ptr, Push<Output>&, OpCtx&) -> Task<void> override {
    co_return;
  }

  auto finalize(Push<Output>&, OpCtx&) -> Task<FinalizeBehavior> override {
    control_->gated.fetch_add(1, std::memory_order_relaxed);
    control_->gated_changed.notify_one();
    co_await control_->release.wait();
    co_return FinalizeBehavior::done;
  }

private:
  Arc<AcceptControl> control_;
};

template <class Input>
class DiscardSink final : public Operator<Input, void> {
public:
  auto process(Input, OpCtx&) -> Task<void> override {
    co_return;
  }
};

/// Wraps a runtime operator factory so tests can plan it.
class TestIr final : public ir::Operator {
public:
  TestIr(element_type_tag output, std::function<AnyOperator()> spawn)
    : output_{output}, spawn_{std::move(spawn)} {
  }

  auto name() const -> std::string override {
    return "stream_accept_test";
  }

  auto copy() const -> Box<ir::Operator> override {
    return TestIr{output_, spawn_};
  }

  auto infer_type(element_type_tag, base_ctx) const
    -> failure_or<element_type_tag> override {
    return output_;
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto spawn(element_type_tag, registry const&) const -> AnyOperator override {
    return spawn_();
  }

private:
  element_type_tag output_;
  std::function<AnyOperator()> spawn_;
};

class TestDiagnostics final : public DiagHandler {
public:
  auto emit(diagnostic d) -> void override {
    if (d.severity == severity::error) {
      failed_.store(true, std::memory_order_relaxed);
    }
  }

  auto failure() -> failure_or<void> override {
    if (failed_.load(std::memory_order_relaxed)) {
      return failure::promise();
    }
    return {};
  }

private:
  Atomic<bool> failed_{false};
};

template <template <class> class Accept, class Output>
auto accept_plan(Arc<AcceptControl> control) -> ir::Plan {
  auto sub = ir::pipeline{};
  sub.operators.push_back(
    TestIr{tag_v<Output>, [control]() -> AnyOperator {
             return Box<Operator<chunk_ptr, Output>>{
               GateSub<Output>{control}.with_name("gate")};
           }});
  auto args = FakeAccept::Args{
    .control = control,
    .pipeline = located{std::move(sub), location::unknown},
  };
  auto result = ir::Plan{};
  result.operators.push_back(ir::PlannedOperator{
    .id = OpId{"0"},
    .op = TestIr{tag_v<Output>,
                 [args]() -> AnyOperator {
                   return Box<Operator<void, Output>>{
                     Accept<FakeAccept>{args}.with_name("accept")};
                 }},
    .partition_keys = None{},
    .input = tag_v<void>,
    .output = tag_v<Output>});
  result.operators.push_back(ir::PlannedOperator{
    .id = OpId{"1"},
    .op = TestIr{tag_v<void>,
                 []() -> AnyOperator {
                   return Box<Operator<Output, void>>{
                     DiscardSink<Output>{}.with_name("sink")};
                 }},
    .partition_keys = None{},
    .input = tag_v<Output>,
    .output = tag_v<void>});
  result.channels = {{ir::Port::input, 0, 0, tag_v<void>},
                     {0, 0, 1, tag_v<Output>},
                     {1, 0, ir::Port::output, tag_v<void>}};
  return result;
}

/// Queues a connection whose peer has already hung up.
auto connect_and_close(AcceptControl& control) -> void {
  int fds[2];
  REQUIRE_EQUAL(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  ::close(fds[1]);
  control.pending.enqueue(fds[0]);
}

auto wait_until(Atomic<size_t>& counter, Notify& changed, size_t value)
  -> Task<void> {
  while (counter.load(std::memory_order_relaxed) < value) {
    co_await changed.wait();
  }
}

template <template <class> class Accept, class Output>
auto check_slot_held_until_subpipeline_finishes() -> void {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  auto control = Arc<AcceptControl>{std::in_place};
  auto diagnostics = TestDiagnostics{};
  auto graceful_stop = Notify{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto run
      = scope.spawn(run_plan(accept_plan<Accept, Output>(control), reg, system,
                             diagnostics, NoProfiler{}, false, &graceful_stop));
    // The first connection closes right away, but its subpipeline keeps
    // running until the test releases it.
    connect_and_close(*control);
    co_await wait_until(control->accepted, control->accepted_changed, 1);
    co_await wait_until(control->gated, control->gated_changed, 1);
    // With `max_connections=1`, the subpipeline still holds the only slot, so
    // the second connection must wait. Proving that something does not happen
    // requires giving the operator time to (wrongly) accept it.
    connect_and_close(*control);
    co_await folly::coro::sleep(std::chrono::milliseconds{200});
    CHECK_EQUAL(control->accepted.load(std::memory_order_relaxed), size_t{1});
    // Finishing the first subpipeline frees the slot for the second
    // connection.
    control->release.notify_one();
    co_await wait_until(control->accepted, control->accepted_changed, 2);
    co_await wait_until(control->gated, control->gated_changed, 2);
    control->release.notify_one();
    graceful_stop.notify_one();
    CHECK(not(co_await run.join()).is_error());
  }));
}

} // namespace

TEST("accept holds the connection slot until the subpipeline finishes") {
  check_slot_held_until_subpipeline_finishes<StreamAccept, nova::Events>();
}

TEST("legacy accept holds the connection slot until the subpipeline "
     "finishes") {
  check_slot_held_until_subpipeline_finishes<legacy::StreamAccept,
                                             table_slice>();
}

} // namespace tenzir
