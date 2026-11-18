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
#include <string_view>
#include <thread>

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
  /// Number of subpipelines that stall until `release`.
  Atomic<size_t> stalled{0};
  Notify stalled_changed;
  /// Bytes that reached subpipelines which do not stall.
  Atomic<size_t> delivered{0};
  Notify delivered_changed;
};

/// A listener that accepts one end of socket pairs queued by the test.
struct FakeAccept {
  struct Args {
    Arc<AcceptControl> control;
    located<ir::pipeline> pipeline;
    uint64_t max_connections = 1;
  };

  struct AcceptedInfo {
    Arc<folly::coro::Transport> transport;
  };

  struct ConnectionState {};

  explicit FakeAccept(Args args) : args_{std::move(args)} {
  }

  auto max_connections() const -> uint64_t {
    return args_.max_connections;
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

/// Stalls until the test releases it if its connection starts with `stall`,
/// and otherwise counts the bytes it receives.
template <class Output>
class StallSub final : public Operator<chunk_ptr, Output> {
public:
  explicit StallSub(Arc<AcceptControl> control) : control_{std::move(control)} {
  }

  auto process(chunk_ptr chunk, Push<Output>&, OpCtx&) -> Task<void> override {
    if (not stalls_) {
      auto bytes = std::string_view{
        reinterpret_cast<char const*>(chunk->data()), chunk->size()};
      stalls_ = bytes.starts_with("stall");
      if (*stalls_) {
        control_->stalled.fetch_add(1, std::memory_order_relaxed);
        control_->stalled_changed.notify_one();
        co_await control_->release.wait();
      }
    }
    if (not *stalls_) {
      control_->delivered.fetch_add(chunk->size(), std::memory_order_relaxed);
      control_->delivered_changed.notify_one();
    }
  }

private:
  Arc<AcceptControl> control_;
  Option<bool> stalls_;
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

template <template <class> class Accept, class Output,
          template <class> class Sub = GateSub>
auto accept_plan(Arc<AcceptControl> control, uint64_t max_connections = 1)
  -> ir::Plan {
  auto sub = ir::pipeline{};
  sub.operators.push_back(TestIr{tag_v<Output>, [control]() -> AnyOperator {
                                   return Box<Operator<chunk_ptr, Output>>{
                                     Sub<Output>{control}.with_name("sub")};
                                 }});
  auto args = FakeAccept::Args{
    .control = control,
    .pipeline = located{std::move(sub), location::unknown},
    .max_connections = max_connections,
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

/// Queues a connection and returns the peer's end of it.
auto connect(AcceptControl& control) -> int {
  int fds[2];
  REQUIRE_EQUAL(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  control.pending.enqueue(fds[0]);
  return fds[1];
}

/// Writes all of `data` to `fd`, then closes it.
auto send_and_close(int fd, std::string_view data) -> void {
  while (not data.empty()) {
    auto written = ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
    if (written <= 0) {
      break;
    }
    data.remove_prefix(static_cast<size_t>(written));
  }
  ::close(fd);
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

auto check_stalled_connection_does_not_block_others() -> void {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  auto control = Arc<AcceptControl>{std::in_place};
  auto diagnostics = TestDiagnostics{};
  auto graceful_stop = Notify{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto run = scope.spawn(
      run_plan(accept_plan<StreamAccept, nova::Events, StallSub>(control, 2),
               reg, system, diagnostics, NoProfiler{}, false, &graceful_stop));
    // The first connection sends more data than any buffer between the
    // operator and the stalled subpipeline can hold.
    auto stalled = std::string(64 * 1024 * 1024, 'x');
    stalled.replace(0, 5, "stall");
    auto stalled_writer = std::thread{[fd = connect(*control), &stalled] {
      send_and_close(fd, stalled);
    }};
    co_await wait_until(control->stalled, control->stalled_changed, 1);
    // Give the operator time to fill every buffer in front of the stalled
    // subpipeline before the second connection arrives.
    co_await folly::coro::sleep(std::chrono::milliseconds{500});
    // The second connection must still reach its subpipeline.
    auto other = std::string_view{"hello"};
    send_and_close(connect(*control), other);
    co_await wait_until(control->delivered, control->delivered_changed,
                        other.size());
    CHECK_EQUAL(control->delivered.load(std::memory_order_relaxed),
                other.size());
    control->release.notify_one();
    stalled_writer.join();
    graceful_stop.notify_one();
    CHECK(not(co_await run.join()).is_error());
  }));
}

} // namespace

TEST("accept keeps reading other connections while one stalls") {
  check_stalled_connection_does_not_block_others();
}

TEST("accept holds the connection slot until the subpipeline finishes") {
  check_slot_held_until_subpipeline_finishes<StreamAccept, nova::Events>();
}

TEST("legacy accept holds the connection slot until the subpipeline "
     "finishes") {
  check_slot_held_until_subpipeline_finishes<legacy::StreamAccept,
                                             table_slice>();
}

} // namespace tenzir
