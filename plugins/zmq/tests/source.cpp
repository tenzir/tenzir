//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "zmq/source_message.hpp"

#include <tenzir/async/scope.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/test/nova.hpp>
#include <tenzir/test/test.hpp>

#include <folly/coro/BlockingWait.h>
#include <folly/executors/ManualExecutor.h>

#include <functional>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;

namespace {

using plugins::zmq::SourceMessage;

class SourceCtx final : public test::NovaOpCtx {
public:
  SourceCtx(diagnostic_handler& dh, registry const& reg, AsyncScope& scope)
    : NovaOpCtx{dh, reg}, scope_{scope} {
  }

  auto spawn_task(Task<void> task) -> AsyncHandle<void> override {
    // Hold the reader, and therefore the channel's sender, until the test
    // explicitly lets it observe the stop request and close the channel.
    reader.emplace(std::move(task));
    return scope_.spawn([]() -> Task<void> {
      co_return;
    });
  }

  auto make_counter(MetricsLabel, MetricsDirection, MetricsVisibility,
                    MetricsUnit) -> MetricsCounter override {
    return {};
  }

  auto spawn_sub(SubKey, ir::Plan, DiagnosticBehavior, bool)
    -> Task<AnySubHandle&> override {
    // Observe each accepted payload reaching its parser. No running parser is
    // needed to test the source lifecycle, so cancel at this boundary.
    ++parser_spawns;
    co_yield folly::coro::co_stopped_may_throw;
    TENZIR_UNREACHABLE();
  }

  Option<Task<void>> reader;
  size_t parser_spawns = 0;

private:
  AsyncScope& scope_;
};

class UnusedPush final : public Push<nova::Events> {
public:
  auto operator()(nova::Events) -> Task<void> override {
    FAIL("the source without a publisher must not produce events");
    co_return;
  }
};

auto make_source(std::string_view name, Option<ir::pipeline> parser = None{})
  -> Box<Operator<void, nova::Events>> {
  auto const* plugin = plugins::find<OperatorPlugin>(name);
  REQUIRE(plugin);
  auto desc = plugin->describe();
  auto args = desc.make_args();
  as<std::function<void(Any&, located<std::string>)>>(
    desc.positional.front().setter)(
    args, located{std::string{"inproc://stop-"} + std::string{name},
                  location::unknown});
  if (parser) {
    REQUIRE(desc.pipeline);
    REQUIRE(desc.pipeline->setter);
    (*desc.pipeline->setter)(args,
                             located{std::move(*parser), location::unknown});
  }
  for (auto& spawn : desc.spawns) {
    if (auto* create = try_as<Spawn<void, nova::Events>>(spawn)) {
      return (*create)(std::move(args));
    }
  }
  FAIL("Nova ZeroMQ source is not registered");
}

} // namespace

TEST("ZeroMQ sources stop reading after parser planning fails") {
  for (auto name : {"from_zmq", "accept_zmq"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto parser = ir::pipeline{};
    // A let referencing input cannot be initialized during planning.
    parser.lets.emplace_back(ast::identifier{"value", location::unknown},
                             ast::expression{ast::root_field{
                               ast::identifier{"input", location::unknown}}},
                             let_id{1});
    auto source = make_source(name, std::move(parser));
    auto push = UnusedPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = SourceCtx{dh, reg, scope};
      co_await source->start(ctx);
      REQUIRE(ctx.reader);
      auto payload = chunk::copy(std::string_view{"rejected"});
      auto result
        = Any{Option<SourceMessage>{SourceMessage{std::move(payload)}}};
      co_await source->process_task(std::move(result), push, ctx);
      CHECK_EQUAL(ctx.parser_spawns, size_t{0});
      CHECK_EQUAL(source->state(), OperatorState::normal);
      // A cancelled reader completes without polling. Drive only ready work,
      // so this detects missing cancellation without a timeout or publisher.
      auto executor = folly::ManualExecutor{};
      auto reader = folly::coro::co_withExecutor(&executor,
                                                 std::move(ctx.reader).unwrap())
                      .start();
      executor.drain();
      CHECK(reader.isReady());
      // Also clean up an uncancelled reader when running the regression red.
      co_await source->stop(ctx);
      std::move(reader).via(&executor).getVia(&executor);
      CHECK_EQUAL(source->state(), OperatorState::normal);
      auto eof = co_await source->await_task(dh);
      co_await source->process_task(std::move(eof), push, ctx);
      CHECK_EQUAL(source->state(), OperatorState::done);
    }));
    auto diagnostics = std::move(dh).collect();
    REQUIRE_EQUAL(diagnostics.size(), size_t{1});
    CHECK_EQUAL(diagnostics.front().severity, severity::error);
  }
}

TEST("Nova ZeroMQ sources drain pending results after stop") {
  for (auto name : {"from_zmq", "accept_zmq"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto source = make_source(name);
    auto push = UnusedPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = SourceCtx{dh, reg, scope};
      co_await source->start(ctx);
      REQUIRE(ctx.reader);
      CHECK_EQUAL(source->state(), OperatorState::normal);
      // Simulate receive results already accepted before stop, but waiting for
      // the executor to process them. This requires no live publisher.
      auto pending = std::vector<Any>{};
      for (auto i = 0; i < 3; ++i) {
        auto payload = chunk::copy(std::string_view{"buffered"});
        pending.emplace_back(
          Option<SourceMessage>{SourceMessage{std::move(payload)}});
      }
      co_await source->stop(ctx);
      CHECK_EQUAL(source->state(), OperatorState::normal);
      for (auto& result : pending) {
        auto parsed = co_await catch_cancellation(
          source->process_task(std::move(result), push, ctx));
        CHECK(not parsed);
      }
      CHECK_EQUAL(ctx.parser_spawns, pending.size());
      co_await std::move(ctx.reader).unwrap();
      // Even after the sender exits, completion must wait until the receiver
      // observes end-of-stream, after any buffered payloads have been drained.
      CHECK_EQUAL(source->state(), OperatorState::normal);
      auto result = co_await source->await_task(dh);
      co_await source->process_task(std::move(result), push, ctx);
      CHECK_EQUAL(source->state(), OperatorState::done);
    }));
    CHECK(dh.empty());
  }
}
