//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/test/nova.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/ast.hpp>

#include <folly/coro/BlockingWait.h>

#include <chrono>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

class SinkCtx final : public test::NovaOpCtx {
public:
  SinkCtx(diagnostic_handler& dh, registry const& reg,
          Option<CheckpointSettings> checkpoints = None{})
    : NovaOpCtx{dh, reg}, checkpoints_{std::move(checkpoints)} {
  }

  auto make_counter(MetricsLabel, MetricsDirection, MetricsVisibility,
                    MetricsUnit) -> MetricsCounter override {
    return {};
  }

  auto checkpoint_settings() const
    -> Option<CheckpointSettings const&> override {
    if (checkpoints_) {
      return *checkpoints_;
    }
    return None{};
  }

private:
  Option<CheckpointSettings> checkpoints_;
};

auto make_sink(std::string_view name, std::string endpoint,
               Option<ast::expression> prefix = None{})
  -> Box<Operator<nova::Events, void>> {
  auto const* plugin = plugins::find<OperatorPlugin>(name);
  REQUIRE(plugin);
  auto desc = plugin->describe();
  auto args = desc.make_args();
  as<std::function<void(Any&, located<std::string>)>>(
    desc.positional.front().setter)(args, located{std::move(endpoint),
                                                  location::unknown});
  for (auto const& named : desc.named) {
    if (named.names.front() == "encoding") {
      as<std::function<void(Any&, located<std::string>)>>(
        named.setter)(args, located{std::string{"json"}, location::unknown});
    } else if (prefix and named.names.front() == "prefix") {
      as<std::function<void(Any&, ast::expression)>>(
        named.setter)(args, std::move(*prefix));
    }
  }
  for (auto& spawn : desc.spawns) {
    if (auto* create = try_as<Spawn<nova::Events, void>>(spawn)) {
      return (*create)(std::move(args));
    }
  }
  FAIL("ZeroMQ event sink is not registered");
}

auto one_event() -> nova::Events {
  return nova::Events{nova::Array<nova::Record>::make_empty(1),
                      nova::storage::BitMap{1, true},
                      nova::Events::Meta::make_empty(1)};
}

} // namespace

TEST("ZeroMQ sinks reject checkpointing before processing input") {
  for (auto name : {"to_zmq", "serve_zmq"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto sink = make_sink(name, "inproc://checkpoint-rejected");
    auto ctx
      = SinkCtx{dh, reg, CheckpointSettings{std::chrono::seconds{1}, false}};
    folly::coro::blockingWait(sink->start(ctx));
    REQUIRE_EQUAL(sink->state(), OperatorState::done);
    auto input = one_event();
    REQUIRE_EQUAL(input.active_count(), 1);
    // Still safe when the handler merely collects the startup diagnostic.
    folly::coro::blockingWait(sink->process(std::move(input), ctx));
    CHECK_EQUAL(sink->state(), OperatorState::done);
    auto diagnostics = std::move(dh).collect();
    REQUIRE_EQUAL(diagnostics.size(), size_t{1});
    CHECK_EQUAL(diagnostics.front().severity, severity::error);
    CHECK_EQUAL(diagnostics.front().message,
                "ZeroMQ sinks do not support checkpointing");
  }
}

TEST("ZeroMQ sinks do not process input after prefix preparation fails") {
  for (auto name : {"to_zmq", "serve_zmq"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto call = ast::function_call{
      ast::entity{std::vector{ast::identifier{"secret", location::unknown}}},
      {ast::expression{ast::constant::make(
        located<data>{data{std::string{"prefix"}}, location::unknown})}},
      location::unknown,
      false};
    call.fn.ref
      = entity_path{std::string{entity_pkg_std}, {"secret"}, entity_ns::fn};
    auto sink = make_sink(name, "inproc://prefix-startup",
                          ast::expression{std::move(call)});
    auto ctx = SinkCtx{dh, reg};
    folly::coro::blockingWait(sink->start(ctx));
    REQUIRE_EQUAL(sink->state(), OperatorState::done);
    folly::coro::blockingWait(sink->process(one_event(), ctx));
    CHECK_EQUAL(sink->state(), OperatorState::done);
    auto diagnostics = std::move(dh).collect();
    REQUIRE_EQUAL(diagnostics.size(), size_t{1});
    CHECK_EQUAL(diagnostics.front().severity, severity::error);
    CHECK_EQUAL(diagnostics.front().message,
                "secret resolution is not available in this test");
  }
}

TEST("ZeroMQ sinks do not process input after socket startup fails") {
  for (auto name : {"to_zmq", "serve_zmq"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto sink = make_sink(name, "invalid://socket-startup");
    auto ctx = SinkCtx{dh, reg};
    folly::coro::blockingWait(sink->start(ctx));
    REQUIRE_EQUAL(sink->state(), OperatorState::done);
    folly::coro::blockingWait(sink->process(one_event(), ctx));
    CHECK_EQUAL(sink->state(), OperatorState::done);
    auto diagnostics = std::move(dh).collect();
    REQUIRE_EQUAL(diagnostics.size(), size_t{1});
    CHECK_EQUAL(diagnostics.front().severity, severity::error);
    CHECK_EQUAL(diagnostics.front().message, "failed to open ZeroMQ socket");
  }
}
