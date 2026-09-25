//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/aggregation.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/registry.hpp"

#include <folly/coro/BlockingWait.h>

#include <utility>
#include <vector>

namespace tenzir::test {

/// An operator context that offers only what expression preparation needs:
/// diagnostics, the registry, and secret resolution. Everything else panics.
/// Secret resolution fails by default; tests that need it derive and override
/// `resolve_secrets`.
class NovaOpCtx : public OpCtx {
public:
  NovaOpCtx(diagnostic_handler& dh, registry const& reg) : dh_{dh}, reg_{reg} {
  }

  auto dh() -> diagnostic_handler& override {
    return dh_;
  }
  auto reg() -> registry const& override {
    return reg_;
  }
  auto resolve_secrets(std::vector<secret_request> requests)
    -> Task<failure_or<void>> override {
    TENZIR_ASSERT(not requests.empty());
    diagnostic::error("secret resolution is not available in this test")
      .primary(requests.front().location)
      .emit(dh_);
    co_return failure::promise();
  }
  auto actor_system() -> caf::actor_system& override {
    panic("unused");
  }
  auto spawn_sub(SubKey, ir::Plan, DiagnosticBehavior, bool)
    -> Task<AnySubHandle&> override {
    panic("unused");
  }
  auto get_sub(SubKeyView) -> Option<AnySubHandle&> override {
    panic("unused");
  }
  auto io_executor() -> folly::Executor::KeepAlive<folly::IOExecutor> override {
    panic("unused");
  }
  auto spawn_task(Task<void>) -> AsyncHandle<void> override {
    panic("unused");
  }
  auto save_checkpoint(chunk_ptr) -> Task<void> override {
    panic("unused");
  }
  auto load_checkpoint() -> Task<chunk_ptr> override {
    panic("unused");
  }
  auto flush() -> Task<void> override {
    panic("unused");
  }
  auto make_counter(MetricsLabel, MetricsDirection, MetricsVisibility,
                    MetricsUnit) -> MetricsCounter override {
    panic("unused");
  }
  auto metrics_receiver() const -> metrics_receiver_actor override {
    panic("unused");
  }
  auto is_hidden() const -> bool override {
    return false;
  }
  auto has_terminal() const -> bool override {
    return false;
  }
  auto checkpoint_settings() const
    -> Option<CheckpointSettings const&> override {
    return None{};
  }

private:
  diagnostic_handler& dh_;
  registry const& reg_;
};

/// Prepares `expression` the way an operator does, driving the asynchronous
/// constructor to completion with a minimal operator context.
inline auto make_evaluator(ast::expression expression, diagnostic_handler& dh,
                           registry const& reg) -> failure_or<nova::Evaluator> {
  auto ctx = NovaOpCtx{dh, reg};
  return folly::coro::blockingWait(
    nova::Evaluator::make(std::move(expression), ctx));
}

/// As `make_evaluator`, for an aggregation call.
inline auto make_aggregation(ast::expression expression, diagnostic_handler& dh,
                             registry const& reg)
  -> failure_or<Box<nova::AggregationInstance>> {
  auto ctx = NovaOpCtx{dh, reg};
  return folly::coro::blockingWait(
    nova::AggregationInstance::make(std::move(expression), ctx));
}

} // namespace tenzir::test
