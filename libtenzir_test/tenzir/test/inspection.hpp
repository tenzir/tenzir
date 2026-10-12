//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/scope.hpp"
#include "tenzir/pipeline_observer.hpp"

#include <folly/coro/BlockingWait.h>

namespace tenzir::test {

class InspectionFixture {
public:
  explicit InspectionFixture(uuid process, InspectionOptions options = {})
    : observer_{process, options} {
  }
  auto start(PipeId execution) -> void {
    observer_.start(std::move(execution));
  }
  auto set_sources(SourceMap sources) -> void {
    observer_.publish_sources(std::move(sources));
  }
  auto register_plan(Arc<ir::Plan const> plan,
                     Option<Ref<PlanRegistration>> parent = None{},
                     size_t parent_operator = 0) -> Box<PlanRegistration> {
    return observer_.register_plan(std::move(plan), parent, parent_operator);
  }
  auto subscribe_graph() -> GraphSubscription {
    return observer_.subscribe_graph().unwrap();
  }
  auto subscribe_channel(ChannelTarget target)
    -> Result<TapSubscription, TapSubscriptionError> {
    return observer_.subscribe_channel(std::move(target));
  }
  auto finish(ExecutionOutcome outcome) -> void {
    observer_.finish(outcome);
  }

private:
  PipelineObserver observer_;
};

} // namespace tenzir::test
