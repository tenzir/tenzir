//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_observer.hpp"

#include "tenzir/async.hpp"
#include "tenzir/async/fused.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/source.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/inspection.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/registry.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Timeout.h>
#include <folly/executors/ManualExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <barrier>
#include <filesystem>
#include <thread>

namespace tenzir {
namespace {

class DescriptionProbe final : public ir::Operator {
public:
  explicit DescriptionProbe(Arc<Atomic<size_t>> descriptions,
                            std::function<void()> describe = {})
    : descriptions_{std::move(descriptions)}, describe_{std::move(describe)} {
  }

  auto name() const -> std::string override {
    if (describe_) {
      describe_();
    }
    descriptions_->fetch_add(1, std::memory_order_relaxed);
    return "probe";
  }

  auto display_name() const -> std::string override {
    return "visible_" + name();
  }

  auto infer_type(element_type_tag input, base_ctx) const
    -> failure_or<element_type_tag> override {
    return input;
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto spawn(element_type_tag, registry const&) const -> AnyOperator override {
    panic("description probes are not executed");
  }

private:
  mutable Arc<Atomic<size_t>> descriptions_;
  std::function<void()> describe_;
};

auto plan(Arc<Atomic<size_t>> descriptions, size_t port = 0,
          element_type_tag type = tag_v<nova::Events>, size_t lanes = 1,
          std::function<void()> describe = {},
          location definition = location::unknown) -> Arc<ir::Plan const> {
  auto result = ir::Plan{};
  result.definition_location = definition;
  result.operators.push_back(ir::PlannedOperator{
    .id = OpId{"0"},
    .op = DescriptionProbe{descriptions, std::move(describe)},
    .parallelism = lanes,
    .partition_keys = None{},
    .input = type,
    .output = type});
  result.operators.push_back(
    ir::PlannedOperator{.id = OpId{"1"},
                        .op = DescriptionProbe{descriptions},
                        .parallelism = lanes,
                        .partition_keys = None{},
                        .input = type,
                        .output = type});
  result.channels.push_back({0, port, 1, type});
  return Arc<ir::Plan const>{std::move(result)};
}

auto batch(int64_t value) -> nova::Events {
  auto data = nova::ArrayBuilder<nova::Record>{};
  data.record().field("value").data(value);
  data.record().field("value").data(value + 1);
  auto mask = nova::storage::BitMap::Builder{};
  mask.emplace_back(false);
  mask.emplace_back(true);
  return {data.finish(), mask.finish(),
          nova::Events::Meta::make_empty(2, "tap.test")};
}

auto snapshot(test::InspectionFixture& observer) -> GraphSnapshot {
  auto graph = observer.subscribe_graph();
  return **folly::coro::blockingWait(graph.recv());
}

auto check_same_graph(GraphSnapshot const& left, GraphSnapshot const& right)
  -> void {
  CHECK_EQUAL(left.process_instance_id, right.process_instance_id);
  CHECK_EQUAL(left.execution_id, right.execution_id);
  CHECK_EQUAL(left.end, right.end);
  REQUIRE_EQUAL(left.graphs.size(), right.graphs.size());
  for (auto const& graph : left.graphs) {
    auto found = std::ranges::find_if(right.graphs, [&](auto const& candidate) {
      return candidate->path == graph->path;
    });
    REQUIRE(found != right.graphs.end());
    auto const& a = *graph;
    auto const& b = **found;
    CHECK_EQUAL(a.parent, b.parent);
    CHECK_EQUAL(a.parent_operator, b.parent_operator);
    CHECK_EQUAL(a.definition_location, b.definition_location);
    CHECK(a.operators == b.operators);
    CHECK(a.channels == b.channels);
  }
}

auto target(GraphSnapshot const& graph, size_t pipeline = 0, size_t edge = 0)
  -> ChannelTarget {
  return {graph.process_instance_id, *graph.execution_id,
          graph.graphs[pipeline]->path, edge};
}

auto drain(TapSubscription& tap, ExecutionOutcome expected
                                 = ExecutionOutcome::completed)
  -> Task<size_t> {
  auto count = size_t{0};
  while (auto message = co_await tap.recv()) {
    if (auto* end = try_as<ExecutionOutcome>(*message)) {
      CHECK_EQUAL(*end, expected);
      CHECK(not co_await tap.recv());
      co_return count;
    }
    ++count;
  }
  FAIL("tap reached EOF without a terminal outcome");
  co_return count;
}

auto count(TapSubscription& tap) -> size_t {
  return folly::coro::blockingWait(drain(tap));
}

struct FlowControl {
  Notify ready;
  Notify produce;
  Atomic<size_t> received{0};
};

class GatedSource final : public Operator<void, nova::Events> {
public:
  GatedSource(Arc<FlowControl> control, bool nested)
    : control_{std::move(control)}, nested_{nested} {
  }

  auto start(OpCtx&) -> Task<void> override {
    if (not nested_) {
      control_->ready.notify_one();
    }
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await control_->produce.wait();
    co_return Any{};
  }

  auto process_task(Any, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    for (auto i = 0; i < 12; ++i) {
      co_await push(batch(i));
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  mutable Arc<FlowControl> control_;
  bool nested_;
  bool done_ = false;
};

class CountingSink final : public Operator<nova::Events, void> {
public:
  explicit CountingSink(Arc<FlowControl> control)
    : control_{std::move(control)} {
  }

  auto process(nova::Events, OpCtx&) -> Task<void> override {
    control_->received.fetch_add(1, std::memory_order_relaxed);
    co_return;
  }

private:
  Arc<FlowControl> control_;
};

class ForwardEvents final : public Operator<nova::Events, nova::Events> {
public:
  auto process(nova::Events events, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    co_await push(std::move(events));
  }
};

class SplitEvents final : public Operator<nova::Events, nova::Events, true> {
public:
  auto process(nova::Events events, PushPorts<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    auto values = events.data.field("value")->data.get_alternative<nova::Int>();
    auto value = *values->data.get(1);
    co_await push(static_cast<size_t>(value % 2), std::move(events));
  }
};

auto tap_values(TapSubscription& tap) -> Task<std::vector<int64_t>> {
  auto result = std::vector<int64_t>{};
  while (auto message = co_await tap.recv()) {
    if (auto* end = try_as<ExecutionOutcome>(*message)) {
      CHECK_EQUAL(*end, ExecutionOutcome::completed);
      CHECK(not co_await tap.recv());
      std::ranges::sort(result);
      co_return result;
    }
    auto& events = as<nova::Events>(as<EventBatch>(*message));
    auto values = events.data.field("value")->data.get_alternative<nova::Int>();
    for (auto i = int64_t{0}; i < events.length(); ++i) {
      if (events.mask.get(i)) {
        result.push_back(*values->data.get(i));
      }
    }
  }
  FAIL("tap reached EOF without a terminal outcome");
  co_return result;
}

class RuntimeIr final : public ir::Operator {
public:
  explicit RuntimeIr(std::function<AnyOperator()> spawn)
    : spawn_{std::move(spawn)} {
  }

  auto name() const -> std::string override {
    return "inspection_test";
  }

  auto copy() const -> Box<ir::Operator> override {
    return RuntimeIr{spawn_};
  }

  auto infer_type(element_type_tag input, base_ctx) const
    -> failure_or<element_type_tag> override {
    return input;
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto spawn(element_type_tag, registry const&) const -> AnyOperator override {
    return spawn_();
  }

private:
  std::function<AnyOperator()> spawn_;
};

auto forward_plan() -> ir::Plan {
  auto result = ir::Plan{};
  for (auto i = 0; i < 2; ++i) {
    result.operators.push_back(
      ir::PlannedOperator{.id = OpId{std::to_string(i)},
                          .op = RuntimeIr{[]() -> AnyOperator {
                            return Box<Operator<nova::Events, nova::Events>>{
                              ForwardEvents{}.with_name("forward")};
                          }},
                          .partition_keys = None{},
                          .input = tag_v<nova::Events>,
                          .output = tag_v<nova::Events>});
  }
  result.channels = {{ir::Port::input, 0, 0, tag_v<nova::Events>},
                     {0, 0, 1, tag_v<nova::Events>, ir::ChannelKind::fused},
                     {1, 0, ir::Port::output, tag_v<nova::Events>}};
  return result;
}

class NestedForward final : public Operator<nova::Events, nova::Events> {
public:
  explicit NestedForward(Arc<FlowControl> control)
    : control_{std::move(control)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await ctx.spawn_sub(int64_t{0}, forward_plan(),
                           DiagnosticBehavior::Unchanged);
    control_->ready.notify_one();
  }

  auto process(nova::Events events, Push<nova::Events>&, OpCtx& ctx)
    -> Task<void> override {
    auto handle = ctx.get_sub(int64_t{0});
    TENZIR_ASSERT(handle);
    auto accepted
      = co_await as<SubHandle<nova::Events>>(*handle).push(std::move(events));
    TENZIR_ASSERT(accepted.is_ok());
  }

private:
  Arc<FlowControl> control_;
};

auto flow_plan(Arc<FlowControl> control, ir::ChannelKind kind, bool nested)
  -> ir::Plan {
  auto result = ir::Plan{};
  result.operators.push_back(
    ir::PlannedOperator{.id = OpId{"0"},
                        .op = RuntimeIr{[control, nested]() -> AnyOperator {
                          return Box<Operator<void, nova::Events>>{
                            GatedSource{control, nested}.with_name("source")};
                        }},
                        .partition_keys = None{},
                        .input = tag_v<void>,
                        .output = tag_v<nova::Events>});
  result.operators.push_back(
    ir::PlannedOperator{.id = OpId{"1"},
                        .op = RuntimeIr{[control, nested]() -> AnyOperator {
                          if (nested) {
                            return Box<Operator<nova::Events, nova::Events>>{
                              NestedForward{control}.with_name("nested")};
                          }
                          return Box<Operator<nova::Events, nova::Events>>{
                            ForwardEvents{}.with_name("forward")};
                        }},
                        .partition_keys = None{},
                        .input = tag_v<nova::Events>,
                        .output = tag_v<nova::Events>});
  result.operators.push_back(
    ir::PlannedOperator{.id = OpId{"2"},
                        .op = RuntimeIr{[control]() -> AnyOperator {
                          return Box<Operator<nova::Events, void>>{
                            CountingSink{control}.with_name("sink")};
                        }},
                        .partition_keys = None{},
                        .input = tag_v<nova::Events>,
                        .output = tag_v<void>});
  result.channels = {{ir::Port::input, 0, 0, tag_v<void>},
                     {0, 0, 1, tag_v<nova::Events>, kind},
                     {1, 0, 2, tag_v<nova::Events>, kind},
                     {2, 0, ir::Port::output, tag_v<void>}};
  return result;
}

class TestDiagnostics final : public DiagHandler {
public:
  auto emit(diagnostic) -> void override {
    failed_.store(true, std::memory_order_relaxed);
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

} // namespace

TEST("inspection retains compilation sources and package call sites") {
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto source_id = SourceId{};
  auto caller = location{};
  auto callsite_id = CallSiteId{};
  {
    auto sources = SourceMap{};
    auto root = Source::new_source("acme::inner", "<pipeline>", false);
    auto package = Source::new_source("x = 1", "<packages/acme:inner>", false);
    source_id = package->index;
    caller = location{0, 11, root->index, 0};
    sources.add_source(root);
    sources.add_source(package);
    callsite_id = sources.add_call_site(caller);
    observer.set_sources(sources);
    auto current = snapshot(observer);
    require(current.sources.is_some());
    // Later changes to the caller's map must not affect the snapshot.
    require((*current.sources)->source(source_id).is_some());
    check_eq((*current.sources)->source(source_id)->text, package->text);
    sources.add_source(Source::new_source("later", "<later>", false));
    check_eq((*current.sources)->sources().size(), 2u);
  }
  observer.finish(ExecutionOutcome::completed);
  auto final = snapshot(observer);
  require(final.sources.is_some());
  auto const& sources = **final.sources;
  auto package = sources.source(source_id);
  require(package.is_some());
  check_eq(package->origin, "<packages/acme:inner>");
  check_eq(package->text, "x = 1");
  require(sources.call_site(callsite_id).is_some());
  check_eq(*sources.call_site(callsite_id), caller);
}

TEST("inspection stays lazy and discovers only live and future plans") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  {
    auto ended = observer.register_plan(plan(descriptions, 7));
  }
  auto current = observer.register_plan(plan(descriptions));
  check_eq(descriptions->load(std::memory_order_relaxed), 0u);
  auto first = snapshot(observer);
  check_eq(first.graphs.size(), 1u);
  check_eq(first.graphs[0]->operators[0].name, "visible_probe");
  check_eq(first.graphs[0]->channels[0].from_port, 0u);
  auto second = snapshot(observer);
  check_eq(first.graphs[0]->path, second.graphs[0]->path);
}

TEST("replacing a graph subscription detaches the old execution") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto first = test::InspectionFixture{uuid::null()};
  auto second = test::InspectionFixture{uuid::null()};
  first.start(PipeId{"first"});
  second.start(PipeId{"second"});
  auto graph = first.subscribe_graph();
  {
    auto replacement = second.subscribe_graph();
    graph = std::move(replacement);
  }
  auto unobserved = first.register_plan(plan(descriptions));
  check_eq(descriptions->load(std::memory_order_relaxed), 0u);
  auto initial = folly::coro::blockingWait(graph.recv());
  require(initial.is_some());
  check_eq((*initial)->execution_id, PipeId{"second"});
  auto observed = Option{second.register_plan(plan(descriptions))};
  observed = None{};
  second.finish(ExecutionOutcome::completed);
  auto final = folly::coro::blockingWait(graph.recv());
  require(final.is_some());
  check_eq((*final)->graphs.size(), 1u);
  check_eq((*final)->end, ExecutionOutcome::completed);
  check(not folly::coro::blockingWait(graph.recv()));
}

TEST("inspection preserves parent paths after detaching") {
  auto parent_descriptions = Arc{Atomic<size_t>{0}};
  auto child_descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto graph = Option{observer.subscribe_graph()};
  auto root = Option{observer.register_plan(plan(parent_descriptions))};
  auto ancestry = Ref{**root};
  auto children = std::vector<Box<PlanRegistration>>{};
  for (auto i = 0; i < 64; ++i) {
    children.push_back(
      observer.register_plan(plan(child_descriptions), ancestry));
  }
  auto before = snapshot(observer);
  require_eq(before.graphs.size(), 2u);
  graph = None{};
  graph = observer.subscribe_graph();
  auto after = snapshot(observer);
  require_eq(after.graphs.size(), 2u);
  for (auto const& graph : before.graphs) {
    check(std::ranges::any_of(after.graphs, [&](auto const& candidate) {
      return candidate->path == graph->path;
    }));
  }
  // New children still resolve to the same parent after reconnecting.
  auto later = observer.register_plan(plan(child_descriptions, 1), ancestry);
  auto current = snapshot(observer);
  require_eq(current.graphs.size(), 3u);
  auto root_path = std::ranges::find_if(before.graphs, [](auto const& graph) {
    return not graph->parent;
  });
  require(root_path != before.graphs.end());
  auto added = std::ranges::find_if(current.graphs, [](auto const& graph) {
    return graph->channels[0].from_port == 1;
  });
  require(added != current.graphs.end());
  check_eq((*added)->parent, (*root_path)->path);
}

TEST("inspection aggregates lanes and separates nested structures and ports") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto graph = observer.subscribe_graph();
  auto root = observer.register_plan(plan(descriptions));
  auto different = observer.register_plan(plan(descriptions, 1), Ref{*root}, 0);
  auto one = observer.register_plan(plan(descriptions), Ref{*root}, 0);
  auto parallel = observer.register_plan(
    plan(descriptions, 0, tag_v<nova::Events>, 8), Ref{*root}, 0);
  auto another_parent
    = observer.register_plan(plan(descriptions), Ref{*root}, 1);
  auto current = snapshot(observer);
  check_eq(current.graphs.size(), 4u);
  // Repeated physical lanes share one graph; the distinct port and parent
  // operator each introduce their own nested graph.
  auto placements = std::vector<std::pair<size_t, size_t>>{};
  for (auto const& graph : current.graphs) {
    if (graph->parent) {
      placements.emplace_back(*graph->parent_operator,
                              graph->channels[0].from_port);
    }
  }
  std::ranges::sort(placements);
  check_eq(placements,
           (std::vector<std::pair<size_t, size_t>>{{0, 0}, {0, 1}, {1, 0}}));
}

TEST("inspection distinguishes definitions when operator locations are "
     "unknown") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto definition = [&](location source) {
    return plan(descriptions, 0, tag_v<nova::Events>, 1, {}, source);
  };
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(descriptions));
  auto first = observer.register_plan(definition({0, 1, 1, 0}), Ref{*root});
  auto repeated = observer.register_plan(definition({0, 1, 1, 0}), Ref{*root});
  auto other = observer.register_plan(definition({2, 3, 1, 0}), Ref{*root});
  auto current = snapshot(observer);
  check_eq(current.graphs.size(), 3u);
}

TEST("inspection taps preserve native batches and independent queue limits") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto registration = Option{observer.register_plan(plan(descriptions))};
  auto discovered = snapshot(observer);
  auto slow = observer.subscribe_channel(target(discovered)).unwrap();
  auto fast = observer.subscribe_channel(target(discovered)).unwrap();
  for (auto i = int64_t{0}; i < 12; ++i) {
    auto events = batch(i);
    (*registration)->tap(0)->capture(events);
    auto read = folly::coro::blockingWait(fast.recv());
    require(read.is_some());
    auto* copied_batch = try_as<EventBatch>(*read);
    require(copied_batch);
    auto* copied = try_as<nova::Events>(*copied_batch);
    require(copied);
    check_eq(copied->length(), 2);
    check_eq(copied->active_count(), 1);
    check(not copied->mask.get(0));
    check(copied->mask.get(1));
    check_eq(*copied->meta.name.get(1), "tap.test");
    auto values
      = copied->data.field("value")->data.get_alternative<nova::Int>();
    check_eq(*values->data.get(1), i + 1);
  }
  registration = None{};
  observer.finish(ExecutionOutcome::completed);
  check_eq(count(slow), 4u);
  check_eq(count(fast), 0u);
}

TEST("inspection addresses nested channels by their complete exact path") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto graph = observer.subscribe_graph();
  auto root = Option{observer.register_plan(plan(descriptions))};
  auto parent_a
    = Option{observer.register_plan(plan(descriptions, 1), Ref{**root}, 0)};
  auto parent_b
    = Option{observer.register_plan(plan(descriptions, 2), Ref{**root}, 0)};
  auto child_a
    = Option{observer.register_plan(plan(descriptions, 3), Ref{**parent_a}, 0)};
  auto child_b
    = Option{observer.register_plan(plan(descriptions, 3), Ref{**parent_b}, 0)};
  auto current = snapshot(observer);
  auto children = std::vector<PlanPath>{};
  for (auto const& pipeline : current.graphs) {
    if (pipeline->channels[0].from_port == 3) {
      children.push_back(pipeline->path);
    }
  }
  require_eq(children.size(), 2u);
  auto path_a = children[0];
  auto path_b = children[1];
  require_eq(path_a.size(), 5u);
  require_eq(path_b.size(), 5u);
  check(path_a.front() == path_b.front());
  check_eq(path_a[1], 0u);
  check_eq(path_b[1], 0u);
  check(path_a[2] != path_b[2]);
  check_eq(path_a[3], 0u);
  check_eq(path_b[3], 0u);
  check(path_a.back() != path_b.back());
  // Reconstruct each target from integer path values, without a graph object.
  // The identical leaf descriptions under different parents stay separate.
  auto target_a = ChannelTarget{uuid::null(), PipeId{"root"}, path_a, 0};
  auto target_b = ChannelTarget{uuid::null(), PipeId{"root"}, path_b, 0};
  auto tap_a = observer.subscribe_channel(target_a).unwrap();
  auto tap_b = observer.subscribe_channel(target_b).unwrap();
  (*child_a)->tap(0)->capture(batch(10));
  (*child_b)->tap(0)->capture(batch(20));
  auto malformed = target_a;
  malformed.path.clear();
  check_eq(observer.subscribe_channel(malformed).unwrap_err(),
           TapSubscriptionError::invalid_target);
  malformed = target_a;
  malformed.path.pop_back();
  check_eq(observer.subscribe_channel(malformed).unwrap_err(),
           TapSubscriptionError::invalid_target);
  malformed = target_a;
  malformed.path.back() = target_b.path.back();
  check_eq(observer.subscribe_channel(malformed).unwrap_err(),
           TapSubscriptionError::invalid_target);
  child_b = None{};
  child_a = None{};
  parent_b = None{};
  parent_a = None{};
  root = None{};
  observer.finish(ExecutionOutcome::completed);
  check_eq(folly::coro::blockingWait(tap_values(tap_a)),
           (std::vector<int64_t>{11}));
  check_eq(folly::coro::blockingWait(tap_values(tap_b)),
           (std::vector<int64_t>{21}));
}

TEST("inspection keeps a logical tap across nested gaps and cache eviction") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{
    uuid::null(), InspectionOptions{.finished_pipeline_graphs = 0}};
  observer.start(PipeId{"root"});
  auto root = Option{observer.register_plan(plan(descriptions))};
  auto nested
    = Option{observer.register_plan(plan(descriptions, 3), Ref{**root}, 0)};
  auto graph = observer.subscribe_graph();
  auto current = snapshot(observer);
  auto child = std::ranges::find_if(current.graphs, [](auto const& graph) {
    return graph->parent.is_some();
  });
  require(child != current.graphs.end());
  auto nested_target
    = ChannelTarget{uuid::null(), PipeId{"root"}, (*child)->path, 0};
  auto tap = observer.subscribe_channel(nested_target).unwrap();
  (*nested)->tap(0)->capture(batch(1));
  nested = None{};
  check_eq(snapshot(observer).graphs.size(), 1u);
  auto first = folly::coro::blockingWait(tap.recv());
  require(first.is_some());
  check(is<EventBatch>(*first));
  nested = observer.register_plan(plan(descriptions, 3), Ref{**root}, 0);
  current = snapshot(observer);
  auto returned = std::ranges::find_if(current.graphs, [&](auto const& graph) {
    return graph->path == nested_target.path;
  });
  require(returned != current.graphs.end());
  (*nested)->tap(0)->capture(batch(2));
  nested = None{};
  root = None{};
  observer.finish(ExecutionOutcome::completed);
  check_eq(count(tap), 1u);
}

TEST("inspection publication tolerates retirement and completion during "
     "metadata collection") {
  for (auto [complete, retained] :
       {std::pair{false, 0u}, {true, 0u}, {false, 1u}, {true, 1u}}) {
    auto descriptions = Arc{Atomic<size_t>{0}};
    auto observer = test::InspectionFixture{
      uuid::null(), InspectionOptions{.finished_pipeline_graphs = retained}};
    observer.start(PipeId{"root"});
    auto root = Option{observer.register_plan(plan(descriptions))};
    auto parent
      = Option{observer.register_plan(plan(descriptions), Ref{**root})};
    auto gate = std::barrier{2};
    auto nested = Option{
      observer.register_plan(plan(descriptions, 0, tag_v<nova::Events>, 1,
                                  [&] {
                                    gate.arrive_and_wait();
                                    gate.arrive_and_wait();
                                  }),
                             Ref{**parent})};
    auto graph = Option<GraphSnapshot>{};
    auto subscriber = std::jthread{[&] {
      graph = snapshot(observer);
    }};
    // Retire the child and its cached parent while HTTP discovery holds copies
    // of their plans. It must not dereference either borrowed registration.
    gate.arrive_and_wait();
    nested = None{};
    parent = None{};
    if (complete) {
      root = None{};
      observer.finish(ExecutionOutcome::completed);
    }
    gate.arrive_and_wait();
    subscriber.join();
    require(graph.is_some());
    require_eq(graph->graphs.size(), 1u + retained);
    check(not graph->graphs.front()->parent);
    check_eq(graph->end.is_some(), complete);
  }
}

TEST("inspection publishes complete initial graphs to concurrent readers") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto gate = std::barrier{2};
  auto entered = Atomic<bool>{false};
  auto root
    = observer.register_plan(plan(descriptions, 0, tag_v<nova::Events>, 1, [&] {
        if (not entered.exchange(true, std::memory_order_relaxed)) {
          gate.arrive_and_wait();
          gate.arrive_and_wait();
        }
      }));
  auto first = Option<GraphSubscription>{};
  auto subscriber = std::jthread{[&] {
    first = observer.subscribe_graph();
  }};
  gate.arrive_and_wait();
  auto second = observer.subscribe_graph();
  auto executor = folly::ManualExecutor{};
  auto pending = folly::coro::co_withExecutor(
                   folly::getKeepAliveToken(&executor), second.recv())
                   .start();
  executor.drain();
  auto child = observer.register_plan(plan(descriptions, 1), Ref{*root});
  executor.drain();
  auto published_during_initialization = pending.isReady();
  gate.arrive_and_wait();
  subscriber.join();
  check(not published_during_initialization);
  auto current = std::move(pending).via(&executor).getVia(&executor).unwrap();
  auto other = folly::coro::blockingWait(first->recv()).unwrap();
  require_eq(current->graphs.size(), 2u);
  check_same_graph(*current, *other);
}

TEST("inspection validates evicted targets before accepting a tap") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{
    uuid::null(), InspectionOptions{.finished_pipeline_graphs = 0}};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(descriptions));
  auto nested = Option{observer.register_plan(
    plan(descriptions, 0, tag_v<chunk_ptr>), Ref{*root}, 0)};
  auto graph = observer.subscribe_graph();
  auto current = snapshot(observer);
  auto child = std::ranges::find_if(current.graphs, [](auto const& graph) {
    return graph->parent.is_some();
  });
  require(child != current.graphs.end());
  auto known = ChannelTarget{uuid::null(), PipeId{"root"}, (*child)->path, 0};
  check_eq(observer.subscribe_channel(known).unwrap_err(),
           TapSubscriptionError::not_events);
  nested = None{};
  check_eq(observer.subscribe_channel(known).unwrap_err(),
           TapSubscriptionError::not_events);
  auto invalid = known;
  invalid.channel_index = 999;
  check_eq(observer.subscribe_channel(invalid).unwrap_err(),
           TapSubscriptionError::invalid_target);
  invalid = known;
  invalid.path.back() += 10000;
  check_eq(observer.subscribe_channel(invalid).unwrap_err(),
           TapSubscriptionError::invalid_target);
  nested = observer.register_plan(plan(descriptions, 0, tag_v<chunk_ptr>),
                                  Ref{*root}, 0);
  check_eq(observer.subscribe_channel(known).unwrap_err(),
           TapSubscriptionError::not_events);
  auto wrong = known;
  wrong.execution_id = PipeId{"other"};
  check_eq(observer.subscribe_channel(wrong).unwrap_err(),
           TapSubscriptionError::wrong_execution);
}

TEST("inspection detaches taps and releases queued batches without capture") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto registration
    = observer.register_plan(plan(descriptions, 0, tag_v<table_slice>));
  auto tap
    = Option{observer.subscribe_channel(target(snapshot(observer))).unwrap()};
  auto payload = std::weak_ptr<arrow::RecordBatch>{};
  {
    auto builder = series_builder{};
    builder.record().field("value").data(int64_t{42});
    auto batch = builder.finish_assert_one_slice();
    payload = to_record_batch(batch);
    registration->tap(0)->capture(batch);
  }
  check(not payload.expired());
  tap = None{};
  // No subsequent capture or graph update is needed to release queued data.
  check(payload.expired());
  auto before = descriptions->load(std::memory_order_relaxed);
  auto unobserved = observer.register_plan(plan(descriptions, 1));
  check_eq(descriptions->load(std::memory_order_relaxed), before);
}

TEST("known channel discovery visits only candidate runtime placements") {
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(Arc{Atomic<size_t>{0}}));
  auto parent = Option{
    observer.register_plan(plan(Arc{Atomic<size_t>{0}}, 1), Ref{*root}, 0)};
  auto child = Option{
    observer.register_plan(plan(Arc{Atomic<size_t>{0}}, 2), Ref{**parent}, 0)};
  auto known = target(snapshot(observer), 2);
  child = None{};
  parent = None{};
  // These registrations arrive with no reader or tap, so their descriptions
  // remain unknown until a request actually needs them.
  parent
    = observer.register_plan(plan(Arc{Atomic<size_t>{0}}, 1), Ref{*root}, 0);
  child
    = observer.register_plan(plan(Arc{Atomic<size_t>{0}}, 2), Ref{**parent}, 0);
  auto unrelated_calls = Arc{Atomic<size_t>{0}};
  auto unrelated = observer.register_plan(plan(unrelated_calls), Ref{*root}, 1);
  auto other_parent
    = observer.register_plan(plan(Arc{Atomic<size_t>{0}}, 3), Ref{*root}, 0);
  auto other_child_calls = Arc{Atomic<size_t>{0}};
  auto other_child
    = observer.register_plan(plan(other_child_calls), Ref{*other_parent}, 0);
  auto tap = observer.subscribe_channel(known).unwrap();
  check_eq(unrelated_calls->load(std::memory_order_relaxed), 0u);
  check_eq(other_child_calls->load(std::memory_order_relaxed), 0u);
  (*child)->tap(0)->capture(batch(4));
  auto received = folly::coro::blockingWait(tap.recv());
  require(received.is_some());
  check(is<EventBatch>(*received));
  // Opening the graph after targeted tap discovery must still discover the
  // rest of the live execution, even though the tap keeps observation active.
  auto complete = snapshot(observer);
  check_eq(complete.graphs.size(), 6u);
}

TEST("inspection subscriptions drain after their execution owner is "
     "destroyed") {
  auto graph = Option<GraphSubscription>{};
  auto tap = Option<TapSubscription>{};
  auto retained_plan = std::weak_ptr<ir::Plan const>{};
  {
    auto observer = test::InspectionFixture{uuid::null()};
    observer.start(PipeId{"root"});
    auto planned = plan(Arc{Atomic<size_t>{0}});
    retained_plan = Arc{planned}.into_shared();
    auto registration = Option{observer.register_plan(std::move(planned))};
    graph = observer.subscribe_graph();
    auto initial = folly::coro::blockingWait(graph->recv());
    require(initial.is_some());
    tap = observer.subscribe_channel(target(*initial)).unwrap();
    (*registration)->tap(0)->capture(batch(41));
    registration = None{};
    observer.finish(ExecutionOutcome::completed);
  }
  // Delivery retains queued events and published metadata, not runtime plans.
  check(retained_plan.expired());
  check_eq(folly::coro::blockingWait(tap_values(*tap)),
           (std::vector<int64_t>{42}));
  auto final = folly::coro::blockingWait(graph->recv());
  require(final.is_some());
  check_eq((*final)->end, ExecutionOutcome::completed);
  require_eq((*final)->graphs.size(), 1u);
  check_eq((*final)->graphs[0]->operators[0].name, "visible_probe");
  check(not folly::coro::blockingWait(graph->recv()));
}

TEST("inspection owner destruction joins concurrent subscription disconnects") {
  for (auto iteration = 0; iteration < 50; ++iteration) {
    auto observer
      = Option{Box<test::InspectionFixture>{std::in_place, uuid::null()}};
    (*observer)->start(PipeId{"root"});
    auto graph = Option{(*observer)->subscribe_graph()};
    auto tap = Option<TapSubscription>{};
    {
      auto registration
        = (*observer)->register_plan(plan(Arc{Atomic<size_t>{0}}));
      tap
        = (*observer)->subscribe_channel(target(snapshot(**observer))).unwrap();
      registration->tap(0)->capture(batch(0));
    }
    auto start = std::barrier{2};
    auto disconnect = std::thread{
      [&start, graph = std::move(graph), tap = std::move(tap)]() mutable {
        start.arrive_and_wait();
        graph = None{};
        tap = None{};
      }};
    start.arrive_and_wait();
    observer = None{};
    disconnect.join();
  }
}

TEST("cancelling a tap read detaches an idle channel") {
  for (auto precancelled : {false, true}) {
    auto descriptions = Arc{Atomic<size_t>{0}};
    auto observer = test::InspectionFixture{uuid::null()};
    observer.start(PipeId{"root"});
    auto registration = observer.register_plan(plan(descriptions));
    auto tap = observer.subscribe_channel(target(snapshot(observer))).unwrap();
    auto executor = folly::ManualExecutor{};
    auto cancellation = folly::CancellationSource{};
    if (precancelled) {
      cancellation.requestCancellation();
    }
    auto receiving
      = folly::coro::co_withCancellation(cancellation.getToken(), tap.recv());
    auto read
      = folly::coro::co_withExecutor(folly::getKeepAliveToken(&executor),
                                     catch_cancellation(std::move(receiving)))
          .start();
    auto cleanup = detail::scope_guard{[&]() noexcept {
      cancellation.requestCancellation();
      executor.drain();
    }};
    executor.drain();
    if (not precancelled) {
      require(not read.isReady());
      cancellation.requestCancellation();
      executor.drain();
    }
    require(read.isReady());
    check(not std::move(read).get());
    check(not folly::coro::blockingWait(tap.recv()));
    auto before = descriptions->load(std::memory_order_relaxed);
    auto unobserved = observer.register_plan(plan(descriptions, 1));
    check_eq(descriptions->load(std::memory_order_relaxed), before);
  }
}

TEST("inspection final graph bypasses coalescing and drains before EOF") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto graph = observer.subscribe_graph();
  auto registration = Option{observer.register_plan(plan(descriptions))};
  registration = None{};
  observer.finish(ExecutionOutcome::failed);
  auto final = folly::coro::blockingWait(graph.recv());
  require(final.is_some());
  check_eq((*final)->graphs.size(), 1u);
  check_eq(*(*final)->end, ExecutionOutcome::failed);
  check(not folly::coro::blockingWait(graph.recv()));
}

TEST("inspection delivers the final graph to every subscriber") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto first = observer.subscribe_graph();
  auto root = Option{observer.register_plan(plan(descriptions))};
  auto second = observer.subscribe_graph();
  auto child = Option{observer.register_plan(plan(descriptions), Ref{**root})};
  child = None{};
  root = None{};
  observer.finish(ExecutionOutcome::completed);
  auto final = folly::coro::blockingWait(first.recv()).unwrap();
  auto second_final = folly::coro::blockingWait(second.recv()).unwrap();
  check_eq(final->graphs.size(), 2u);
  check_eq(final->end, ExecutionOutcome::completed);
  check_same_graph(*final, *second_final);
  check(not folly::coro::blockingWait(first.recv()));
  check(not folly::coro::blockingWait(second.recv()));
}

TEST("inspection publisher delivers timed updates to every graph stream") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto first = observer.subscribe_graph();
  auto second = observer.subscribe_graph();
  require(folly::coro::blockingWait(first.recv()).is_some());
  require(folly::coro::blockingWait(second.recv()).is_some());
  auto root = observer.register_plan(plan(descriptions));
  auto update
    = folly::coro::blockingWait(
        folly::coro::timeoutNoDiscard(first.recv(), std::chrono::seconds{2}))
        .unwrap();
  auto second_update
    = folly::coro::blockingWait(
        folly::coro::timeoutNoDiscard(second.recv(), std::chrono::seconds{2}))
        .unwrap();
  check_eq(update->graphs.size(), 1u);
  check(not update->end);
  check_same_graph(*update, *second_update);
}

TEST("a reconnected graph reader receives current state and later changes") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(descriptions));
  {
    auto first = observer.subscribe_graph();
    require(folly::coro::blockingWait(first.recv()).is_some());
  }
  auto child = observer.register_plan(plan(descriptions, 1), Ref{*root});
  auto resumed = observer.subscribe_graph();
  auto update
    = folly::coro::blockingWait(
        folly::coro::timeoutNoDiscard(resumed.recv(), std::chrono::seconds{2}))
        .unwrap();
  check_eq(update->graphs.size(), 2u);
  check(not update->end);
  auto later = observer.register_plan(plan(descriptions, 2), Ref{*root});
  auto next
    = folly::coro::blockingWait(
        folly::coro::timeoutNoDiscard(resumed.recv(), std::chrono::seconds{2}))
        .unwrap();
  check_eq(next->graphs.size(), 3u);
  check(not next->end);
}

TEST("reconnecting a tap receives new events from every physical channel") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto first = Option{observer.register_plan(plan(descriptions))};
  auto second = Option{observer.register_plan(plan(descriptions))};
  auto channel = target(snapshot(observer));
  auto tap = Option{observer.subscribe_channel(channel).unwrap()};
  (*first)->tap(0)->capture(batch(1));
  (*second)->tap(0)->capture(batch(2));
  tap = None{};
  auto resumed = observer.subscribe_channel(channel).unwrap();
  (*first)->tap(0)->capture(batch(42));
  (*second)->tap(0)->capture(batch(43));
  first = None{};
  second = None{};
  observer.finish(ExecutionOutcome::completed);
  check_eq(folly::coro::blockingWait(tap_values(resumed)),
           (std::vector<int64_t>{43, 44}));
}

TEST("inspection reconnect publishes current state while a tap stays open") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(descriptions));
  auto first = Option{observer.subscribe_graph()};
  auto before = folly::coro::blockingWait(first->recv()).unwrap();
  auto tap = observer.subscribe_channel(target(*before)).unwrap();
  first = None{};
  auto child = observer.register_plan(plan(descriptions, 1), Ref{*root});
  auto second = observer.subscribe_graph();
  auto after
    = folly::coro::blockingWait(
        folly::coro::timeoutNoDiscard(second.recv(), std::chrono::seconds{2}))
        .unwrap();
  check_eq(after->graphs.size(), 2u);
  check_eq(before->graphs.size(), 1u);
  check(not after->end);
}

TEST("a slow graph reader receives the latest state and then completion") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto fast = observer.subscribe_graph();
  auto slow = observer.subscribe_graph();
  require(folly::coro::blockingWait(fast.recv()).is_some());
  require(folly::coro::blockingWait(slow.recv()).is_some());
  auto root = Option{observer.register_plan(plan(descriptions))};
  for (auto port = size_t{1}; port <= 4; ++port) {
    auto child = observer.register_plan(plan(descriptions, port), Ref{**root});
    auto update = folly::coro::blockingWait(fast.recv()).unwrap();
    check_eq(update->graphs.size(), port + 1);
  }
  auto latest = folly::coro::blockingWait(slow.recv()).unwrap();
  require_eq(latest->graphs.size(), 5u);
  auto ports = std::set<size_t>{};
  for (auto const& graph : latest->graphs) {
    require_eq(graph->channels.size(), 1u);
    ports.insert(graph->channels[0].from_port);
  }
  check_eq(ports, (std::set<size_t>{0, 1, 2, 3, 4}));
  // Finishing replaces a pending update too; neither reader needs to drain
  // intermediate snapshots before receiving the final one.
  auto child
    = Option{observer.register_plan(plan(descriptions, 5), Ref{**root})};
  child = None{};
  root = None{};
  observer.finish(ExecutionOutcome::completed);
  auto fast_final = folly::coro::blockingWait(fast.recv()).unwrap();
  auto slow_final = folly::coro::blockingWait(slow.recv()).unwrap();
  check_eq(fast_final->end, ExecutionOutcome::completed);
  check_eq(fast_final->graphs.size(), 6u);
  check_same_graph(*fast_final, *slow_final);
  check(not folly::coro::blockingWait(fast.recv()));
  check(not folly::coro::blockingWait(slow.recv()));
}

TEST("inspection keeps only the latest update while the graph reader is "
     "stalled") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto graph = observer.subscribe_graph();
  // All publications share one path: unread snapshots are replaced even
  // before the consumer's first read.
  auto root = Option{observer.register_plan(plan(descriptions))};
  auto child
    = Option{observer.register_plan(plan(descriptions), Ref{**root}, 0)};
  auto repeated
    = Option{observer.register_plan(plan(descriptions), Ref{**root}, 0)};
  auto current = folly::coro::blockingWait(graph.recv());
  require(current.is_some());
  check_eq((*current)->graphs.size(), 2u);
  check(not(*current)->end);
  auto executor = folly::ManualExecutor{};
  auto cancellation = folly::CancellationSource{};
  auto pending
    = folly::coro::co_withExecutor(
        folly::getKeepAliveToken(&executor),
        folly::coro::co_withCancellation(cancellation.getToken(), graph.recv()))
        .start();
  auto cleanup = detail::scope_guard{[&]() noexcept {
    cancellation.requestCancellation();
    executor.drain();
  }};
  executor.drain();
  require(not pending.isReady());
  // A duplicate must leave the pending read suspended.
  auto duplicate
    = Option{observer.register_plan(plan(descriptions), Ref{**root}, 0)};
  executor.drain();
  check(not pending.isReady());
  auto different
    = Option{observer.register_plan(plan(descriptions, 1), Ref{**root}, 0)};
  current = std::move(pending).via(&executor).getVia(&executor);
  require(current.is_some());
  check_eq((*current)->graphs.size(), 3u);
  check(not(*current)->end);
  different = None{};
  duplicate = None{};
  repeated = None{};
  child = None{};
  root = None{};
  observer.finish(ExecutionOutcome::completed);
  auto final = folly::coro::blockingWait(graph.recv());
  require(final.is_some());
  check_eq((*final)->graphs.size(), 3u);
  check_eq(*(*final)->end, ExecutionOutcome::completed);
  check(not folly::coro::blockingWait(graph.recv()));
}

TEST("inspection keeps publishing while a notified graph reader is stalled") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto root = Option{observer.register_plan(plan(descriptions))};
  auto graph = observer.subscribe_graph();
  require(folly::coro::blockingWait(graph.recv()).is_some());
  auto witness = observer.subscribe_graph();
  require(folly::coro::blockingWait(witness.recv()).is_some());
  require(folly::coro::blockingWait(graph.recv()).is_some());
  auto executor = folly::ManualExecutor{};
  auto cancellation = folly::CancellationSource{};
  auto pending
    = folly::coro::co_withExecutor(
        folly::getKeepAliveToken(&executor),
        folly::coro::co_withCancellation(cancellation.getToken(), graph.recv()))
        .start();
  auto cleanup = detail::scope_guard{[&]() noexcept {
    cancellation.requestCancellation();
    executor.drain();
  }};
  executor.drain();
  require(not pending.isReady());
  // The first publication wakes the read, but its executor cannot resume it.
  // Further publications and completion must still replace pending updates.
  for (auto port = size_t{1}; port <= 32; ++port) {
    auto child = observer.register_plan(plan(descriptions, port), Ref{**root});
    require(folly::coro::blockingWait(witness.recv()).is_some());
  }
  root = None{};
  observer.finish(ExecutionOutcome::completed);
  require(not pending.isReady());
  executor.drain();
  require(pending.isReady());
  auto current = std::move(pending).get().unwrap();
  if (not current->end) {
    current = folly::coro::blockingWait(graph.recv()).unwrap();
  }
  check_eq(current->end, ExecutionOutcome::completed);
  check_eq(current->graphs.size(), 33u);
  check(not folly::coro::blockingWait(graph.recv()));
}

TEST("inspection retains live ancestors and evicts whole finished branches") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{
    uuid::null(), InspectionOptions{.finished_pipeline_graphs = 1}};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(descriptions));
  auto graph = observer.subscribe_graph();
  auto parent
    = Option{observer.register_plan(plan(descriptions), Ref{*root}, 0)};
  auto child
    = Option{observer.register_plan(plan(descriptions), Ref{**parent}, 0)};
  auto initial = snapshot(observer);
  auto parent_graph
    = std::ranges::find_if(initial.graphs, [](auto const& graph) {
        return graph->path.size() == 3;
      });
  auto child_graph
    = std::ranges::find_if(initial.graphs, [](auto const& graph) {
        return graph->path.size() == 5;
      });
  require(parent_graph != initial.graphs.end());
  require(child_graph != initial.graphs.end());
  auto parent_id = (*parent_graph)->path;
  auto child_id = (*child_graph)->path;
  for (auto port = size_t{1}; port < 5; ++port) {
    auto finished
      = observer.register_plan(plan(descriptions, port), Ref{*root}, 0);
  }
  auto current = snapshot(observer);
  check_eq(current.graphs.size(), 4u);
  for (auto const& id : {parent_id, child_id}) {
    check(std::ranges::any_of(current.graphs, [&](auto const& pipeline) {
      return pipeline->path == id;
    }));
  }
  child = None{};
  parent = None{};
  current = snapshot(observer);
  check_eq(current.graphs.size(), 2u);
  for (auto const& pipeline : current.graphs) {
    check_ne(pipeline->path, parent_id);
    check_ne(pipeline->path, child_id);
    if (pipeline->parent) {
      check(std::ranges::any_of(current.graphs, [&](auto const& candidate) {
        return candidate->path == *pipeline->parent;
      }));
    }
  }
}

TEST("inspection evicts by visibility order when a known graph returns") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{
    uuid::null(), InspectionOptions{.finished_pipeline_graphs = 1}};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(descriptions));
  auto graph = observer.subscribe_graph();
  auto first
    = Option{observer.register_plan(plan(descriptions, 1), Ref{*root})};
  auto initial = snapshot(observer);
  require_eq(initial.graphs.size(), 2u);
  auto first_path = initial.graphs[1]->path;
  first = None{};
  {
    auto second = observer.register_plan(plan(descriptions, 2), Ref{*root});
  }
  auto evicted = snapshot(observer);
  require_eq(evicted.graphs.size(), 2u);
  check_ne(evicted.graphs[1]->path, first_path);
  {
    auto returned = observer.register_plan(plan(descriptions, 1), Ref{*root});
  }
  auto restored = snapshot(observer);
  require_eq(restored.graphs.size(), 2u);
  check_eq(restored.graphs[1]->path, first_path);
}

TEST("inspection retirement preserves visibility order across many live "
     "layouts") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{
    uuid::null(), InspectionOptions{.finished_pipeline_graphs = 5}};
  observer.start(PipeId{"root"});
  auto root = observer.register_plan(plan(descriptions));
  auto graph = observer.subscribe_graph();
  auto children = std::vector<Box<PlanRegistration>>{};
  for (auto port = size_t{0}; port < 64; ++port) {
    children.push_back(
      observer.register_plan(plan(descriptions, port), Ref{*root}));
  }
  auto initial = snapshot(observer);
  require_eq(initial.graphs.size(), 65u);
  // Finish newest layouts first: eviction must still follow visibility order,
  // rather than treating the most recently retired layout as the newest one.
  while (not children.empty()) {
    children.pop_back();
  }
  auto retained = snapshot(observer);
  require_eq(retained.graphs.size(), 6u);
  for (auto index = size_t{1}; index < retained.graphs.size(); ++index) {
    check_eq(retained.graphs[index]->path, initial.graphs[59 + index]->path);
  }
}

TEST("inspection fused receive captures data once and excludes control "
     "signals") {
  auto descriptions = Arc{Atomic<size_t>{0}};
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto registration = Option{observer.register_plan(plan(descriptions))};
  auto tap = observer.subscribe_channel(target(snapshot(observer))).unwrap();
  auto [sender, receiver]
    = fused_channel<OperatorMsg<nova::Events>>((*registration)->tap(0));
  folly::coro::blockingWait(folly::coro::collectAll(
    [](FusedSender<OperatorMsg<nova::Events>> sender) -> Task<void> {
      co_await sender.send(batch(0));
      co_await sender.send(EndOfData{});
    }(std::move(sender)),
    [&]() -> Task<void> {
      require((co_await receiver.recv()).is_some());
      require((co_await receiver.recv()).is_some());
      check(not co_await receiver.recv());
    }()));
  registration = None{};
  observer.finish(ExecutionOutcome::completed);
  check_eq(count(tap), 1u);
}

TEST("inspection subscription races capture and joined execution completion") {
  for (auto iteration = 0; iteration < 50; ++iteration) {
    auto descriptions = Arc{Atomic<size_t>{0}};
    auto observer = test::InspectionFixture{uuid::null()};
    observer.start(PipeId{"root"});
    auto registration = Option{observer.register_plan(plan(descriptions))};
    auto id = target(snapshot(observer));
    auto accepted = observer.subscribe_channel(id).unwrap();
    // This batch is accepted before concurrent subscription changes.
    (*registration)->tap(0)->capture(batch(-100));
    auto subscriptions = std::vector<TapSubscription>{};
    auto rejections = std::vector<TapSubscriptionError>{};
    auto start = std::barrier{3};
    auto producer = std::thread{[&] {
      start.arrive_and_wait();
      auto registered
        = observer.register_plan(plan(descriptions, 1), Ref{**registration}, 0);
      for (auto i = 0; i < 20; ++i) {
        (*registration)->tap(0)->capture(batch(i));
      }
    }};
    auto consumer = std::thread{[&] {
      start.arrive_and_wait();
      for (auto i = 0; i < 20; ++i) {
        auto subscribed = observer.subscribe_channel(id);
        if (subscribed.is_ok()) {
          subscriptions.push_back(std::move(subscribed).unwrap());
        } else {
          rejections.push_back(subscribed.unwrap_err());
        }
      }
    }};
    start.arrive_and_wait();
    producer.join();
    registration = None{};
    observer.finish(ExecutionOutcome::completed);
    consumer.join();
    auto values = folly::coro::blockingWait(tap_values(accepted));
    require(not values.empty());
    check_eq(values.front(), -99);
    check(values.size() <= 4);
    check(std::adjacent_find(values.begin(), values.end()) == values.end());

    check_eq(subscriptions.size() + rejections.size(), 20u);
    for (auto reason : rejections) {
      check_eq(reason, TapSubscriptionError::finished);
    }
    for (auto& tap : subscriptions) {
      check(count(tap) <= 4);
    }
  }
}

TEST("inspection preserves lazy discovery under concurrent nested churn") {
  for (auto observed : {false, true}) {
    auto descriptions = Arc{Atomic<size_t>{0}};
    auto observer = test::InspectionFixture{uuid::null()};
    observer.start(PipeId{"root"});
    auto graph = Option<GraphSubscription>{};
    if (observed) {
      graph = observer.subscribe_graph();
    }
    auto root = observer.register_plan(plan(descriptions));
    auto start = std::barrier{5};
    auto workers = std::vector<std::thread>{};
    for (auto thread = 0; thread < 4; ++thread) {
      workers.emplace_back([&] {
        start.arrive_and_wait();
        for (auto i = 0; i < 1000; ++i) {
          auto nested = observer.register_plan(plan(descriptions), Ref{*root});
        }
      });
    }
    start.arrive_and_wait();
    for (auto& worker : workers) {
      worker.join();
    }
    if (not observed) {
      check_eq(descriptions->load(std::memory_order_relaxed), 0u);
    }
    if (observed) {
      check_eq(snapshot(observer).graphs.size(), 2u);
    }
  }
}

TEST("cancelling an unstarted or suspended graph read detaches its "
     "subscription") {
  for (auto precancelled : {false, true}) {
    auto descriptions = Arc{Atomic<size_t>{0}};
    auto observer = test::InspectionFixture{uuid::null()};
    observer.start(PipeId{"root"});
    auto registration = observer.register_plan(plan(descriptions));
    auto graph = observer.subscribe_graph();
    require(folly::coro::blockingWait(graph.recv()).is_some());
    auto executor = folly::ManualExecutor{};
    auto cancellation = folly::CancellationSource{};
    if (precancelled) {
      cancellation.requestCancellation();
    }
    auto receiving
      = folly::coro::co_withCancellation(cancellation.getToken(), graph.recv());
    auto read
      = folly::coro::co_withExecutor(folly::getKeepAliveToken(&executor),
                                     catch_cancellation(std::move(receiving)))
          .start();
    auto cleanup = detail::scope_guard{[&]() noexcept {
      cancellation.requestCancellation();
      executor.drain();
    }};
    executor.drain();
    if (not precancelled) {
      require(not read.isReady());
      cancellation.requestCancellation();
      executor.drain();
    }
    require(read.isReady());
    check(not std::move(read).get());
    check(not folly::coro::blockingWait(graph.recv()));
    // The cancelled subscription must no longer trigger metadata discovery.
    auto before = descriptions->load(std::memory_order_relaxed);
    auto unobserved
      = observer.register_plan(plan(descriptions, 1), Ref{*registration});
    check_eq(descriptions->load(std::memory_order_relaxed), before);
    check_eq(snapshot(observer).graphs.size(), 2u);
  }
}

TEST("inspection wakes every suspended graph reader on completion") {
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto subscriptions = std::vector<GraphSubscription>{};
  for (auto i = 0; i < 2; ++i) {
    subscriptions.push_back(observer.subscribe_graph());
  }
  auto executor = folly::ManualExecutor{};
  auto pending
    = std::vector<folly::SemiFuture<Option<Arc<GraphSnapshot const>>>>{};
  auto cancellation = folly::CancellationSource{};
  auto cleanup = detail::scope_guard{[&]() noexcept {
    cancellation.requestCancellation();
    executor.drain();
  }};
  for (auto& graph : subscriptions) {
    require(folly::coro::blockingWait(graph.recv()).is_some());
    pending.push_back(
      folly::coro::co_withExecutor(
        folly::getKeepAliveToken(&executor),
        folly::coro::co_withCancellation(cancellation.getToken(), graph.recv()))
        .start());
  }
  executor.drain();
  for (auto& read : pending) {
    require(not read.isReady());
  }
  observer.finish(ExecutionOutcome::cancelled);
  executor.drain();
  for (auto& read : pending) {
    require(read.isReady());
    auto final = std::move(read).get();
    require(final.is_some());
    check_eq((*final)->end, ExecutionOutcome::cancelled);
  }
  for (auto& graph : subscriptions) {
    check(not folly::coro::blockingWait(graph.recv()));
  }
}

TEST("inspection rejects subscriptions until execution starts") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  auto control = Arc<FlowControl>{std::in_place};
  auto diagnostics = TestDiagnostics{};
  auto observer = Box<PipelineObserver>{std::in_place, uuid::null()};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    // Neither constructing the observer nor creating its lazy execution task
    // admits subscriptions. Rejection must not require either task to start.
    check_eq((observer->subscribe_graph()).unwrap_err(),
             GraphSubscriptionError::not_started);
    auto execution
      = run_plan(flow_plan(control, ir::ChannelKind::regular, false), reg,
                 system, diagnostics, NoProfiler{}, false, nullptr,
                 Ref{*observer});
    check_eq((observer->subscribe_graph()).unwrap_err(),
             GraphSubscriptionError::not_started);
    check_eq(
      (observer->subscribe_channel({uuid::null(), PipeId{"root"}, {0}, 0}))
        .unwrap_err(),
      TapSubscriptionError::not_started);
    auto run = scope.spawn(std::move(execution));
    co_await control->ready.wait();
    auto graph = (observer->subscribe_graph()).unwrap();
    auto initial = co_await graph.recv();
    require(initial.is_some());
    auto tap = (observer->subscribe_channel(target(*initial, 0, 1))).unwrap();
    control->produce.notify_one();
    check(not(co_await run.join()).is_error());
    check(count(tap) > 0);

    auto final_graph = (observer->subscribe_graph()).unwrap();
    auto final = co_await final_graph.recv();
    require(final.is_some());
    check_eq((*final)->end, ExecutionOutcome::completed);
    check(not co_await final_graph.recv());
    check_eq((observer->subscribe_channel(target(*initial, 0, 1))).unwrap_err(),
             TapSubscriptionError::finished);
  }));
}

TEST("run_plan finishes inspection when already cancelled before startup") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  auto control = Arc<FlowControl>{std::in_place};
  auto diagnostics = TestDiagnostics{};
  auto observer = Box<PipelineObserver>{std::in_place, uuid::null()};
  auto cancellation = folly::CancellationSource{};
  cancellation.requestCancellation();
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto execution = scope.spawn([&]() -> Task<failure_or<void>> {
      co_return co_await folly::coro::co_withCancellation(
        cancellation.getToken(),
        run_plan(flow_plan(control, ir::ChannelKind::regular, false), reg,
                 system, diagnostics, NoProfiler{}, false, nullptr,
                 Ref{*observer}));
    });
    check(not co_await execution.try_join());
    auto subscribed = observer->subscribe_graph();
    require(subscribed.is_ok());
    auto graph = std::move(subscribed).unwrap();
    auto final = co_await graph.recv();
    require(final.is_some());
    check_eq((*final)->end, ExecutionOutcome::cancelled);
    check(not(*final)->execution_id);
    check((*final)->graphs.empty());
    check(not co_await graph.recv());
  }));
}

TEST("destroying an unstarted run_plan leaves inspection unstarted") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  auto control = Arc<FlowControl>{std::in_place};
  auto diagnostics = TestDiagnostics{};
  auto observer = Box<PipelineObserver>{std::in_place, uuid::null()};
  {
    auto execution
      = run_plan(flow_plan(control, ir::ChannelKind::regular, false), reg,
                 system, diagnostics, NoProfiler{}, false, nullptr,
                 Ref{*observer});
    check_eq(observer->subscribe_graph().unwrap_err(),
             GraphSubscriptionError::not_started);
  }
  check_eq(observer->subscribe_graph().unwrap_err(),
           GraphSubscriptionError::not_started);
}

TEST("shared plans run root and nested pipelines without an observer") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  for (auto kind : {ir::ChannelKind::regular, ir::ChannelKind::tiny,
                    ir::ChannelKind::fused}) {
    for (auto nested : {false, true}) {
      auto control = Arc<FlowControl>{std::in_place};
      auto diagnostics = TestDiagnostics{};
      control->produce.notify_one();
      auto result = folly::coro::blockingWait(
        run_plan(flow_plan(control, kind, nested), reg, system, diagnostics,
                 NoProfiler{}, false));
      check(not result.is_error());
      check_eq(control->received.load(std::memory_order_relaxed), 12u);
    }
  }
}

TEST("inspection runtime captures regular tiny fused and nested bridge "
     "channels") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  for (auto kind : {ir::ChannelKind::regular, ir::ChannelKind::tiny,
                    ir::ChannelKind::fused}) {
    for (auto nested : {false, true}) {
      for (auto [profiling, capacity] :
           {std::pair{false, size_t{4}}, std::pair{false, size_t{64}},
            std::pair{true, size_t{64}}}) {
        auto control = Arc<FlowControl>{std::in_place};
        auto observer
          = Box<PipelineObserver>{std::in_place, uuid::null(),
                                  InspectionOptions{.tap_capacity = capacity}};
        auto diagnostics = TestDiagnostics{};
        auto profile_path
          = std::filesystem::temp_directory_path()
            / fmt::format("tenzir-inspection-{}.json", uuid::random());
        auto cleanup = detail::scope_guard{[&] noexcept {
          auto error = std::error_code{};
          std::filesystem::remove(profile_path, error);
        }};
        auto profiler = profiling
                          ? Profiler{PerfettoProfiler{profile_path.string()}}
                          : Profiler{NoProfiler{}};
        folly::coro::blockingWait(
          async_scope([&](AsyncScope& scope) -> Task<void> {
            auto execution
              = scope.spawn(run_plan(flow_plan(control, kind, nested), reg,
                                     system, diagnostics, std::move(profiler),
                                     false, nullptr, Ref{*observer}));
            co_await control->ready.wait();
            auto graph = (observer->subscribe_graph()).unwrap();
            auto initial = co_await graph.recv();
            require(initial.is_some());
            check_eq((*initial)->graphs.size(), nested ? 2u : 1u);
            auto taps = std::vector<TapSubscription>{};
            for (auto const& pipeline : (*initial)->graphs) {
              for (auto i = size_t{0}; i < pipeline->channels.size(); ++i) {
                if (pipeline->channels[i].type.is<nova::Events>()) {
                  taps.push_back((observer->subscribe_channel(
                                    {uuid::null(), *(*initial)->execution_id,
                                     pipeline->path, i}))
                                   .unwrap());
                }
              }
            }
            check_eq(taps.size(), nested ? 5u : 2u);
            control->produce.notify_one();
            auto result = co_await execution.join();
            check(not result.is_error());
            check_eq(control->received.load(std::memory_order_relaxed), 12u);
            for (auto& tap : taps) {
              auto received = co_await drain(tap);
              check_eq(received, std::min(capacity, size_t{12}));
            }
            auto final = *initial;
            while (auto next = co_await graph.recv()) {
              final = std::move(*next);
            }
            check_eq(*final->end, ExecutionOutcome::completed);
          }));
      }
    }
  }
}

TEST("inspection aggregates physical lanes across scatter shuffle and gather") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  for (auto [lanes, keyed] :
       {std::pair{size_t{3}, false}, std::pair{size_t{2}, false},
        std::pair{size_t{3}, true}}) {
    auto control = Arc<FlowControl>{std::in_place};
    auto planned = flow_plan(control, ir::ChannelKind::tiny, false);
    auto second = planned.operators[1];
    second.id = OpId{"2"};
    second.parallelism = lanes;
    if (keyed) {
      auto diagnostics = collecting_diagnostic_handler{};
      auto provider = session_provider::make(diagnostics, *reg);
      auto key = parse_expression_with_location_override(
        "value", location::unknown, provider.as_session());
      require(not key.is_error());
      second.partition_keys = std::move(*key);
    }
    planned.operators[1].parallelism = 3;
    planned.operators[2].id = OpId{"3"};
    planned.operators.insert(planned.operators.begin() + 2, std::move(second));
    planned.channels = {
      {ir::Port::input, 0, 0, tag_v<void>},
      {0, 0, 1, tag_v<nova::Events>, ir::ChannelKind::tiny},
      {1, 0, 2, tag_v<nova::Events>, ir::ChannelKind::tiny},
      {2, 0, 3, tag_v<nova::Events>, ir::ChannelKind::tiny},
      {3, 0, ir::Port::output, tag_v<void>},
    };
    auto observer = Box<PipelineObserver>{
      std::in_place, uuid::null(), InspectionOptions{.tap_capacity = 64}};
    auto diagnostics = TestDiagnostics{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto execution
        = scope.spawn(run_plan(std::move(planned), reg, system, diagnostics,
                               NoProfiler{}, false, nullptr, Ref{*observer}));
      co_await control->ready.wait();
      auto graph = (observer->subscribe_graph()).unwrap();
      auto current = co_await graph.recv();
      require(current.is_some());
      require_eq((*current)->graphs.size(), 1u);
      auto taps = std::vector<TapSubscription>{};
      for (auto edge : {1u, 2u, 3u}) {
        taps.push_back(
          (observer->subscribe_channel(target(*current, 0, edge))).unwrap());
      }
      control->produce.notify_one();
      check(not(co_await execution.join()).is_error());
      check_eq(control->received.load(std::memory_order_relaxed), 12u);
      auto expected = std::vector<int64_t>{};
      for (auto i = int64_t{1}; i <= 12; ++i) {
        expected.push_back(i);
      }
      for (auto& tap : taps) {
        check_eq(co_await tap_values(tap), expected);
      }
    }));
  }
}

TEST("inspection separates output ports connected to the same downstream") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  auto control = Arc<FlowControl>{std::in_place};
  auto planned = flow_plan(control, ir::ChannelKind::regular, false);
  planned.operators[1].op = RuntimeIr{[]() -> AnyOperator {
    return Box<Operator<nova::Events, nova::Events, true>>{
      SplitEvents{}.with_name("split")};
  }};
  planned.channels.insert(planned.channels.begin() + 3,
                          {1, 1, 2, tag_v<nova::Events>});
  auto observer = Box<PipelineObserver>{std::in_place, uuid::null(),
                                        InspectionOptions{.tap_capacity = 64}};
  auto diagnostics = TestDiagnostics{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto execution
      = scope.spawn(run_plan(std::move(planned), reg, system, diagnostics,
                             NoProfiler{}, false, nullptr, Ref{*observer}));
    co_await control->ready.wait();
    auto graph = (observer->subscribe_graph()).unwrap();
    auto current = co_await graph.recv();
    require(current.is_some());
    require_eq((*current)->graphs.size(), 1u);
    auto even = (observer->subscribe_channel(target(*current, 0, 2))).unwrap();
    auto odd = (observer->subscribe_channel(target(*current, 0, 3))).unwrap();
    control->produce.notify_one();
    check(not(co_await execution.join()).is_error());
    check_eq(control->received.load(std::memory_order_relaxed), 12u);
    check_eq(co_await tap_values(even),
             (std::vector<int64_t>{2, 4, 6, 8, 10, 12}));
    check_eq(co_await tap_values(odd),
             (std::vector<int64_t>{1, 3, 5, 7, 9, 11}));
  }));
}

TEST("run_plan closes retained inspection subscriptions when cancelled") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  // An operator error cancels the same runtime task as an external request.
  // Its diagnostic must still produce failed rather than cancelled inspection.
  for (auto failed : {false, true}) {
    auto control = Arc<FlowControl>{std::in_place};
    auto observer = Box<PipelineObserver>{std::in_place, uuid::null()};
    auto diagnostics = TestDiagnostics{};
    auto cancellation = folly::CancellationSource{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto execution = scope.spawn([&]() -> Task<failure_or<void>> {
        co_return co_await folly::coro::co_withCancellation(
          cancellation.getToken(),
          run_plan(flow_plan(control, ir::ChannelKind::regular, false), reg,
                   system, diagnostics, NoProfiler{}, false, nullptr,
                   Ref{*observer}));
      });
      co_await control->ready.wait();
      auto graph = (observer->subscribe_graph()).unwrap();
      auto initial = co_await graph.recv();
      require(initial.is_some());
      auto tap = (observer->subscribe_channel(target(*initial, 0, 1))).unwrap();
      if (failed) {
        diagnostic::error("inspection test failure").emit(diagnostics);
      }
      cancellation.requestCancellation();
      check(not co_await execution.try_join());
      // Retain the observer through EOF: its destructor must not be what
      // closes these subscriptions, and the caller never calls finish().
      auto expected
        = failed ? ExecutionOutcome::failed : ExecutionOutcome::cancelled;
      check_eq(co_await drain(tap, expected), 0u);
      auto final = *initial;
      while (auto next = co_await graph.recv()) {
        final = std::move(*next);
      }
      check_eq(*final->end, expected);
    }));
  }
}

TEST("run_plan reports diagnostic failure without caller completion") {
  class FailingSink final : public Operator<nova::Events, void> {
  public:
    auto process(nova::Events, OpCtx&) -> Task<void> override {
      co_return;
    }
    auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
      diagnostic::error("inspection test failure").emit(ctx);
      co_return FinalizeBehavior::done;
    }
  };
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto reg = make_builtin_registry();
  auto control = Arc<FlowControl>{std::in_place};
  auto observer = Box<PipelineObserver>{std::in_place, uuid::null()};
  auto diagnostics = TestDiagnostics{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto planned = flow_plan(control, ir::ChannelKind::regular, false);
    planned.operators.back().op = RuntimeIr{[]() -> AnyOperator {
      return Box<Operator<nova::Events, void>>{
        FailingSink{}.with_name("failing_sink")};
    }};
    auto execution
      = scope.spawn(run_plan(std::move(planned), reg, system, diagnostics,
                             NoProfiler{}, false, nullptr, Ref{*observer}));
    co_await control->ready.wait();
    auto graph = (observer->subscribe_graph()).unwrap();
    auto initial = co_await graph.recv();
    require(initial.is_some());
    auto tap = (observer->subscribe_channel(target(*initial, 0, 1))).unwrap();
    control->produce.notify_one();
    check((co_await execution.join()).is_error());
    std::ignore = co_await drain(tap, ExecutionOutcome::failed);
    auto final = *initial;
    while (auto next = co_await graph.recv()) {
      final = std::move(*next);
    }
    check_eq(*final->end, ExecutionOutcome::failed);
  }));
}

} // namespace tenzir
