//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_observer.hpp"

#include "tenzir/async/result.hpp"
#include "tenzir/detail/scope_guard.hpp"

#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <folly/CancellationToken.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <ranges>
#include <tuple>

namespace tenzir::detail {

namespace {
// Identity lasts for the execution; visible history is separately bounded.
// All membership, ordering, and eviction indexes are maintained here.
class GraphHistory {
public:
  explicit GraphHistory(size_t limit) : limit_{limit} {
  }

  auto add_instance(PipelineGraph graph, Option<size_t> parent, size_t op,
                    PlanRegistration& instance) -> size_t {
    auto& candidates = placements_[{parent, parent ? op : 0}];
    auto description = [](PipelineGraph const& value) {
      return std::tie(value.definition_location, value.operators,
                      value.channels);
    };
    auto found = std::ranges::lower_bound(
      candidates, description(graph), {}, [&](size_t id) {
        return description(*records_[id].graph);
      });
    auto id = records_.size();
    if (found != candidates.end()
        and description(*records_[*found].graph) == description(graph)) {
      id = *found;
    } else {
      if (parent) {
        graph.parent = records_[*parent].graph->path;
        graph.parent_operator = op;
        graph.path = *graph.parent;
        graph.path.push_back(op);
      }
      graph.path.push_back(id);
      records_.push_back({Arc<PipelineGraph const>{std::move(graph)}, {}});
      candidates.insert(found, id);
    }
    observe_instance(id, instance);
    return id;
  }

  auto observe_instance(size_t id, PlanRegistration& instance) -> void {
    auto order = include(id);
    finished_.erase(order);
    records_[id].instances.insert(&instance);
  }

  auto remove_instance(size_t id, PlanRegistration& instance) -> void {
    auto& record = records_[id];
    record.instances.erase(&instance);
    if (not record.graph->parent or not record.instances.empty()) {
      return;
    }
    auto found = visible_.find(record.graph->path);
    if (found == visible_.end()) {
      return;
    }
    finished_.emplace(found->order, id);
    if (finished_.size() <= limit_) {
      return;
    }
    auto const& branch = records_[finished_.begin()->second].graph->path;
    // Descendants are contiguous in path order. No live layout can be below
    // this finished layout: runtime parents outlive all their children.
    auto position = visible_.lower_bound(branch);
    while (position != visible_.end()
           and std::ranges::starts_with(position->graph->path, branch)) {
      finished_.erase(position->order);
      position = visible_.erase(position);
    }
    dirty_ = true;
  }

  auto end_observation() -> void {
    visible_.clear();
    finished_.clear();
  }

  auto finish() -> void {
    end_observation();
    placements_.clear();
    records_.clear();
  }

  auto snapshot() const -> std::vector<Arc<PipelineGraph const>> {
    auto result = std::vector<Arc<PipelineGraph const>>{};
    result.reserve(visible_.size());
    for (auto const& entry : visible_.get<1>()) {
      result.push_back(entry.graph);
    }
    return result;
  }

  auto changed() const -> bool {
    return dirty_;
  }
  auto acknowledge_changes() -> void {
    dirty_ = false;
  }
  auto size() const -> size_t {
    return records_.size();
  }
  auto graph(size_t id) const -> Arc<PipelineGraph const> const& {
    return records_[id].graph;
  }
  auto instances(size_t id) const -> std::set<PlanRegistration*> const& {
    return records_[id].instances;
  }

private:
  struct Record {
    Arc<PipelineGraph const> graph;
    std::set<PlanRegistration*> instances;
  };
  struct Visible {
    Arc<PipelineGraph const> graph;
    size_t order;
    auto path() const -> PlanPath const& {
      return graph->path;
    }
  };

  auto include(size_t id) -> size_t {
    auto const& graph = records_[id].graph;
    if (auto found = visible_.find(graph->path); found != visible_.end()) {
      return found->order;
    }
    if (graph->parent) {
      include(graph->parent->back());
    }
    auto order = next_order_++;
    visible_.insert({graph, order});
    dirty_ = true;
    return order;
  }

  size_t limit_;
  size_t next_order_ = 0;
  bool dirty_ = false;
  std::vector<Record> records_;
  std::map<std::pair<Option<size_t>, size_t>, std::vector<size_t>> placements_;
  // Only finished visible layouts, ordered by when they became visible.
  std::map<size_t, size_t> finished_;
  boost::multi_index_container<
    Visible,
    boost::multi_index::indexed_by<
      boost::multi_index::ordered_unique<boost::multi_index::const_mem_fun<
        Visible, PlanPath const&, &Visible::path>>,
      boost::multi_index::ordered_unique<
        boost::multi_index::member<Visible, size_t, &Visible::order>>>>
    visible_;
};
} // namespace

struct InspectionState {
  explicit InspectionState(uuid process, size_t history_limit)
    : process_instance_id{process}, history{history_limit} {
  }
  // Runtime registration is synchronous. A command queue would require a
  // worker and acknowledgments; capture never takes this mutex. Delivery
  // releases it because a resumed consumer can call back into inspection.
  std::mutex mutex;
  uuid process_instance_id;
  Option<PipeId> execution_id;
  Option<ExecutionOutcome> end;
  Option<Arc<SourceMap const>> sources;
  GraphHistory history;
  GraphBroadcast graph_updates;
  // Layout ID and edge index address each logical channel directly.
  std::map<std::pair<size_t, size_t>, std::shared_ptr<TapBroadcast>> taps;
  Option<folly::CancellationSource> timer;
  bool initializing = false;
  bool dirty = false;

  auto has_taps() const -> bool {
    return std::ranges::any_of(taps, [](auto const& entry) {
      return entry.second->has_receivers();
    });
  }

  auto has_readers() const -> bool {
    return graph_updates.has_receivers();
  }

  auto observing() const -> bool {
    return initializing or has_readers() or has_taps();
  }

  auto clear_if_unobserved() -> void {
    if (not end and not observing()) {
      history.end_observation();
    }
  }

  auto subscribe() -> GraphSubscription {
    dirty = true;
    return graph_updates.subscribe();
  }

  auto snapshot() const -> Arc<GraphSnapshot const> {
    return GraphSnapshot{process_instance_id, execution_id, history.snapshot(),
                         end, sources};
  }

  auto publish(std::unique_lock<std::mutex>& lock) -> void {
    if ((not dirty and not history.changed()) or initializing) {
      return;
    }
    auto update = snapshot();
    history.acknowledge_changes();
    dirty = false;
    lock.unlock();
    auto relock = detail::scope_guard{[&]() noexcept {
      lock.lock();
    }};
    graph_updates.publish(update);
  }

  auto begin_observation(std::weak_ptr<InspectionState> self) -> void {
    clear_if_unobserved();
    if (timer or end) {
      return;
    }
    timer.emplace();
    folly::coro::co_withCancellation(
      timer->getToken(),
      folly::coro::co_withExecutor(folly::getGlobalCPUExecutor(),
                                   publish_loop(std::move(self))))
      .start([](auto&& result) {
        if (not result.template hasException<folly::OperationCancelled>()) {
          result.value();
        }
      });
  }

private:
  static auto publish_loop(std::weak_ptr<InspectionState> self) -> Task<void> {
    while (true) {
      co_await sleep_for(std::chrono::milliseconds{250});
      auto state = self.lock();
      if (not state) {
        co_return;
      }
      auto lock = std::unique_lock{state->mutex};
      if (state->end) {
        co_return;
      }
      std::erase_if(state->taps, [&](auto const& entry) {
        if (entry.second->has_receivers()) {
          return false;
        }
        auto [graph, edge] = entry.first;
        for (auto instance : state->history.instances(graph)) {
          instance->taps_[edge].broadcast_.store(nullptr);
        }
        return true;
      });
      state->clear_if_unobserved();
      if (not state->observing()) {
        state->timer = None{};
        co_return;
      }
      if (state->has_readers()) {
        state->publish(lock);
      }
    }
  }
};

namespace {
auto describe_plan(ir::Plan const& plan) -> PipelineGraph {
  auto graph = PipelineGraph{};
  graph.definition_location = plan.definition_location;
  for (auto const& op : plan.operators) {
    graph.operators.push_back(
      {op.id, op.op->display_name(), op.op->main_location()});
  }
  graph.channels = plan.channels;
  return graph;
}
} // namespace
} // namespace tenzir::detail

namespace tenzir {

auto PipelineObserver::ensure_graph(uint64_t id,
                                    std::unique_lock<std::mutex>& lock)
  -> Option<size_t> {
  auto found = live_.find(id);
  if (found == live_.end() or not state_->observing() or state_->end) {
    return None{};
  }
  auto& entry = *found->second;
  if (entry.graph_id_) {
    state_->history.observe_instance(*entry.graph_id_, entry);
    return entry.graph_id_;
  }
  // Description invokes operator code. Keep only owned plan data and IDs across
  // the unlock: HTTP discovery may race registration destruction.
  auto plan = entry.plan_;
  auto parent_id = entry.parent_.map([](auto parent) {
    return parent->id_;
  });
  auto parent_operator = entry.parent_operator_;
  auto graph = [&] {
    lock.unlock();
    auto relock = detail::scope_guard{[&]() noexcept {
      lock.lock();
    }};
    return detail::describe_plan(*plan);
  }();
  auto parent = parent_id.and_then([&](auto id) {
    return ensure_graph(id, lock);
  });
  found = live_.find(id);
  if (found == live_.end() or not state_->observing() or state_->end) {
    return None{};
  }
  // A live child keeps its parent registered, so ancestors cannot be pruned
  // between materialization and publishing this child.
  TENZIR_ASSERT(not parent_id or parent);
  auto graph_id = state_->history.add_instance(std::move(graph), parent,
                                               parent_operator, *found->second);
  found->second->graph_id_ = graph_id;
  attach_taps(*found->second);
  return graph_id;
}

auto PipelineObserver::initialize_graph(std::unique_lock<std::mutex>& lock)
  -> void {
  state_->initializing = true;
  auto initialized = detail::scope_guard{[&]() noexcept {
    state_->initializing = false;
  }};
  auto plans = std::vector<uint64_t>{};
  for (auto const& [id, _] : live_) {
    plans.push_back(id);
  }
  for (auto id : plans) {
    std::ignore = ensure_graph(id, lock);
  }
}

auto PipelineObserver::discover_instances(size_t graph_id,
                                          std::unique_lock<std::mutex>& lock)
  -> void {
  if (state_->end) {
    return;
  }
  auto graph = state_->history.graph(graph_id);
  if (graph->parent) {
    discover_instances(graph->parent->back(), lock);
    if (state_->end) {
      return;
    }
  }
  auto candidates = std::vector<uint64_t>{};
  auto append_children = [&](uint64_t parent, size_t op) {
    if (auto found = children_.find({parent, op}); found != children_.end()) {
      candidates.insert(candidates.end(), found->second.begin(),
                        found->second.end());
    }
  };
  if (graph->parent) {
    for (auto instance : state_->history.instances(graph->parent->back())) {
      append_children(instance->id_, *graph->parent_operator);
    }
  } else {
    append_children(0, 0);
  }
  for (auto id : candidates) {
    std::ignore = ensure_graph(id, lock);
  }
}

auto PipelineObserver::attach_taps(PlanRegistration& registration) -> void {
  TENZIR_ASSERT(registration.graph_id_);
  for (auto edge = size_t{0}; edge < registration.taps_.size(); ++edge) {
    auto found = state_->taps.find({*registration.graph_id_, edge});
    if (found != state_->taps.end()) {
      registration.taps_[edge].broadcast_.store(found->second);
    }
  }
}

auto PipelineObserver::finish(ExecutionOutcome outcome) -> void {
  auto lock = std::unique_lock{state_->mutex};
  TENZIR_ASSERT(live_.empty());
  if (state_->end) {
    return;
  }
  state_->end = outcome;
  auto final = state_->snapshot();
  state_->history.finish();
  auto taps = std::exchange(state_->taps, {});
  auto timer = std::exchange(state_->timer, None{});
  auto state = state_;
  lock.unlock();
  if (timer) {
    timer->requestCancellation();
  }
  state->graph_updates.close(std::move(final));
  // Runtime work has joined, so accepted batches precede the terminal outcome.
  for (auto const& [_, channel] : taps) {
    channel->close(outcome);
  }
}

auto PipelineObserver::start(PipeId execution) -> void {
  auto lock = std::unique_lock{state_->mutex};
  TENZIR_ASSERT(not state_->execution_id and not state_->end);
  state_->execution_id = std::move(execution);
  state_->dirty = true;
}
auto PipelineObserver::publish_sources(SourceMap sources) -> void {
  auto owned = Arc<SourceMap const>{std::move(sources)};
  auto lock = std::unique_lock{state_->mutex};
  TENZIR_ASSERT(not state_->sources and not state_->end);
  state_->sources = std::move(owned);
  state_->dirty = true;
}
auto PipelineObserver::register_plan(Arc<ir::Plan const> plan,
                                     Option<Ref<PlanRegistration>> parent,
                                     size_t parent_operator)
  -> Box<PlanRegistration> {
  // Release the lock before the registration if metadata construction throws.
  auto owned = Option<Box<PlanRegistration>>{};
  auto lock = std::unique_lock{state_->mutex};
  TENZIR_ASSERT(state_->execution_id and not state_->end);
  owned.emplace(std::in_place, *this, next_registration_id_++, std::move(plan),
                parent, parent_operator);
  auto& registration = *owned;
  state_->clear_if_unobserved();
  live_.emplace(registration->id_, Ref{*registration});
  children_[{parent ? (*parent)->id_ : 0, parent ? parent_operator : 0}].insert(
    registration->id_);
  if (state_->observing()) {
    std::ignore = ensure_graph(registration->id_, lock);
  }
  return std::move(registration);
}

auto PipelineObserver::unregister_plan(PlanRegistration& registration) -> void {
  auto lock = std::unique_lock{state_->mutex};
  // A child borrows its parent registration, including during retirement.
  auto child = children_.lower_bound({registration.id_, 0});
  TENZIR_ASSERT(child == children_.end()
                or child->first.first != registration.id_);
  auto key
    = std::pair{registration.parent_ ? (*registration.parent_)->id_ : 0,
                registration.parent_ ? registration.parent_operator_ : 0};
  if (auto found = children_.find(key); found != children_.end()) {
    found->second.erase(registration.id_);
    if (found->second.empty()) {
      children_.erase(found);
    }
  }
  live_.erase(registration.id_);
  if (registration.graph_id_) {
    state_->history.remove_instance(*registration.graph_id_, registration);
  }
  state_->clear_if_unobserved();
}

PlanRegistration::~PlanRegistration() {
  observer_.unregister_plan(*this);
}

PipelineObserver::PipelineObserver(uuid process, InspectionOptions options)
  : state_{std::in_place, process, options.finished_pipeline_graphs},
    tap_capacity_{options.tap_capacity} {
  TENZIR_ASSERT(options.tap_capacity > 0
                and options.tap_capacity
                      <= std::numeric_limits<uint32_t>::max());
}
PipelineObserver::~PipelineObserver() {
  finish(ExecutionOutcome::cancelled);
}
auto PipelineObserver::subscribe_graph()
  -> Result<GraphSubscription, GraphSubscriptionError> {
  auto lock = std::unique_lock{state_->mutex};
  if (not state_->execution_id and not state_->end) {
    return Err{GraphSubscriptionError::not_started};
  }
  state_->begin_observation(Arc{state_}.into_shared());
  if (not state_->end and not state_->initializing
      and not state_->has_readers()) {
    initialize_graph(lock);
  }
  return state_->subscribe();
}
auto PipelineObserver::subscribe_channel(ChannelTarget target)
  -> Result<TapSubscription, TapSubscriptionError> {
  auto lock = std::unique_lock{state_->mutex};
  if (state_->end) {
    return Err{TapSubscriptionError::finished};
  }
  if (not state_->execution_id) {
    return Err{TapSubscriptionError::not_started};
  }
  if (target.process_instance_id != state_->process_instance_id
      or target.execution_id != *state_->execution_id) {
    return Err{TapSubscriptionError::wrong_execution};
  }
  if (target.path.empty() or target.path.back() >= state_->history.size()) {
    return Err{TapSubscriptionError::invalid_target};
  }
  auto const& layout = state_->history.graph(target.path.back());
  if (layout->path != target.path
      or target.channel_index >= layout->channels.size()) {
    return Err{TapSubscriptionError::invalid_target};
  }
  auto const& type = layout->channels[target.channel_index].type;
  if (not type.is<table_slice>() and not type.is<nova::Events>()) {
    return Err{TapSubscriptionError::not_events};
  }
  state_->begin_observation(Arc{state_}.into_shared());
  auto key = std::pair{target.path.back(), target.channel_index};
  auto found = state_->taps.find(key);
  if (found == state_->taps.end()) {
    found = state_->taps
              .emplace(key, std::make_shared<detail::TapBroadcast>(
                              static_cast<uint32_t>(tap_capacity_)))
              .first;
  }
  auto channel = found->second;
  auto subscription = channel->subscribe();
  for (auto registration : state_->history.instances(target.path.back())) {
    registration->taps_[target.channel_index].broadcast_.store(channel);
  }
  discover_instances(target.path.back(), lock);
  if (state_->end) {
    return Err{TapSubscriptionError::finished};
  }
  return subscription;
}

PlanRegistration::PlanRegistration(PipelineObserver& observer, uint64_t id,
                                   Arc<ir::Plan const> plan,
                                   Option<Ref<PlanRegistration>> parent,
                                   size_t parent_operator)
  : observer_{observer},
    id_{id},
    plan_{std::move(plan)},
    parent_{parent},
    parent_operator_{parent_operator},
    taps_(plan_->channels.size()) {
}
auto PlanRegistration::plan() const -> ir::Plan const& {
  return *plan_;
}
auto PlanRegistration::tap(size_t edge) const -> Ref<ChannelTap const> {
  TENZIR_ASSERT(edge < taps_.size());
  return Ref{taps_[edge]};
}
auto ChannelTap::capture(table_slice const& batch) const -> void {
  if (auto channel = broadcast_.load(std::memory_order_acquire);
      channel and channel->has_receivers()) {
    channel->publish(EventBatch{batch});
  }
}
auto ChannelTap::capture(nova::Events const& batch) const -> void {
  if (auto channel = broadcast_.load(std::memory_order_acquire);
      channel and channel->has_receivers()) {
    channel->publish(EventBatch{batch});
  }
}

} // namespace tenzir
