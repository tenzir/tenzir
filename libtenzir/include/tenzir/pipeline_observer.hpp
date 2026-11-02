//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/broadcast.hpp"
#include "tenzir/async/channel_tap.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/uuid.hpp"

#include <compare>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace tenzir {

class PipelineObserver;

struct GraphOperator {
  OpId id;
  std::string name;
  location source;

  auto operator<=>(GraphOperator const&) const = default;
};

/// An execution-local path: a root layout ID followed by pairs of owning
/// operator index and child layout ID. IDs are allocated by comparing source
/// locations and graph structure, and remain reserved until execution ends.
/// Operator argument values are excluded; unknown locations compare equal.
/// Targets carry only these integer components, not hashes or embedded plan
/// descriptions.
using PlanPath = std::vector<size_t>;

/// Scoped to an execution, never to its profiling IDs or parallel lanes.
struct ChannelTarget {
  uuid process_instance_id;
  PipeId execution_id;
  PlanPath path;
  size_t channel_index = 0;

  auto operator==(ChannelTarget const&) const -> bool = default;
};

/// Describes operators and channels shared by matching runtime plans.
/// Matching uses parent placement, definition location, and operator/channel
/// metadata. It does not establish semantic equivalence between plans.
struct PipelineGraph {
  PlanPath path;
  Option<PlanPath> parent;
  Option<size_t> parent_operator;
  location definition_location = location::unknown;
  std::vector<GraphOperator> operators;
  /// Indices refer to `operators`; boundary endpoints use `ir::Port`.
  /// The vector index is `ChannelTarget::channel_index`.
  std::vector<ir::Channel> channels;
};

struct GraphSnapshot {
  uuid process_instance_id;
  Option<PipeId> execution_id;
  std::vector<Arc<PipelineGraph const>> graphs;
  Option<ExecutionOutcome> end;
  Option<Arc<SourceMap const>> sources;
};

enum class GraphSubscriptionError { not_started };

enum class TapSubscriptionError {
  not_started,
  invalid_target,
  wrong_execution,
  not_events,
  finished,
};

struct InspectionOptions {
  size_t tap_capacity = 4;
  /// Bounds visible finished nested graphs, not the identity catalog. Exact
  /// descriptions remain reserved until execution completion so recurring
  /// layouts keep their channel targets, even after history eviction.
  size_t finished_pipeline_graphs = 256;
};

namespace detail {
struct InspectionState;
using GraphBroadcast
  = Broadcast<Arc<GraphSnapshot const>, BroadcastMode::latest>;
} // namespace detail

/// Latest snapshots, followed by a final snapshot and EOF.
using GraphSubscription = detail::GraphBroadcast::Receiver;
/// Accepted native batches, followed by an execution outcome and EOF.
using TapSubscription = detail::TapBroadcast::Receiver;

/// Runtime ownership extends through the last channel receive. A parent
/// registration must outlive its children, including their output bridges.
class PlanRegistration {
public:
  PlanRegistration(PipelineObserver& observer, uint64_t id,
                   Arc<ir::Plan const> plan,
                   Option<Ref<PlanRegistration>> parent,
                   size_t parent_operator);
  ~PlanRegistration();
  PlanRegistration(PlanRegistration&&) = delete;
  auto operator=(PlanRegistration&&) -> PlanRegistration& = delete;
  PlanRegistration(PlanRegistration const&) = delete;
  auto operator=(PlanRegistration const&) -> PlanRegistration& = delete;
  auto plan() const -> ir::Plan const&;
  auto tap(size_t edge) const -> Ref<ChannelTap const>;

private:
  friend class PipelineObserver;
  friend struct detail::InspectionState;
  PipelineObserver& observer_;
  uint64_t id_;
  Arc<ir::Plan const> plan_;
  Option<Ref<PlanRegistration>> parent_;
  size_t parent_operator_;
  std::vector<ChannelTap> taps_;
  Option<size_t> graph_id_;
};

/// Inspection for one execution. Must outlive its runtime registrations;
/// returned subscriptions own delivery queues separately.
///
/// The executor calls start(), publishes sources, and owns one
/// PlanRegistration per running plan. live_ borrows these registrations by
/// runtime ID; children_ indexes their parent instance and operator even before
/// their graphs are described. Destruction reports retirement to the observer.
///
/// state_ owns execution metadata, graph history, channel broadcasts, and
/// delivery. Its private graph history owns stable identities, live instance
/// membership, visibility, and eviction together. Graph IDs are catalog
/// indexes; matching compares descriptions only at the same parent graph
/// ID/operator. Retiring the last instance enforces the finished-history limit.
/// Stopping observation clears visible history, but identities survive until
/// completion.
///
/// The first graph reader discovers all live plans. A channel request instead
/// validates its full target, then follows children_ along that graph's
/// ancestry to discover matching instances created during an unobserved gap.
/// Only candidate placements on that ancestry are described. Opening the graph
/// later still performs full discovery. Subsequent registrations are described
/// while readers or taps exist, and inherit the subscribers of their matching
/// logical channels.
///
/// Each logical channel has one broadcast shared by its physical instances.
/// The broadcast owns subscriber management; dropping or cancelling a receiver
/// detaches it immediately. Capture publishes native batches without locking
/// the observer; full queues drop inspection copies. The graph broadcast
/// instead replaces pending snapshots and orders completion after publication.
///
/// Every 250 ms, changed history is copied to an ordered vector of immutable
/// graph references. Producer lookup indexes stay private; readers receive only
/// GraphSnapshot values. Unread snapshots are replaced. All metadata and
/// history changes use state_->mutex. Description callbacks and delivery run
/// unlocked; runtime IDs allow rechecking liveness after a description
/// callback.
///
/// One periodic task starts on the first subscription and stops on the first
/// tick without subscribers, restarting when observation resumes. Completion
/// cancels it immediately. It holds state_ only during a tick, not while
/// waiting. Subscriptions retain queues, not execution state. After all
/// registrations are destroyed, finish() releases the identity catalog,
/// publishes final history, and closes taps after accepted batches. The
/// observer must outlive registrations, but not draining readers.
class PipelineObserver {
public:
  explicit PipelineObserver(uuid process_instance_id, InspectionOptions options
                                                      = {});
  ~PipelineObserver();
  PipelineObserver(PipelineObserver const&) = delete;
  auto operator=(PipelineObserver const&) -> PipelineObserver& = delete;

  /// Rejects before startup; completed executions return the retained final
  /// snapshot. Without prior inspection, it may contain no pipeline graphs.
  auto subscribe_graph() -> Result<GraphSubscription, GraphSubscriptionError>;
  /// Validates against reserved identities, including evicted graph layouts.
  auto subscribe_channel(ChannelTarget target)
    -> Result<TapSubscription, TapSubscriptionError>;

  /// Execution-side operations. Start once, register plans before receiving
  /// on their channels, and finish only after all runtime work has joined and
  /// its registrations have been destroyed.
  auto start(PipeId execution) -> void;
  auto publish_sources(SourceMap sources) -> void;
  auto register_plan(Arc<ir::Plan const> plan,
                     Option<Ref<PlanRegistration>> parent = None{},
                     size_t parent_operator = 0) -> Box<PlanRegistration>;
  auto finish(ExecutionOutcome outcome) -> void;

private:
  friend class PlanRegistration;
  auto ensure_graph(uint64_t registration, std::unique_lock<std::mutex>& lock)
    -> Option<size_t>;
  auto initialize_graph(std::unique_lock<std::mutex>& lock) -> void;
  auto attach_taps(PlanRegistration& registration) -> void;
  auto discover_instances(size_t graph, std::unique_lock<std::mutex>& lock)
    -> void;
  auto unregister_plan(PlanRegistration& registration) -> void;

  // Owns history and delivery; shared with an in-flight timer callback only.
  Arc<detail::InspectionState> state_;
  size_t tap_capacity_;
  // Protected by state_->mutex. Subscriptions only access published
  // graph data; no subscription retains these execution-owned registries.
  std::map<uint64_t, Ref<PlanRegistration>> live_;
  // Runtime parent ID and operator -> children, including undescribed plans.
  // Parent ID zero denotes roots; real registration IDs start at one.
  std::map<std::pair<uint64_t, size_t>, std::set<uint64_t>> children_;
  // Runtime instance IDs, distinct from graph IDs; never reused after unlock.
  uint64_t next_registration_id_ = 1;
};

} // namespace tenzir
