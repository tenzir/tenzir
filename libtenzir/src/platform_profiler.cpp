// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/async/scope.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/controller/http_transport.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/os.hpp>
#include <tenzir/platform_profiler.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tunnel/platform.hpp>
#include <tenzir/uuid.hpp>

#include <folly/coro/DetachOnCancel.h>
#include <folly/coro/Retry.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>

#include <algorithm>
#include <deque>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <tuple>
#include <utility>
#include <vector>

namespace tenzir {
namespace {

constexpr auto max_batch_bytes = std::size_t{256} << 10;
// The platform refuses a batch with more rows than this for any one of its
// tables: pipeline samples, connector flows (the sources and sinks of all
// pipeline samples together), diagnostics, run transitions, deployment samples
// and served requests each count on their own.
constexpr auto max_batch_rows = std::size_t{10'000};
constexpr auto max_diagnostic_bytes = std::size_t{16} << 10;
// How long a batch waits for the platform to take it, as it does while a
// rollout of the platform takes it down. Past it, the batch counts as dropped.
constexpr auto max_batch_age = std::chrono::minutes{2};
// The wait between attempts to deliver a batch: no shorter than the interval
// at which batches are sent, so that retrying never sends faster than a
// healthy deployment does, and then every few seconds while the platform is
// down.
constexpr auto min_retry_delay = std::chrono::seconds{1};
constexpr auto max_retry_delay = std::chrono::seconds{5};
// How long one attempt waits for the platform to answer. One batch is in
// flight at a time, so a sender that waits for the answer slows down with the
// platform rather than adding to its load. Well past the platform's usual
// latency, which is a few hundred milliseconds even under load; the final
// flush has its own, shorter deadline.
constexpr auto request_timeout = std::chrono::seconds{10};
// Room for the batches of `max_batch_age` at one a second, so that an outage
// costs no resolution while they wait. At `max_batch_bytes` each, they take
// at most 32 MiB, though a batch is usually a few KiB.
constexpr auto max_pending_batches = uint32_t{128};
constexpr auto max_wire_count = uint64_t{9'007'199'254'740'991};
// Ample for a deployment the platform calls a few times a second, and bounded
// for one that something floods: past it, a request counts as dropped.
constexpr auto max_pending_requests = std::size_t{1000};
// Distinct diagnostics of one run that wait for a batch. Repeats fold into the
// one pending with the same fingerprint, and take no room of their own.
constexpr auto max_pending_diagnostics = std::size_t{256};
constexpr auto max_pending_transitions = std::size_t{8};
// How often the process reports what it uses. The platform takes the rate of
// CPU time between two samples at most a minute apart.
constexpr auto process_sample_interval = std::chrono::seconds{10};

// One sequence across all senders in the process, including retries.
auto next_sequence() -> uint64_t {
  static auto sequence = Atomic<uint64_t>{0};
  auto value = sequence.fetch_add(1, std::memory_order_relaxed);
  TENZIR_ASSERT(value <= max_wire_count, "telemetry sequence exhausted");
  return value;
}

auto boot() -> Uuid const& {
  static auto const value
    = Uuid::make(fmt::format("{}", uuid::random())).expect("a UUID");
  return value;
}

template <class Count>
auto count(uint64_t value) -> Count {
  return Count::make(static_cast<int64_t>(std::min(value, max_wire_count)))
    .expect("bounded telemetry count");
}

/// Seconds on the wire, which never go backwards.
auto seconds_of(duration value) -> double {
  return std::chrono::duration<double>{std::max(value, duration{})}.count();
}

auto note_kind(diagnostic_note_kind value) -> std::string {
  switch (value) {
    case diagnostic_note_kind::note:
      return "note";
    case diagnostic_note_kind::usage:
      return "usage";
    case diagnostic_note_kind::hint:
      return "hint";
    case diagnostic_note_kind::docs:
      return "docs";
  }
  TENZIR_UNREACHABLE();
}

/// Sends the batches of one reporter, bounded and best effort.
///
/// A batch that does not fit, or that does not arrive, counts as dropped along
/// with the drops it reported, and the next batch reports them all.
class Uplink {
public:
  struct Pending {
    std::string body;
    std::chrono::steady_clock::time_point created;
    uint64_t dropped;
  };

  Uplink(PlatformConnection connection, Option<Box<Transport>> transport)
    : connection{std::move(connection)},
      transport{std::move(transport)},
      discover_transport{not this->transport} {
  }

  /// Whether another batch would find no room. Then what is pending waits in
  /// its own bounded queues, where samples that overflow merge and other
  /// entries count as dropped one by one, rather than in a batch that would be
  /// dropped whole as one.
  auto full() const -> bool {
    return queue.size() >= max_pending_batches;
  }

  /// Queues `batch`, which reports `lost` drops taken from `dropped`.
  auto enqueue(Batch const& batch, uint64_t lost) const -> void {
    auto body = std::string{};
    write_json(batch, body);
    if (body.size() > max_batch_bytes
        or not queue.try_enqueue(
          Pending{std::move(body), std::chrono::steady_clock::now(), lost})) {
      dropped.fetch_add(lost + 1, std::memory_order_relaxed);
    }
  }

  auto transmit(Pending const& pending) const -> Task<Option<bool>> {
    if (not transport) {
      auto discovered = co_await discover_api_url(connection.platform_url,
                                                  {std::chrono::seconds{1},
                                                   std::chrono::seconds{2}});
      if (discovered.is_err()) {
        TENZIR_WARN("platform telemetry discovery failed");
        co_return {};
      }
      auto key = co_await folly::coro::detachOnCancel(
        spawn_blocking([path = connection.key_file] {
          return read_deployment_key(path);
        }));
      if (not key) {
        TENZIR_WARN("platform telemetry key file is unavailable or invalid");
        co_return false;
      }
      transport
        = Box<Transport>::from_non_null(std::make_unique<BufferedHttpTransport>(
          std::move(discovered).unwrap(), std::move(*key), request_timeout));
    }
    auto identity = co_await connection.identity();
    if (identity.is_err()) {
      TENZIR_WARN("platform telemetry could not learn the deployment's "
                  "workspace: {}",
                  std::move(identity).unwrap_err());
      co_return {};
    }
    auto& sender = *transport;
    auto response = co_await sender->request(
      HttpRequest{"POST",
                  fmt::format("{}/telemetry", identity.unwrap().route()),
                  pending.body,
                  {{"content-type", "application/json"}}});
    if (response.is_err()) {
      // Discover the platform again only when it may have moved. A timeout
      // says it is slow, and discovering it again would only add a request.
      if (discover_transport
          and response.unwrap_err().kind != RequestError::Kind::timeout) {
        transport.reset();
      }
      co_return {};
    }
    auto status = response.unwrap().status;
    if (status == 204) {
      co_return true;
    }
    if (status == 429 or status >= 500) {
      co_return {};
    }
    TENZIR_WARN("platform telemetry rejected with HTTP {}", status);
    if (discover_transport and (status == 401 or status == 403)) {
      // Refresh a rotated deployment key for the next batch, not this rejected
      // one. Injected transports retain ownership of their own reconnection.
      transport.reset();
    }
    co_return false;
  }

  /// Retries `pending` until the platform answers for good or the batch is
  /// older than `max_batch_age`. The jitter keeps deployments from retrying in
  /// lockstep when the platform comes back.
  auto deliver(Pending const& pending) const -> Task<bool> {
    struct RetryTelemetry : std::exception {};
    try {
      co_return co_await folly::coro::retryWithExponentialBackoff(
        std::numeric_limits<uint32_t>::max(), min_retry_delay, max_retry_delay,
        0.1,
        [this, &pending]() -> Task<bool> {
          if (std::chrono::steady_clock::now() - pending.created
              > max_batch_age) {
            co_return false;
          }
          auto result = co_await transmit(pending);
          if (result) {
            co_return *result;
          }
          co_yield folly::coro::co_error(RetryTelemetry{});
        },
        [](folly::exception_wrapper const& error) {
          return error.is_compatible_with<RetryTelemetry>();
        });
    } catch (RetryTelemetry const&) {
      co_return false;
    }
  }

  auto send() const -> Task<void> {
    while (true) {
      if (not in_flight) {
        in_flight = co_await queue.dequeue();
      }
      if (not co_await deliver(*in_flight)) {
        dropped.fetch_add(in_flight->dropped + 1, std::memory_order_relaxed);
      }
      in_flight = None{};
    }
  }

  /// Delivers what is queued, with `prepare` folding what is pending into
  /// batches as room is made.
  template <class Prepare>
  auto flush(Prepare prepare) const -> Task<void> {
    auto operation = folly::coro::co_invoke([this, &prepare]() -> Task<void> {
      // Make room before folding the final transition or drop report.
      prepare();
      auto deadline
        = std::chrono::steady_clock::now() + std::chrono::seconds{3};
      while (std::chrono::steady_clock::now() < deadline) {
        auto pending = std::exchange(in_flight, None{});
        if (not pending) {
          pending = queue.try_dequeue();
        }
        if (not pending) {
          if (dropped.load(std::memory_order_relaxed) == 0) {
            co_return;
          }
          prepare();
          continue;
        }
        auto sent = false;
        auto account = detail::scope_guard{[&]() noexcept {
          if (not sent) {
            dropped.fetch_add(pending->dropped + 1, std::memory_order_relaxed);
          }
        }};
        sent = co_await deliver(*pending);
        if (not sent) {
          // No busy loop manufacturing drop-report batches while offline.
          co_return;
        }
        prepare();
      }
      if (not queue.empty()) {
        TENZIR_WARN("platform telemetry final flush expired with {} queued "
                    "batches",
                    queue.size());
      }
    });
    try {
      co_await folly::coro::timeout(std::move(operation),
                                    std::chrono::seconds{3});
    } catch (folly::FutureTimeout const&) {
      TENZIR_WARN("platform telemetry final flush timed out");
    }
  }

  PlatformConnection connection;
  mutable BoundedQueue<Pending> queue{max_pending_batches};
  // send and flush run sequentially; cancellation leaves this batch first.
  mutable Option<Pending> in_flight;
  mutable Atomic<uint64_t> dropped{0};
  mutable Option<Box<Transport>> transport;
  bool discover_transport;
};

auto add(TelemetryCount const& a, TelemetryCount const& b) -> TelemetryCount {
  return count<TelemetryCount>(static_cast<uint64_t>(a.value())
                               + static_cast<uint64_t>(b.value()));
}

auto add(TelemetrySeconds const& a, TelemetrySeconds const& b)
  -> TelemetrySeconds {
  return TelemetrySeconds::make(a.value() + b.value()).expect("finite seconds");
}

/// Adds `later` into `earlier`, as one sample would have reported both: the
/// counters and the CPU time add up over the combined interval, and the queue
/// is as `later` found it.
auto merge(PipelineSample& earlier, PipelineSample later) -> void {
  auto merge_flows
    = [](std::vector<ConnectorFlow>& into, std::vector<ConnectorFlow> from) {
        for (auto& flow : from) {
          auto found = std::ranges::find(into, flow.connector,
                                         &ConnectorFlow::connector);
          if (found == into.end()) {
            into.push_back(std::move(flow));
            continue;
          }
          found->events = add(found->events, flow.events);
          found->bytes = add(found->bytes, flow.bytes);
        }
      };
  merge_flows(earlier.sources, std::move(later.sources));
  merge_flows(earlier.sinks, std::move(later.sinks));
  earlier.window = later.window;
  // Resources come all together or not at all.
  if (not later.cpu_seconds) {
    return;
  }
  if (not earlier.cpu_seconds) {
    earlier.cpu_seconds = std::move(later.cpu_seconds);
    earlier.interval_seconds = std::move(later.interval_seconds);
    earlier.queued_bytes = std::move(later.queued_bytes);
    earlier.activity = std::move(later.activity);
    return;
  }
  earlier.cpu_seconds = add(*earlier.cpu_seconds, *later.cpu_seconds);
  if (earlier.interval_seconds and later.interval_seconds) {
    earlier.interval_seconds
      = add(*earlier.interval_seconds, *later.interval_seconds);
  }
  if (earlier.activity and later.activity) {
    earlier.activity = add(*earlier.activity, *later.activity);
  }
  earlier.queued_bytes = std::move(later.queued_bytes);
}

/// What a batch takes on the wire besides its entries, at most.
auto envelope_bytes() -> std::size_t {
  static auto const value = [] {
    auto widest = count<TelemetryCount>(max_wire_count);
    auto body = std::string{};
    write_json(Batch{boot(), widest, widest, {}, {}, {}, {}, {}}, body);
    return body.size();
  }();
  return value;
}

/// A batch in the making, which takes entries for as long as it stays within
/// the platform's limits: the bytes of its body and the rows of each table.
class Draft {
public:
  auto empty() const -> bool {
    return pipelines_.empty() and diagnostics_.empty() and runs_.empty()
           and deployment_.empty() and requests_.empty();
  }

  /// Takes `sample` if it fits, and leaves it as it is otherwise.
  auto add(PipelineSample& sample) -> bool {
    auto flows = sample.sources.size() + sample.sinks.size();
    if (pipelines_.size() >= max_batch_rows or flows_ + flows > max_batch_rows
        or not fits(sample)) {
      return false;
    }
    flows_ += flows;
    pipelines_.push_back(std::move(sample));
    return true;
  }

  auto add(DiagnosticSample& sample) -> bool {
    return add_to(diagnostics_, sample);
  }

  auto add(RunTransition& transition) -> bool {
    return add_to(runs_, transition);
  }

  auto add(ServedRequest& request) -> bool {
    return add_to(requests_, request);
  }

  auto add(DeploymentSample& sample) -> bool {
    return add_to(deployment_, sample);
  }

  /// The batch, as the next in the process's sequence, reporting `lost`.
  auto finish(uint64_t lost) && -> Batch {
    return Batch{boot(),
                 count<TelemetryCount>(next_sequence()),
                 count<TelemetryCount>(lost),
                 std::move(pipelines_),
                 std::move(diagnostics_),
                 std::move(runs_),
                 std::move(deployment_),
                 std::move(requests_)};
  }

private:
  template <class T>
  auto add_to(std::vector<T>& entries, T& entry) -> bool {
    if (entries.size() >= max_batch_rows or not fits(entry)) {
      return false;
    }
    entries.push_back(std::move(entry));
    return true;
  }

  /// Counts `entry` against the size of the body if it fits.
  template <class T>
  auto fits(T const& entry) -> bool {
    scratch_.clear();
    write_json(entry, scratch_);
    // With the separator before it, which the first entry of an array lacks.
    auto size = scratch_.size() + 1;
    if (bytes_ + size > max_batch_bytes) {
      return false;
    }
    bytes_ += size;
    return true;
  }

  std::size_t bytes_ = envelope_bytes();
  std::size_t flows_ = 0;
  std::vector<PipelineSample> pipelines_;
  std::vector<DiagnosticSample> diagnostics_;
  std::vector<RunTransition> runs_;
  std::vector<DeploymentSample> deployment_;
  std::vector<ServedRequest> requests_;
  std::string scratch_;
};

/// Moves entries from the front of `pending` into `draft` while they fit, and
/// tells whether all of them did. One that does not fit even an empty batch
/// never will, and counts as dropped.
template <class Pending, class Front, class Pop>
auto take(Pending& pending, Draft& draft, Atomic<uint64_t>& dropped,
          Front front, Pop pop) -> bool {
  while (not pending.empty()) {
    if (not draft.add(front(pending))) {
      if (not draft.empty()) {
        return false;
      }
      dropped.fetch_add(1, std::memory_order_relaxed);
    }
    pop(pending);
  }
  return true;
}

template <class T>
auto take(std::deque<T>& pending, Draft& draft, Atomic<uint64_t>& dropped)
  -> bool {
  return take(
    pending, draft, dropped,
    [](std::deque<T>& entries) -> T& {
      return entries.front();
    },
    [](std::deque<T>& entries) {
      entries.pop_front();
    });
}

template <class T>
auto take(std::map<std::string, T>& pending, Draft& draft,
          Atomic<uint64_t>& dropped) -> bool {
  return take(
    pending, draft, dropped,
    [](std::map<std::string, T>& entries) -> T& {
      return entries.begin()->second;
    },
    [](std::map<std::string, T>& entries) {
      entries.erase(entries.begin());
    });
}

/// What one run recorded that no batch has taken yet, and the counters that
/// its samples are the differences of.
class RunReport {
public:
  RunReport(PipelineId id, TelemetryCount run)
    : id{std::move(id)}, run{std::move(run)} {
  }

  /// Records the connector counters, and what the operators cost if known.
  auto sample(std::span<MetricsSnapshotEntry const> counters,
              Option<PipelineResources> resources) const -> void {
    auto lock = std::scoped_lock{mutex};
    if (not counters.empty()) {
      totals.clear();
      all_events = 0;
      for (auto const& entry : counters) {
        if (entry.instrument == MetricsInstrument::counter
            and entry.type == MetricsUnit::events) {
          all_events += entry.value;
        }
        if (entry.visibility != MetricsVisibility::external_
            or entry.instrument != MetricsInstrument::counter
            or entry.connector.empty()) {
          continue;
        }
        auto& [events, bytes] = totals[{entry.direction, entry.connector}];
        auto& value = entry.type == MetricsUnit::events ? events : bytes;
        value += entry.value;
      }
    }
    auto sources = std::vector<ConnectorFlow>{};
    auto sinks = std::vector<ConnectorFlow>{};
    for (auto const& [key, value] : totals) {
      auto& previous = baseline[key];
      auto events = value.first >= previous.first ? value.first - previous.first
                                                  : value.first;
      auto bytes = value.second >= previous.second
                     ? value.second - previous.second
                     : value.second;
      previous = value;
      if (events == 0 and bytes == 0) {
        continue;
      }
      auto& flows = key.first == MetricsDirection::read ? sources : sinks;
      flows.emplace_back(key.second, count<TelemetryCount>(events),
                         count<TelemetryCount>(bytes));
    }
    // Counters never go away, so their sum only shrinks if one started over.
    auto activity = all_events >= activity_baseline
                      ? all_events - activity_baseline
                      : all_events;
    activity_baseline = all_events;
    if (sources.empty() and sinks.empty() and not resources) {
      return;
    }
    auto window = floor(time::clock::now(), std::chrono::milliseconds{1});
    // ClickHouse and replay cursors retain milliseconds, not finer precision.
    if (last_sample) {
      window = std::max(window, *last_sample + std::chrono::milliseconds{1});
    }
    last_sample = window;
    auto cpu = Option<TelemetrySeconds>{};
    auto interval = Option<TelemetrySeconds>{};
    auto queued = Option<TelemetryCount>{};
    auto moved = Option<TelemetryCount>{};
    if (resources) {
      moved = count<TelemetryCount>(activity);
      cpu = TelemetrySeconds::make(seconds_of(resources->cpu))
              .expect("finite CPU time");
      interval = TelemetrySeconds::make(seconds_of(resources->interval))
                   .expect("finite interval");
      queued = count<TelemetryCount>(resources->queued_bytes);
    }
    auto sample = PipelineSample{id,
                                 run,
                                 window,
                                 std::move(sources),
                                 std::move(sinks),
                                 std::move(cpu),
                                 std::move(interval),
                                 std::move(queued),
                                 std::move(moved)};
    if (samples.size() < PlatformProfiler::max_pending_samples) {
      samples.push_back(std::move(sample));
    } else {
      merge(samples.back(), std::move(sample));
    }
  }

  /// Folds `sample` into the pending one with the same fingerprint, if any.
  auto diagnostic(DiagnosticSample sample, Atomic<uint64_t>& dropped) const
    -> void {
    auto lock = std::scoped_lock{mutex};
    auto found = diagnostics.find(sample.fingerprint);
    if (found != diagnostics.end()) {
      auto& pending = found->second;
      pending.first_seen = std::min(pending.first_seen, sample.first_seen);
      pending.last_seen = std::max(pending.last_seen, sample.last_seen);
      pending.occurrences = count<TelemetrySmallCount>(
        std::min(pending.occurrences.value() + sample.occurrences.value(),
                 int64_t{4'294'967'295}));
      return;
    }
    if (diagnostics.size() >= max_pending_diagnostics) {
      dropped.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    auto fingerprint = sample.fingerprint;
    diagnostics.emplace(std::move(fingerprint), std::move(sample));
  }

  auto transition(RunTransition value, Atomic<uint64_t>& dropped) const
    -> void {
    auto lock = std::scoped_lock{mutex};
    if (transitions.size() >= max_pending_transitions) {
      dropped.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    transitions.push_back(std::move(value));
  }

  /// Discards what is pending, and tells how many entries that was.
  auto discard() const -> std::size_t {
    auto lock = std::scoped_lock{mutex};
    auto discarded = transitions.size() + samples.size() + diagnostics.size();
    transitions.clear();
    samples.clear();
    diagnostics.clear();
    return discarded;
  }

  /// Moves what is pending into `draft` while it fits, and tells whether all
  /// of it did. Transitions go first, being few and what the platform shows
  /// a run's state by, then the samples in their order, then the diagnostics.
  auto take(Draft& draft, Atomic<uint64_t>& dropped) const -> bool {
    auto lock = std::scoped_lock{mutex};
    return tenzir::take(transitions, draft, dropped)
           and tenzir::take(samples, draft, dropped)
           and tenzir::take(diagnostics, draft, dropped);
  }

  PipelineId const id;
  TelemetryCount const run;
  /// Set once the last `PlatformProfiler` of the run is gone, after which the
  /// run records nothing more.
  mutable Atomic<bool> closed{false};

private:
  mutable std::mutex mutex;
  // Guarded by `mutex`. The totals are the counters' latest, the baselines
  // what the samples carried.
  mutable std::deque<PipelineSample> samples;
  mutable std::map<std::string, DiagnosticSample> diagnostics;
  mutable std::deque<RunTransition> transitions;
  mutable Option<time> last_sample;
  mutable std::map<std::pair<MetricsDirection, std::string>,
                   std::pair<uint64_t, uint64_t>>
    totals;
  mutable std::map<std::pair<MetricsDirection, std::string>,
                   std::pair<uint64_t, uint64_t>>
    baseline;
  /// The sum of all event counters: what all sources read and all sinks
  /// wrote, external or internal, in both directions together. It is the
  /// activity of a pipeline, which also shows one that has no external
  /// connectors at work.
  mutable uint64_t all_events = 0;
  mutable uint64_t activity_baseline = 0;
};

/// When the batch after `now` is due: half a second into a second, when the
/// runs have sampled at its start. A wake-up a little before the batch due at
/// `last` does not make the same second due twice.
auto next_batch(time now, Option<time> last) -> time {
  auto due
    = floor(now, std::chrono::seconds{1}) + std::chrono::milliseconds{500};
  if (due <= now) {
    due += std::chrono::seconds{1};
  }
  // Unless the clock went back, in which case `last` says nothing.
  if (last and due <= *last and *last - now < std::chrono::seconds{1}) {
    due = *last + std::chrono::seconds{1};
  }
  return due;
}

} // namespace

class PlatformReporter::State {
public:
  explicit State(PlatformConnection connection, Option<Box<Transport>> transport
                                                = None{})
    : uplink{std::move(connection), std::move(transport)} {
  }

  auto add(Arc<RunReport> run) const -> void {
    auto lock = std::scoped_lock{runs_mutex};
    // An ended run stays until a batch has taken what it recorded, which waits
    // while the uplink is full. Past the bound, the oldest one goes.
    auto ended = [](Arc<RunReport> const& report) {
      return report->closed.load(std::memory_order_acquire);
    };
    if (static_cast<std::size_t>(std::ranges::count_if(runs, ended))
        >= PlatformReporter::max_ended_runs) {
      auto oldest = std::ranges::find_if(runs, ended);
      uplink.dropped.fetch_add((*oldest)->discard(), std::memory_order_relaxed);
      runs.erase(oldest);
    }
    runs.push_back(std::move(run));
  }

  auto request(ServedRequest served) const -> void {
    auto lock = std::scoped_lock{requests_mutex};
    if (requests.size() >= max_pending_requests) {
      uplink.dropped.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    requests.push_back(std::move(served));
  }

  /// Folds what is pending into batches while the uplink has room for them,
  /// splitting only where a batch would exceed the platform's limits.
  ///
  /// The runs go first, and take turns at what room there is, starting after
  /// the one that last found none, so that a run that records more than there
  /// is room for cannot keep the others waiting. What a run records is
  /// bounded, while served requests can keep coming, and would otherwise take
  /// every slot that frees up. The requests take the room that is left. A run
  /// whose last profiler is gone is forgotten once a batch has taken all it
  /// recorded.
  auto prepare() const -> void {
    if (uplink.full()) {
      return;
    }
    auto draft = Draft{};
    // Seals the draft as a batch, and tells whether another one has room.
    auto seal = [&] {
      auto lost = uplink.dropped.exchange(0, std::memory_order_relaxed);
      uplink.enqueue(std::exchange(draft, Draft{}).finish(lost), lost);
      return not uplink.full();
    };
    // The process sample goes first, before the runs can take every slot
    // there is, which would otherwise starve it while telemetry flows. A
    // batch sealed below carries it. One that found no room stays until a
    // batch takes it, and no new one is taken meanwhile, so the queue filling
    // up loses none.
    if (not unsent_sample) {
      unsent_sample = process_sample();
    }
    if (unsent_sample) {
      while (not draft.add(*unsent_sample)) {
        if (not seal()) {
          return;
        }
      }
      unsent_sample = None{};
    }
    auto pending = [&] {
      auto lock = std::scoped_lock{runs_mutex};
      return runs;
    }();
    auto drained = std::vector<RunReport const*>{};
    auto forget = [&] {
      if (drained.empty()) {
        return;
      }
      auto lock = std::scoped_lock{runs_mutex};
      std::erase_if(runs, [&](Arc<RunReport> const& run) {
        return std::ranges::contains(drained, &*run);
      });
    };
    for (auto i = std::size_t{0}; i < pending.size(); ++i) {
      auto index = (next_run + i) % pending.size();
      auto& run = pending[index];
      // Before taking, so that nothing it records after is left behind.
      auto closed = run->closed.load(std::memory_order_acquire);
      while (not run->take(draft, uplink.dropped)) {
        if (not seal()) {
          next_run = index + 1;
          forget();
          return;
        }
      }
      if (closed) {
        drained.push_back(&*run);
      }
    }
    // Whatever a batch has taken from them is sealed below in any case.
    forget();
    while (true) {
      auto taken = [&] {
        auto lock = std::scoped_lock{requests_mutex};
        return take(requests, draft, uplink.dropped);
      }();
      if (taken) {
        break;
      }
      if (not seal()) {
        return;
      }
    }
    if (not draft.empty()
        or uplink.dropped.load(std::memory_order_relaxed) > 0) {
      seal();
    }
  }

  /// Batches once a second until `stop` is requested, then flushes.
  auto run(StopFlag stop) const -> Task<void> {
    // Batching and sending end together, because the flush below takes over
    // the queue and the batch in flight, which the sender must have let go of.
    std::ignore
      = co_await stop.run(async_scope([this](AsyncScope& scope) -> Task<void> {
          scope.spawn(uplink.send());
          auto last = Option<time>{};
          while (true) {
            auto now = time::clock::now();
            auto due = next_batch(now, last);
            last = due;
            // Bounded, should the clock jump.
            auto delay = std::clamp(due - now, duration::zero(),
                                    duration{std::chrono::milliseconds{1500}});
            co_await folly::coro::sleep(
              std::chrono::duration_cast<folly::HighResDuration>(delay));
            prepare();
          }
        }));
    co_await flush();
  }

  /// The last part of `run`, once its sender has let go of the queue.
  auto flush() const -> Task<void> {
    co_await uplink.flush([this] {
      prepare();
    });
  }

  Uplink uplink;

private:
  /// What the process uses, once every `process_sample_interval`: its CPU
  /// time and uptime as running totals, which the platform takes the rate
  /// of between two samples, and the memory it has resident at the sample.
  auto process_sample() const -> Option<DeploymentSample> {
    auto now = std::chrono::steady_clock::now();
    if (last_process_sample
        and now - *last_process_sample < process_sample_interval) {
      return None{};
    }
    last_process_sample = now;
    if (not host) {
      return None{};
    }
    // What the `processes` operator reports of this process.
    auto self = Option<process>{};
    try {
      self = host->current_process();
    } catch (std::exception const&) {
      return None{};
    }
    if (not self->utime or not self->stime or not self->rsize) {
      return None{};
    }
    auto window = floor(time::clock::now(), std::chrono::milliseconds{1});
    auto uptime = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::max(window - self->startup, duration::zero()));
    return DeploymentSample{
      window,
      TelemetrySeconds::make(seconds_of(*self->utime + *self->stime))
        .expect("finite CPU time"),
      count<TelemetryCount>(*self->rsize),
      count<TelemetryCount>(static_cast<uint64_t>(uptime.count()))};
  }

  // The system that tells what the process uses, where there is one.
  std::unique_ptr<os> host = os::make();
  // Only `prepare` uses it, which runs one at a time.
  mutable Option<std::chrono::steady_clock::time_point> last_process_sample;
  // A sample that no batch had room for yet. Only `prepare` uses it.
  mutable Option<DeploymentSample> unsent_sample;
  mutable std::mutex requests_mutex;
  mutable std::deque<ServedRequest> requests;
  mutable std::mutex runs_mutex;
  mutable std::vector<Arc<RunReport>> runs;
  // Only `prepare` uses it, which runs one at a time.
  mutable std::size_t next_run = 0;
};

class PlatformProfiler::State {
public:
  State(PlatformReporter reporter, Arc<RunReport> report)
    : reporter{std::move(reporter)}, report{std::move(report)} {
  }

  ~State() {
    report->closed.store(true, std::memory_order_release);
  }

  State(State const&) = delete;
  auto operator=(State const&) -> State& = delete;
  State(State&&) = delete;
  auto operator=(State&&) -> State& = delete;

  PlatformReporter reporter;
  Arc<RunReport> report;
};

PlatformReporter::PlatformReporter(PlatformConnection connection)
  : state_{std::in_place, std::move(connection)} {
}

PlatformReporter::PlatformReporter(PlatformConnection connection,
                                   Box<Transport> transport)
  : state_{std::in_place, std::move(connection), std::move(transport)} {
}

auto PlatformReporter::request(ServedRequest served) const -> void {
  state_->request(std::move(served));
}

auto PlatformReporter::run(StopFlag stop) const -> Task<void> {
  // Owns the state rather than referring to this handle, which may be gone by
  // the time the task runs.
  return folly::coro::co_invoke(
    [state = state_, stop = std::move(stop)]() -> Task<void> {
      co_await state->run(stop);
    });
}

PlatformProfiler::PlatformProfiler(PlatformReporter const& reporter,
                                   std::string pipeline_id, uint64_t run)
  : state_{std::in_place, reporter,
           Arc<RunReport>{std::in_place,
                          PipelineId::make(std::move(pipeline_id))
                            .expect("validated pipeline ID"),
                          count<TelemetryCount>(run)}} {
  reporter.state_->add(state_->report);
}

auto PlatformProfiler::diagnostic(tenzir::diagnostic const& value,
                                  SourceMap const& sources) const -> void {
  auto& dropped = state_->reporter.state_->uplink.dropped;
  if (value.notes.size() > 32 or value.annotations.size() > 64
      or value.message.size() > max_diagnostic_bytes) {
    dropped.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  auto now = time::clock::now();
  // With the call sites of a user-defined operator, so that the text says
  // where the pipeline called it, as `tenzir` prints it.
  auto enriched = sources.enrich(value);
  auto stream = std::ostringstream{};
  make_diagnostic_printer(sources, color_diagnostics::no, stream)
    ->emit(enriched);
  auto rendered = std::move(stream).str();
  auto notes = std::vector<PushTelemetryPayloadDiagnosticsNotes>{};
  auto annotations = std::vector<PushTelemetryPayloadDiagnosticsAnnotations>{};
  auto size = rendered.size() + value.message.size();
  for (auto const& note : value.notes) {
    size += note.message.size();
    notes.emplace_back(note_kind(note.kind), note.message);
  }
  for (auto const& annotation : enriched.annotations) {
    // Offsets inside a user-defined operator point into its body rather than
    // into the definition, so of those only the trace of its calls counts.
    if (not annotation.source or annotation.source.callsite_index != 0) {
      continue;
    }
    size += annotation.text.size();
    annotations.emplace_back(annotation.primary, annotation.text,
                             count<TelemetryCount>(annotation.source.begin),
                             count<TelemetryCount>(annotation.source.end));
  }
  if (size > max_diagnostic_bytes) {
    dropped.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  auto severity
    = value.severity == tenzir::severity::error     ? TelemetrySeverity::error
      : value.severity == tenzir::severity::warning ? TelemetrySeverity::warning
                                                    : TelemetrySeverity::note;
  auto const& report = *state_->report;
  auto sample = DiagnosticSample{report.id,
                                 report.run,
                                 now,
                                 now,
                                 count<TelemetrySmallCount>(1),
                                 {},
                                 severity,
                                 value.message,
                                 std::move(rendered),
                                 std::move(notes),
                                 std::move(annotations)};
  // Hash structured content without interval timestamps or occurrence count.
  sample.fingerprint = fmt::format(
    "{:016x}", tenzir::hash(sample.severity, sample.message, sample.rendered,
                            sample.notes, sample.annotations));
  state_->report->diagnostic(std::move(sample), dropped);
}

auto PlatformProfiler::sample(std::span<MetricsSnapshotEntry const> counters,
                              Option<PipelineResources> resources) const
  -> void {
  state_->report->sample(counters, std::move(resources));
}

auto PlatformProfiler::transition(std::string state,
                                  Option<std::string> error) const -> void {
  auto& dropped = state_->reporter.state_->uplink.dropped;
  if (error and error->size() > max_diagnostic_bytes) {
    error = None{};
    dropped.fetch_add(1, std::memory_order_relaxed);
  }
  auto value = state == "running"    ? RunState::running
               : state == "finished" ? RunState::finished
               : state == "stopped"  ? RunState::stopped
                                     : RunState::failed;
  auto const& report = *state_->report;
  state_->report->transition(RunTransition{report.id, report.run,
                                           time::clock::now(), value,
                                           std::move(error)},
                             dropped);
}

auto PlatformProfiler::reporter() const -> PlatformReporter {
  return state_->reporter;
}

} // namespace tenzir
