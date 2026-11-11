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
#include <tenzir/platform_profiler.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tunnel/platform.hpp>
#include <tenzir/uuid.hpp>

#include <folly/coro/DetachOnCancel.h>
#include <folly/coro/Retry.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>

#include <algorithm>
#include <exception>
#include <map>
#include <sstream>
#include <tuple>
#include <utility>

namespace tenzir {
namespace {

constexpr auto max_batch_bytes = std::size_t{256} << 10;
constexpr auto max_diagnostic_bytes = std::size_t{16} << 10;
constexpr auto max_pending_batches = uint32_t{16};
constexpr auto max_wire_count = uint64_t{9'007'199'254'740'991};
// Ample for a deployment the platform calls a few times a second, and bounded
// for one that something floods: past it, a sample counts as dropped.
constexpr auto max_pending_requests = uint32_t{1000};

// One sequence across all pipeline senders in the process, including retries.
auto next_sequence() -> uint64_t {
  static auto sequence = Atomic<uint64_t>{0};
  auto value = sequence.fetch_add(1, std::memory_order_relaxed);
  TENZIR_ASSERT(value <= max_wire_count, "telemetry sequence exhausted");
  return value;
}

auto boot() -> std::string const& {
  static auto const value = fmt::format("{}", uuid::random());
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
  /// its own bounded queues, where what overflows counts as dropped one by
  /// one, rather than in a batch that would be dropped whole as one.
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
          std::move(discovered).unwrap(), std::move(*key)));
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
      if (discover_transport) {
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

  auto deliver(Pending const& pending) const -> Task<bool> {
    struct RetryTelemetry : std::exception {};
    try {
      co_return co_await folly::coro::retryWithExponentialBackoff(
        2, std::chrono::milliseconds{250}, std::chrono::milliseconds{500}, 0.0,
        [this, &pending]() -> Task<bool> {
          if (std::chrono::steady_clock::now() - pending.created
              > std::chrono::seconds{15}) {
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

} // namespace

class PlatformProfiler::State {
public:
  State(PlatformConnection connection, std::string id, uint64_t run,
        Option<Box<Transport>> transport = None{})
    : uplink{std::move(connection), std::move(transport)},
      id{PipelineId::make(std::move(id)).expect("validated pipeline ID")},
      run{count<TelemetryCount>(run)} {
  }

  /// Folds what is pending into a batch, unless the uplink has no room for
  /// one. Then the counters' totals are kept for the next time: they only
  /// grow, so what the batch would have carried arrives with the next one,
  /// even with the final flush, which has no counters of its own. The
  /// resources of a sample without room are lost with the interval they
  /// cover, which leaves their rate intact.
  auto prepare(std::span<MetricsSnapshotEntry const> counters,
               Option<PipelineResources> resources = None{}) const -> void {
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
    if (uplink.full()) {
      return;
    }
    auto now = time::clock::now();
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
    auto folded = std::map<std::string, DiagnosticSample>{};
    while (auto value = diagnostics.try_dequeue()) {
      auto found = folded.find(value->fingerprint);
      if (found != folded.end()) {
        found->second.first_seen
          = std::min(found->second.first_seen, value->first_seen);
        found->second.last_seen
          = std::max(found->second.last_seen, value->last_seen);
        found->second.occurrences = count<TelemetrySmallCount>(std::min(
          found->second.occurrences.value() + 1, int64_t{4'294'967'295}));
      } else {
        folded.emplace(value->fingerprint, std::move(*value));
      }
    }
    auto samples = std::vector<DiagnosticSample>{};
    for (auto& [key, value] : folded) {
      samples.push_back(std::move(value));
    }
    auto runs = std::vector<RunTransition>{};
    while (auto value = transitions.try_dequeue()) {
      runs.push_back(std::move(*value));
    }
    if (sources.empty() and sinks.empty() and not resources and samples.empty()
        and runs.empty()
        and uplink.dropped.load(std::memory_order_relaxed) == 0) {
      return;
    }
    auto lost = uplink.dropped.exchange(0, std::memory_order_relaxed);
    auto pipeline_samples = std::vector<PipelineSample>{};
    if (not sources.empty() or not sinks.empty() or resources) {
      auto window = floor(now, std::chrono::milliseconds{1});
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
      pipeline_samples.emplace_back(id, run, window, std::move(sources),
                                    std::move(sinks), std::move(cpu),
                                    std::move(interval), std::move(queued),
                                    std::move(moved));
    }
    // No `requests`: a pipeline answers none, and the deployment reports its
    // own through `PlatformReporter`.
    auto batch = Batch{boot(),
                       count<TelemetryCount>(next_sequence()),
                       count<TelemetryCount>(lost),
                       std::move(pipeline_samples),
                       // Backends before pipeline resources require the array.
                       std::vector<RetiredOperatorSample>{},
                       std::move(samples),
                       std::move(runs),
                       {},
                       None{}};
    uplink.enqueue(batch, lost);
  }

  Uplink uplink;
  PipelineId id;
  TelemetryCount run;
  mutable BoundedQueue<DiagnosticSample> diagnostics{256};
  mutable BoundedQueue<RunTransition> transitions{8};
  // Only the sampling task owns these; send/flush run sequentially. The
  // totals are the counters' latest, the baselines what batches carried.
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

class PlatformReporter::State {
public:
  explicit State(PlatformConnection connection, Option<Box<Transport>> transport
                                                = None{})
    : uplink{std::move(connection), std::move(transport)} {
  }

  /// Folds what is pending into batches of at most `max_batch_requests`.
  auto prepare() const -> void {
    while (not uplink.full()) {
      auto pending = std::vector<ServedRequest>{};
      while (pending.size() < PlatformReporter::max_batch_requests) {
        auto value = requests.try_dequeue();
        if (not value) {
          break;
        }
        pending.push_back(std::move(*value));
      }
      auto full = pending.size() == PlatformReporter::max_batch_requests;
      if (pending.empty()
          and uplink.dropped.load(std::memory_order_relaxed) == 0) {
        return;
      }
      auto lost = uplink.dropped.exchange(0, std::memory_order_relaxed);
      auto batch = Batch{boot(),
                         count<TelemetryCount>(next_sequence()),
                         count<TelemetryCount>(lost),
                         {},
                         std::vector<RetiredOperatorSample>{},
                         {},
                         {},
                         {},
                         std::move(pending)};
      uplink.enqueue(batch, lost);
      if (not full) {
        return;
      }
    }
  }

  Uplink uplink;
  mutable BoundedQueue<ServedRequest> requests{max_pending_requests};
};

PlatformProfiler::PlatformProfiler(PlatformConnection connection,
                                   std::string pipeline_id, uint64_t run)
  : state_{std::in_place, std::move(connection), std::move(pipeline_id), run} {
}

PlatformProfiler::PlatformProfiler(PlatformConnection connection,
                                   std::string pipeline_id, uint64_t run,
                                   Box<Transport> transport)
  : state_{std::in_place, std::move(connection), std::move(pipeline_id), run,
           std::move(transport)} {
}

auto PlatformProfiler::diagnostic(tenzir::diagnostic const& value,
                                  SourceMap const& sources) const -> void {
  if (value.notes.size() > 32 or value.annotations.size() > 64
      or value.message.size() > max_diagnostic_bytes) {
    state_->uplink.dropped.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  auto now = time::clock::now();
  auto stream = std::ostringstream{};
  make_diagnostic_printer(sources, color_diagnostics::no, stream)->emit(value);
  auto rendered = std::move(stream).str();
  auto notes = std::vector<PushTelemetryPayloadDiagnosticsNotes>{};
  auto annotations = std::vector<PushTelemetryPayloadDiagnosticsAnnotations>{};
  auto size = rendered.size() + value.message.size();
  for (auto const& note : value.notes) {
    size += note.message.size();
    notes.emplace_back(note_kind(note.kind), note.message);
  }
  for (auto const& annotation : value.annotations) {
    size += annotation.text.size();
    annotations.emplace_back(annotation.primary, annotation.text,
                             count<TelemetryCount>(annotation.source.begin),
                             count<TelemetryCount>(annotation.source.end));
  }
  if (size > max_diagnostic_bytes) {
    state_->uplink.dropped.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  auto severity = value.severity == tenzir::severity::error ? Severity::error
                  : value.severity == tenzir::severity::warning
                    ? Severity::warning
                    : Severity::note;
  auto sample = DiagnosticSample{state_->id,
                                 state_->run,
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
  if (not state_->diagnostics.try_enqueue(std::move(sample))) {
    state_->uplink.dropped.fetch_add(1, std::memory_order_relaxed);
  }
}

auto PlatformProfiler::sample(std::span<MetricsSnapshotEntry const> counters,
                              Option<PipelineResources> resources) const
  -> void {
  state_->prepare(counters, resources);
}

auto PlatformProfiler::transition(std::string state,
                                  Option<std::string> error) const -> void {
  if (error and error->size() > max_diagnostic_bytes) {
    error = None{};
    state_->uplink.dropped.fetch_add(1, std::memory_order_relaxed);
  }
  auto value = state == "running"    ? RunState::running
               : state == "finished" ? RunState::finished
               : state == "stopped"  ? RunState::stopped
                                     : RunState::failed;
  if (not state_->transitions.try_enqueue(
        RunTransition{state_->id, state_->run, time::clock::now(), value,
                      std::move(error)})) {
    state_->uplink.dropped.fetch_add(1, std::memory_order_relaxed);
  }
}

auto PlatformProfiler::send() const -> Task<void> {
  co_await state_->uplink.send();
}

auto PlatformProfiler::flush() const -> Task<void> {
  co_await state_->uplink.flush([this] {
    state_->prepare({});
  });
}

PlatformReporter::PlatformReporter(PlatformConnection connection)
  : state_{std::in_place, std::move(connection)} {
}

PlatformReporter::PlatformReporter(PlatformConnection connection,
                                   Box<Transport> transport)
  : state_{std::in_place, std::move(connection), std::move(transport)} {
}

auto PlatformReporter::request(ServedRequest served) const -> void {
  if (not state_->requests.try_enqueue(std::move(served))) {
    state_->uplink.dropped.fetch_add(1, std::memory_order_relaxed);
  }
}

auto PlatformReporter::run(StopFlag stop) const -> Task<void> {
  // Batching and sending end together, because the flush below takes over
  // the queue and the batch in flight, which the sender must have let go of.
  std::ignore
    = co_await stop.run(async_scope([this](AsyncScope& scope) -> Task<void> {
        scope.spawn(state_->uplink.send());
        while (true) {
          co_await folly::coro::sleep(std::chrono::seconds{1});
          state_->prepare();
        }
      }));
  co_await flush();
}

auto PlatformReporter::flush() const -> Task<void> {
  co_await state_->uplink.flush([this] {
    state_->prepare();
  });
}

} // namespace tenzir
