// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/controller/http_transport.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/platform_profiler.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tunnel/platform.hpp>
#include <tenzir/uuid.hpp>

#include <folly/coro/DetachOnCancel.h>
#include <folly/coro/Retry.h>
#include <folly/coro/Timeout.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <utility>

namespace tenzir {
namespace {

constexpr auto max_batch_bytes = std::size_t{256} << 10;
constexpr auto max_diagnostic_bytes = std::size_t{16} << 10;
constexpr auto max_pending_batches = uint32_t{16};
constexpr auto max_wire_count = uint64_t{9'007'199'254'740'991};

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

} // namespace

class PlatformProfiler::State {
public:
  struct Pending {
    std::string body;
    std::chrono::steady_clock::time_point created;
    uint64_t dropped;
  };

  State(PlatformConnection connection, std::string id, uint64_t run,
        Option<Box<Transport>> transport = None{})
    : connection{std::move(connection)},
      id{PipelineId::make(std::move(id)).expect("validated pipeline ID")},
      run{count<TelemetryCount2>(run)},
      transport{std::move(transport)},
      discover_transport{not this->transport} {
  }

  auto prepare(std::span<MetricsSnapshotEntry const> counters) const -> void {
    auto now = time::clock::now();
    auto totals = std::map<std::pair<MetricsDirection, std::string>,
                           std::pair<uint64_t, uint64_t>>{};
    for (auto const& entry : counters) {
      if (entry.visibility != MetricsVisibility::external_
          or entry.instrument != MetricsInstrument::counter
          or entry.connector.empty()) {
        continue;
      }
      auto& [events, bytes] = totals[{entry.direction, entry.connector}];
      auto& value = entry.type == MetricsUnit::events ? events : bytes;
      value += entry.value;
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
      flows.emplace_back(key.second, count<TelemetryCount2>(events),
                         count<TelemetryCount2>(bytes));
    }
    auto folded = std::map<std::string, DiagnosticSample>{};
    while (auto value = diagnostics.try_dequeue()) {
      auto found = folded.find(value->fingerprint);
      if (found != folded.end()) {
        found->second.first_seen
          = std::min(found->second.first_seen, value->first_seen);
        found->second.last_seen
          = std::max(found->second.last_seen, value->last_seen);
        found->second.occurrences
          = count<TenzirTelemetryTelemetrySmallCount>(std::min(
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
    if (sources.empty() and sinks.empty() and samples.empty() and runs.empty()
        and dropped.load(std::memory_order_relaxed) == 0) {
      return;
    }
    auto lost = dropped.exchange(0, std::memory_order_relaxed);
    auto pipeline_samples = std::vector<PipelineSample>{};
    if (not sources.empty() or not sinks.empty()) {
      auto window = floor(now, std::chrono::milliseconds{1});
      // ClickHouse and replay cursors retain milliseconds, not finer precision.
      if (last_sample) {
        window = std::max(window, *last_sample + std::chrono::milliseconds{1});
      }
      last_sample = window;
      pipeline_samples.emplace_back(id, run, window, std::move(sources),
                                    std::move(sinks));
    }
    auto batch = Batch{boot(),
                       count<TelemetryCount>(next_sequence()),
                       count<TenzirTelemetryTelemetryCount>(lost),
                       std::move(pipeline_samples),
                       {},
                       std::move(samples),
                       std::move(runs),
                       {}};
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
        spawn_blocking([path = connection.key_file]() -> Option<std::string> {
          auto error = std::error_code{};
          if (not std::filesystem::is_regular_file(path, error) or error) {
            return {};
          }
          auto file = std::ifstream{path, std::ios::binary};
          auto contents = std::string(4097, '\0');
          file.read(contents.data(), contents.size());
          contents.resize(file.gcount());
          if (file.bad() or contents.size() > 4096) {
            return {};
          }
          auto value = std::string{detail::trim(contents)};
          if (value.empty()
              or value.find_first_of("\r\n") != std::string::npos) {
            return {};
          }
          return value;
        }));
      if (not key) {
        TENZIR_WARN("platform telemetry key file is unavailable or invalid");
        co_return false;
      }
      transport
        = Box<Transport>::from_non_null(std::make_unique<BufferedHttpTransport>(
          std::move(discovered).unwrap(), std::move(*key)));
    }
    auto& sender = *transport;
    auto response = co_await sender->request(HttpRequest{
      "POST",
      fmt::format("/deployments/{}/telemetry", connection.deployment_id),
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

  PlatformConnection connection;
  PipelineId id;
  TelemetryCount2 run;
  mutable BoundedQueue<DiagnosticSample> diagnostics{256};
  mutable BoundedQueue<RunTransition> transitions{8};
  mutable BoundedQueue<Pending> queue{max_pending_batches};
  // send and flush run sequentially; cancellation leaves this batch first.
  mutable Option<Pending> in_flight;
  mutable Atomic<uint64_t> dropped{0};
  // Only the sampling task owns the baseline; send/flush run sequentially.
  mutable Option<time> last_sample;
  mutable std::map<std::pair<MetricsDirection, std::string>,
                   std::pair<uint64_t, uint64_t>>
    baseline;
  mutable Option<Box<Transport>> transport;
  bool discover_transport;
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
    state_->dropped.fetch_add(1, std::memory_order_relaxed);
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
                             count<TelemetryCount2>(annotation.source.begin),
                             count<TelemetryCount2>(annotation.source.end));
  }
  if (size > max_diagnostic_bytes) {
    state_->dropped.fetch_add(1, std::memory_order_relaxed);
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
                                 count<TenzirTelemetryTelemetrySmallCount>(1),
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
    state_->dropped.fetch_add(1, std::memory_order_relaxed);
  }
}

auto PlatformProfiler::sample(
  std::span<MetricsSnapshotEntry const> counters) const -> void {
  state_->prepare(counters);
}

auto PlatformProfiler::transition(std::string state,
                                  Option<std::string> error) const -> void {
  if (error and error->size() > max_diagnostic_bytes) {
    error = None{};
    state_->dropped.fetch_add(1, std::memory_order_relaxed);
  }
  auto value = state == "running"    ? RunState::running
               : state == "finished" ? RunState::finished
               : state == "stopped"  ? RunState::stopped
                                     : RunState::failed;
  if (not state_->transitions.try_enqueue(
        RunTransition{state_->id, state_->run, time::clock::now(), value,
                      std::move(error)})) {
    state_->dropped.fetch_add(1, std::memory_order_relaxed);
  }
}

auto PlatformProfiler::send() const -> Task<void> {
  while (true) {
    if (not state_->in_flight) {
      state_->in_flight = co_await state_->queue.dequeue();
    }
    if (not co_await state_->deliver(*state_->in_flight)) {
      state_->dropped.fetch_add(state_->in_flight->dropped + 1,
                                std::memory_order_relaxed);
    }
    state_->in_flight = None{};
  }
}

auto PlatformProfiler::flush() const -> Task<void> {
  auto operation = folly::coro::co_invoke([this]() -> Task<void> {
    // Make room before folding the final transition or drop report. Overflow
    // during execution is lossy, but flushing need not discard one more batch.
    if (state_->queue.size() < max_pending_batches) {
      state_->prepare({});
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    while (std::chrono::steady_clock::now() < deadline) {
      auto pending = std::exchange(state_->in_flight, None{});
      if (not pending) {
        pending = state_->queue.try_dequeue();
      }
      if (not pending) {
        if (state_->dropped.load(std::memory_order_relaxed) == 0) {
          co_return;
        }
        state_->prepare({});
        continue;
      }
      auto sent = false;
      auto account = detail::scope_guard{[&]() noexcept {
        if (not sent) {
          state_->dropped.fetch_add(pending->dropped + 1,
                                    std::memory_order_relaxed);
        }
      }};
      sent = co_await state_->deliver(*pending);
      if (not sent) {
        // No busy loop manufacturing drop-report batches while offline.
        co_return;
      }
      state_->prepare({});
    }
    if (not state_->queue.empty()) {
      TENZIR_WARN("platform telemetry final flush expired with {} queued "
                  "batches",
                  state_->queue.size());
    }
  });
  try {
    co_await folly::coro::timeout(std::move(operation),
                                  std::chrono::seconds{3});
  } catch (folly::FutureTimeout const&) {
    TENZIR_WARN("platform telemetry final flush timed out");
  }
}

} // namespace tenzir
