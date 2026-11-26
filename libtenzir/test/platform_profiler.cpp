// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/stop_flag.hpp>
#include <tenzir/controller/api_support.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/platform_profiler.hpp>
#include <tenzir/source.hpp>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>

// After folly, whose headers use glog's `CHECK`.
#include <tenzir/test/test.hpp>

#include <array>
#include <limits>
#include <map>
#include <set>

namespace tenzir {
namespace {

constexpr auto max_batch_bytes = std::size_t{256} << 10;

/// What a transport was sent, as the platform decodes it, and when.
struct Sent {
  std::vector<Batch> batches;
  std::vector<std::size_t> sizes;
  std::vector<std::chrono::steady_clock::time_point> times;
};

class RecordingTransport final : public Transport {
public:
  explicit RecordingTransport(std::shared_ptr<Sent> sent,
                              std::vector<uint16_t> statuses = {})
    : sent_{std::move(sent)}, statuses_{std::move(statuses)} {
  }
  auto request(HttpRequest request)
    -> Task<Result<HttpResponse, RequestError>> override {
    auto document
      = api::Document::parse(request.body, "Batch").expect("valid JSON");
    sent_->batches.push_back(
      parse_batch(document.root()).expect("valid batch"));
    sent_->sizes.push_back(request.body.size());
    sent_->times.push_back(std::chrono::steady_clock::now());
    auto attempt = sent_->batches.size();
    auto status
      = attempt <= statuses_.size() ? statuses_[attempt - 1] : uint16_t{204};
    if (status == 0) {
      co_return Err{RequestError{"lost acknowledgement"}};
    }
    co_return HttpResponse{status, {}};
  }

private:
  std::shared_ptr<Sent> sent_;
  std::vector<uint16_t> statuses_;
};

class SlowTransport final : public Transport {
public:
  explicit SlowTransport(std::shared_ptr<int> calls)
    : calls_{std::move(calls)} {
  }
  auto request(HttpRequest)
    -> Task<Result<HttpResponse, RequestError>> override {
    ++*calls_;
    co_await folly::coro::sleep(std::chrono::hours{1});
    co_return HttpResponse{204, {}};
  }

private:
  std::shared_ptr<int> calls_;
};

/// Records the first batch, and then is cancelled before it can acknowledge
/// it, as a sender is when its deployment stops mid-request.
class CancelOnceTransport final : public Transport {
public:
  explicit CancelOnceTransport(std::shared_ptr<Sent> sent)
    : inner_{std::move(sent)} {
  }
  auto request(HttpRequest request)
    -> Task<Result<HttpResponse, RequestError>> override {
    auto response = co_await inner_.request(std::move(request));
    if (not canceled_) {
      canceled_ = true;
      throw folly::OperationCancelled{};
    }
    co_return response;
  }

private:
  RecordingTransport inner_;
  bool canceled_ = false;
};

/// A connection whose identity is known, so that the transports above see
/// nothing but telemetry.
auto known() -> PlatformConnection {
  return PlatformConnection::confirmed_as(DeploymentIdentity{
    OrganizationId::make("00000000-0000-4000-8000-000000000001").unwrap(),
    WorkspaceId::make("00000000-0000-4000-8000-000000000002").unwrap(),
    DeploymentId::make("00000000-0000-4000-8000-000000000003").unwrap()});
}

class PathTransport final : public Transport {
public:
  explicit PathTransport(std::shared_ptr<std::string> path)
    : path_{std::move(path)} {
  }
  auto request(HttpRequest request)
    -> Task<Result<HttpResponse, RequestError>> override {
    *path_ = request.path;
    co_return HttpResponse{204, {}};
  }

private:
  std::shared_ptr<std::string> path_;
};

auto recording(std::shared_ptr<Sent> sent, std::vector<uint16_t> statuses = {})
  -> PlatformReporter {
  return PlatformReporter{known(),
                          RecordingTransport{std::move(sent), statuses}};
}

/// Sends what `reporter` has pending, as its run does once stopped.
auto flush(PlatformReporter const& reporter) -> void {
  auto stop = StopFlag{};
  stop.request();
  folly::coro::blockingWait(reporter.run(stop));
}

/// Runs `reporter` until `sent` holds `attempts` requests, and then stops it.
/// Unlike `flush`, the retries take the backoff of a running deployment rather
/// than fitting into the deadline of the final flush.
auto send_until(PlatformReporter const& reporter, Sent const& sent,
                std::size_t attempts) -> void {
  auto stop = StopFlag{};
  auto watch = [&]() -> Task<void> {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (sent.batches.size() < attempts
           and std::chrono::steady_clock::now() < deadline) {
      co_await folly::coro::sleep(std::chrono::milliseconds{10});
    }
    stop.request();
  };
  folly::coro::blockingWait(
    folly::coro::collectAll(reporter.run(stop), watch()));
}

auto external(uint64_t events, MetricsDirection direction
                               = MetricsDirection::read)
  -> MetricsSnapshotEntry {
  return {MetricsLabel{"peer_ip", "192.0.2.1"},
          "from_kafka",
          direction,
          MetricsVisibility::external_,
          MetricsInstrument::counter,
          MetricsUnit::events,
          events};
}

auto busy(duration cpu = std::chrono::milliseconds{100}) -> PipelineResources {
  return PipelineResources{
    .cpu = cpu, .interval = std::chrono::seconds{1}, .queued_bytes = 64};
}

/// A served request as wide as the wire lets it be.
auto widest_request() -> ServedRequest {
  // JSON escapes a control character as `\u00XX`, six bytes for one.
  auto escaped = [](std::size_t size) {
    return std::string(size, '\x01');
  };
  return ServedRequest{
    time::clock::now(),
    // The longest a double gets, `2.2250738585072014e-308`.
    TelemetrySeconds::make(std::numeric_limits<double>::min()).unwrap(),
    HttpMethod::make(escaped(16)).unwrap(),
    EndpointName::make(escaped(128)).unwrap(),
    TelemetrySmallCount::make(4'294'967'295).unwrap(),
    TelemetryCount::make(9'007'199'254'740'991).unwrap(),
    Traceparent::make("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
      .unwrap(),
  };
}

} // namespace

TEST("platform wire counters retain safe numeric bounds") {
  CHECK(TelemetryCount::make(-1).is_err());
  CHECK(TelemetryCount::make(9'007'199'254'740'991).is_ok());
  CHECK(TelemetryCount::make(9'007'199'254'740'992).is_err());
  CHECK(TelemetrySmallCount::make(-1).is_err());
  CHECK(TelemetrySmallCount::make(4'294'967'295).is_ok());
  CHECK(TelemetrySmallCount::make(4'294'967'296).is_err());
  auto numeric = api::Document::parse("7", "count").expect("valid JSON");
  auto parsed = parse_telemetry_count(numeric.root());
  REQUIRE(parsed.is_ok());
  CHECK_EQUAL(parsed.unwrap().value(), 7);
  auto text = api::Document::parse(R"("7")", "count").expect("valid JSON");
  CHECK(parse_telemetry_count(text.root()).is_err());
}

TEST("platform telemetry goes to the deployment's nested route") {
  auto path = std::make_shared<std::string>();
  auto reporter = PlatformReporter{known(), PathTransport{path}};
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  profiler.transition("finished");
  flush(reporter);
  CHECK_EQUAL(*path, "/organizations/00000000-0000-4000-8000-000000000001/"
                     "workspaces/00000000-0000-4000-8000-000000000002/"
                     "deployments/00000000-0000-4000-8000-000000000003/"
                     "telemetry");
}

TEST("platform profiler differences connector counters and folds diagnostics") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto run = uint64_t{4'294'967'296};
  auto profiler = PlatformProfiler{reporter, "pipeline", run};
  auto counters = std::array{external(5), external(9, MetricsDirection::write)};
  profiler.sample(counters);
  flush(reporter);
  counters[0].value = 8;
  auto sources = SourceMap{};
  auto warning = diagnostic{severity::warning, "retrying"};
  warning.notes.emplace_back(diagnostic_note_kind::hint, "try again");
  profiler.diagnostic(warning, sources);
  profiler.diagnostic(warning, sources);
  profiler.sample(counters);
  flush(reporter);
  profiler.transition("finished");
  flush(reporter);
  auto const& batches = sent->batches;
  REQUIRE_EQUAL(batches.size(), 3u);
  CHECK_EQUAL(batches[0].pipelines[0].run.value(), run);
  CHECK_EQUAL(batches[0].pipelines[0].sources[0].events.value(), 5);
  CHECK_EQUAL(batches[0].pipelines[0].sinks[0].events.value(), 9);
  CHECK_EQUAL(batches[1].pipelines[0].sources[0].events.value(), 3);
  CHECK(batches[1].pipelines[0].sinks.empty());
  REQUIRE_EQUAL(batches[1].diagnostics.size(), 1u);
  CHECK_EQUAL(batches[1].diagnostics[0].run.value(), run);
  CHECK_EQUAL(batches[1].diagnostics[0].occurrences.value(), 2);
  REQUIRE_EQUAL(batches[1].diagnostics[0].notes.size(), 1u);
  CHECK_EQUAL(batches[1].diagnostics[0].notes[0].kind, "hint");
  CHECK_EQUAL(batches[1].diagnostics[0].notes[0].message, "try again");
  CHECK(batches[1].diagnostics[0].first_seen
        <= batches[1].diagnostics[0].last_seen);
  CHECK_EQUAL(batches[2].runs[0].state, RunState::finished);
  CHECK_EQUAL(batches[2].runs[0].run.value(), run);
  // Without resources, a sample carries none.
  CHECK(batches[0].pipelines[0].cpu_seconds.is_none());
  CHECK(batches[0].pipelines[0].queued_bytes.is_none());
  CHECK(batches[0].pipelines[0].activity.is_none());
}

TEST("platform profiler reports resources even without traffic") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  // External and internal counters in both directions add up to the activity.
  auto counters = std::array{external(5), external(2), external(1),
                             external(4, MetricsDirection::write)};
  for (auto& counter : std::span{counters}.subspan(1)) {
    counter.visibility = MetricsVisibility::internal_;
  }
  // Bytes are not events.
  counters[2].type = MetricsUnit::bytes;
  profiler.sample(counters,
                  PipelineResources{.cpu = std::chrono::milliseconds{250},
                                    .interval = std::chrono::seconds{1},
                                    .queued_bytes = 4096});
  flush(reporter);
  // An idle pipeline still reports what it costs, here over a late tick.
  counters[1].value = 9;
  profiler.sample(counters,
                  PipelineResources{.cpu = std::chrono::milliseconds{1500},
                                    .interval = std::chrono::seconds{3},
                                    .queued_bytes = 0});
  flush(reporter);
  // Without resources or traffic, there is no sample to report.
  profiler.sample({});
  flush(reporter);
  auto const& batches = sent->batches;
  REQUIRE_EQUAL(batches.size(), 2u);
  REQUIRE_EQUAL(batches[0].pipelines.size(), 1u);
  auto const& active = batches[0].pipelines[0];
  CHECK_EQUAL(active.sources[0].events.value(), 5);
  REQUIRE(active.cpu_seconds.is_some());
  CHECK_EQUAL(active.cpu_seconds->value(), 0.25);
  REQUIRE(active.interval_seconds.is_some());
  CHECK_EQUAL(active.interval_seconds->value(), 1.0);
  REQUIRE(active.queued_bytes.is_some());
  CHECK_EQUAL(active.queued_bytes->value(), 4096);
  REQUIRE(active.activity.is_some());
  CHECK_EQUAL(active.activity->value(), 11);
  REQUIRE_EQUAL(batches[1].pipelines.size(), 1u);
  auto const& idle = batches[1].pipelines[0];
  CHECK(idle.sources.empty());
  CHECK(idle.sinks.empty());
  REQUIRE(idle.cpu_seconds.is_some());
  CHECK_EQUAL(idle.cpu_seconds->value(), 1.5);
  REQUIRE(idle.interval_seconds.is_some());
  CHECK_EQUAL(idle.interval_seconds->value(), 3.0);
  REQUIRE(idle.queued_bytes.is_some());
  CHECK_EQUAL(idle.queued_bytes->value(), 0);
  REQUIRE(idle.activity.is_some());
  CHECK_EQUAL(idle.activity->value(), 7);
  CHECK(active.window < idle.window);
  // The wire carries all of them, so the platform reads them.
  auto encoded = std::string{};
  write_json(batches[0], encoded);
  CHECK(encoded.find(R"("cpuSeconds":0.25)") != std::string::npos);
  CHECK(encoded.find(R"("intervalSeconds":1)") != std::string::npos);
  CHECK(encoded.find(R"("queuedBytes":4096)") != std::string::npos);
  CHECK(encoded.find(R"("activity":11)") != std::string::npos);
}

TEST("platform profiler keeps rapid samples distinct at millisecond "
     "precision") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  auto copy = profiler;
  for (auto events = uint64_t{1};
       events <= PlatformProfiler::max_pending_samples; ++events) {
    auto counters = std::array{external(events)};
    (events % 2 == 0 ? copy : profiler).sample(counters);
  }
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  auto const& samples = sent->batches[0].pipelines;
  REQUIRE_EQUAL(samples.size(), PlatformProfiler::max_pending_samples);
  for (auto index = size_t{0}; index < samples.size(); ++index) {
    auto const& sample = samples[index];
    CHECK_EQUAL(sample.sources[0].events.value(), 1);
    CHECK_EQUAL(sample.window,
                floor(sample.window, std::chrono::milliseconds{1}));
    if (index > 0) {
      CHECK(sample.window
            >= samples[index - 1].window + std::chrono::milliseconds{1});
    }
  }
}

TEST("platform profiler merges the samples that do not fit, losing nothing") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  // Nothing batches in between, as when the uplink is full.
  for (auto events = uint64_t{1}; events <= 40; ++events) {
    auto counters = std::array{external(events)};
    profiler.sample(counters, busy());
  }
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  auto const& samples = sent->batches[0].pipelines;
  auto const kept = PlatformProfiler::max_pending_samples;
  REQUIRE_EQUAL(samples.size(), kept);
  auto events = int64_t{0};
  for (auto index = size_t{0}; index < samples.size(); ++index) {
    events += samples[index].sources[0].events.value();
    if (index > 0) {
      CHECK(samples[index - 1].window < samples[index].window);
    }
  }
  CHECK_EQUAL(events, 40);
  CHECK_EQUAL(sent->batches[0].dropped.value(), 0);
  // The last one covers all that came after the others, CPU time and its
  // interval alike, so that the rate stays what it was.
  auto const& merged = samples.back();
  auto const covered = 40 - (kept - 1);
  CHECK_EQUAL(merged.sources[0].events.value(), static_cast<int64_t>(covered));
  REQUIRE(merged.cpu_seconds.is_some());
  REQUIRE(merged.interval_seconds.is_some());
  CHECK_LESS_EQUAL(std::abs(merged.cpu_seconds->value() - 0.1 * covered), 1e-9);
  CHECK_EQUAL(merged.interval_seconds->value(), static_cast<double>(covered));
  REQUIRE(merged.activity.is_some());
  CHECK_EQUAL(merged.activity->value(), static_cast<int64_t>(covered));
  // Another run shares the process's boot and sequence.
  auto other = PlatformProfiler{reporter, "other", 2};
  other.transition("failed");
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 2u);
  CHECK_EQUAL(sent->batches[0].boot, sent->batches[1].boot);
  CHECK(sent->batches[0].seq.value() < sent->batches[1].seq.value());
  CHECK_EQUAL(sent->batches[1].runs[0].pipeline_id.to_string(), "other");
}

TEST("platform reporter sends the runs of a process in one batch") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profilers = std::vector<PlatformProfiler>{};
  for (auto run = uint64_t{0}; run < 300; ++run) {
    auto& profiler
      = profilers.emplace_back(reporter, fmt::format("pipeline-{}", run), run);
    profiler.transition("running");
    auto counters = std::array{external(run + 1)};
    profiler.sample(counters, busy());
  }
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  auto const& batch = sent->batches[0];
  CHECK_EQUAL(batch.pipelines.size(), 300u);
  CHECK_EQUAL(batch.runs.size(), 300u);
  CHECK(batch.requests.empty());
  auto pipelines = std::set<std::string>{};
  for (auto const& sample : batch.pipelines) {
    pipelines.emplace(sample.pipeline_id.to_string());
    CHECK_EQUAL(sample.sources[0].events.value(), sample.run.value() + 1);
  }
  CHECK_EQUAL(pipelines.size(), 300u);
}

TEST("platform reporter samples what the process uses") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  flush(reporter);
  // A process with nothing else to report still reports itself.
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  REQUIRE_EQUAL(sent->batches[0].deployment.size(), 1u);
  auto const& sample = sent->batches[0].deployment[0];
  // Running totals, which the platform takes the rate of, and the memory
  // resident now, which any process has some of.
  CHECK_GREATER(sample.cpu_seconds.value(), 0.0);
  CHECK_GREATER(sample.memory_bytes.value(), 0);
  CHECK_GREATER_EQUAL(sample.uptime_ms.value(), 0);
  // Not again within the interval.
  flush(reporter);
  CHECK_EQUAL(sent->batches.size(), 1u);
}

TEST("platform reporter splits batches only at the platform's limits") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profilers = std::vector<PlatformProfiler>{};
  constexpr auto runs = uint64_t{3000};
  for (auto run = uint64_t{0}; run < runs; ++run) {
    auto& profiler
      = profilers.emplace_back(reporter, fmt::format("pipeline-{}", run), run);
    auto counters = std::array{external(run + 1),
                               external(run + 2, MetricsDirection::write)};
    profiler.sample(counters, busy());
  }
  flush(reporter);
  auto const& batches = sent->batches;
  REQUIRE_GREATER(batches.size(), 1u);
  auto seen = std::map<std::string, int>{};
  for (auto index = size_t{0}; index < batches.size(); ++index) {
    CHECK_LESS_EQUAL(sent->sizes[index], max_batch_bytes);
    // Each but the last is as full as the next sample lets it be.
    if (index + 1 < batches.size()) {
      CHECK_GREATER(sent->sizes[index], max_batch_bytes - 1024);
    }
    for (auto const& sample : batches[index].pipelines) {
      ++seen[std::string{sample.pipeline_id.to_string()}];
    }
  }
  CHECK_EQUAL(seen.size(), runs);
  for (auto const& [pipeline, count] : seen) {
    CHECK_EQUAL(count, 1);
  }
}

TEST("platform reporter splits the widest served requests by size") {
  auto sample = widest_request();
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  for (auto i = 0; i < 1000; ++i) {
    reporter.request(sample);
  }
  flush(reporter);
  REQUIRE_GREATER(sent->batches.size(), 1u);
  auto requests = std::size_t{0};
  for (auto index = size_t{0}; index < sent->batches.size(); ++index) {
    CHECK_LESS_EQUAL(sent->sizes[index], max_batch_bytes);
    requests += sent->batches[index].requests.size();
  }
  CHECK_EQUAL(requests, 1000u);
}

TEST("platform reporter puts the runs ahead of served requests") {
  // Requests that take several batches, as when something floods the API,
  // must not hold back the runs: with an uplink that frees one slot at a
  // time, they would otherwise take each one.
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto sample = widest_request();
  for (auto i = 0; i < 1000; ++i) {
    reporter.request(sample);
  }
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  profiler.transition("running");
  auto counters = std::array{external(3)};
  profiler.sample(counters, busy());
  flush(reporter);
  REQUIRE_GREATER(sent->batches.size(), 1u);
  CHECK_EQUAL(sent->batches[0].runs.size(), 1u);
  CHECK_EQUAL(sent->batches[0].pipelines.size(), 1u);
  auto requests = std::size_t{0};
  for (auto const& batch : sent->batches) {
    requests += batch.requests.size();
  }
  CHECK_EQUAL(requests, 1000u);
}

TEST("platform reporter keeps what a run recorded after its profiler is gone") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  {
    auto profiler = PlatformProfiler{reporter, "pipeline", 1};
    auto counters = std::array{external(3)};
    profiler.sample(counters, busy());
    profiler.transition("finished");
  }
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  CHECK_EQUAL(sent->batches[0].pipelines.size(), 1u);
  CHECK_EQUAL(sent->batches[0].runs.size(), 1u);
  // Once taken, the run is forgotten, and nothing is left to send.
  flush(reporter);
  CHECK_EQUAL(sent->batches.size(), 1u);
}

TEST("platform reporter bounds the ended runs that wait for a batch") {
  // No batch in between, as while the platform is out of reach.
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  constexpr auto extra = std::size_t{10};
  for (auto run = std::size_t{0};
       run < PlatformReporter::max_ended_runs + extra; ++run) {
    auto profiler
      = PlatformProfiler{reporter, fmt::format("pipeline-{}", run), run};
    profiler.transition("finished");
  }
  flush(reporter);
  auto runs = std::set<std::string>{};
  auto dropped = int64_t{0};
  for (auto const& batch : sent->batches) {
    for (auto const& transition : batch.runs) {
      runs.emplace(transition.pipeline_id.to_string());
    }
    dropped += batch.dropped.value();
  }
  // The oldest went, and their transitions count as dropped.
  CHECK_EQUAL(runs.size(), PlatformReporter::max_ended_runs);
  CHECK(not runs.contains("pipeline-0"));
  CHECK(runs.contains(
    fmt::format("pipeline-{}", PlatformReporter::max_ended_runs + extra - 1)));
  CHECK_EQUAL(dropped, static_cast<int64_t>(extra));
}

TEST("platform reporter sends one batch a second for all runs") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profilers = std::vector<PlatformProfiler>{};
  for (auto run = uint64_t{0}; run < 20; ++run) {
    profilers.emplace_back(reporter, fmt::format("pipeline-{}", run), run);
  }
  auto stop = StopFlag{};
  auto started = std::chrono::steady_clock::now();
  // The runs sample at the start of each second, as a pipeline does.
  auto sampling = [&]() -> Task<void> {
    for (auto second = 0; second < 3; ++second) {
      for (auto const& profiler : profilers) {
        profiler.sample({}, busy());
      }
      auto now = time::clock::now();
      auto boundary
        = floor(now, std::chrono::seconds{1}) + std::chrono::seconds{1};
      co_await folly::coro::sleep(
        std::chrono::duration_cast<folly::HighResDuration>(boundary - now));
    }
    stop.request();
  };
  folly::coro::blockingWait(
    folly::coro::collectAll(reporter.run(stop), sampling()));
  auto elapsed = std::chrono::ceil<std::chrono::seconds>(
    std::chrono::steady_clock::now() - started);
  // A batch half a second into each second, and the final one.
  CHECK_LESS_EQUAL(sent->batches.size(),
                   static_cast<std::size_t>(elapsed.count()) + 1);
  auto samples = std::size_t{0};
  auto complete = false;
  for (auto const& batch : sent->batches) {
    samples += batch.pipelines.size();
    complete = complete or batch.pipelines.size() == profilers.size();
  }
  CHECK_EQUAL(samples, 3 * profilers.size());
  CHECK(complete);
}

TEST("platform reporter retries the immutable batch") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent, {0, 429, 204});
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  auto counters = std::array{external(5)};
  profiler.sample(counters);
  send_until(reporter, *sent, 3);
  REQUIRE_EQUAL(sent->batches.size(), 3u);
  auto first = std::string{};
  write_json(sent->batches[0], first);
  for (auto const& batch : sent->batches) {
    auto encoded = std::string{};
    write_json(batch, encoded);
    CHECK_EQUAL(encoded, first);
  }
}

TEST("platform reporter keeps a batch through repeated transient failures") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent, {500, 429, 0});
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  profiler.transition("running");
  send_until(reporter, *sent, 4);
  REQUIRE_EQUAL(sent->batches.size(), 4u);
  CHECK_EQUAL(sent->batches[3].dropped.value(), 0);
  CHECK_EQUAL(sent->batches[3].runs.size(), 1u);
  // A retry never sends faster than a healthy deployment, which sends one
  // batch a second.
  for (auto i = std::size_t{1}; i < sent->times.size(); ++i) {
    auto gap = std::chrono::duration_cast<std::chrono::milliseconds>(
      sent->times[i] - sent->times[i - 1]);
    CHECK_GREATER_EQUAL(gap.count(), int64_t{1000});
  }
}

TEST("platform reporter retries a failed batch in the final flush") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent, {503});
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  profiler.transition("finished");
  flush(reporter);
  // A retry waits at least a second, so one fits into the three seconds of
  // the final flush. A third attempt would not: it comes about three seconds
  // after the first.
  REQUIRE_EQUAL(sent->batches.size(), 2u);
  CHECK_EQUAL(sent->batches[1].dropped.value(), 0);
  CHECK_EQUAL(sent->batches[1].runs.size(), 1u);
}

TEST("platform reporter flushes a canceled batch before later samples") {
  auto sent = std::make_shared<Sent>();
  auto reporter = PlatformReporter{known(), CancelOnceTransport{sent}};
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  auto counters = std::array{external(1)};
  profiler.sample(counters);
  auto stop = StopFlag{};
  // Once the sender took the first batch and was cancelled with it, a later
  // sample comes, and then the stop.
  auto later = [&]() -> Task<void> {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (sent->batches.empty()
           and std::chrono::steady_clock::now() < deadline) {
      co_await folly::coro::sleep(std::chrono::milliseconds{10});
    }
    counters[0].value = 2;
    profiler.sample(counters);
    stop.request();
  };
  folly::coro::blockingWait(
    folly::coro::collectAll(reporter.run(stop), later()));
  auto const& batches = sent->batches;
  REQUIRE_EQUAL(batches.size(), 3u);
  auto original = std::string{};
  auto retried = std::string{};
  write_json(batches[0], original);
  write_json(batches[1], retried);
  CHECK_EQUAL(original, retried);
  CHECK(batches[1].seq.value() < batches[2].seq.value());
  CHECK(batches[1].pipelines[0].window < batches[2].pipelines[0].window);
}

TEST("platform reporter rejects permanent failures without retrying") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent, {401});
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  profiler.transition("running");
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  // A later successful flush accounts for the lost batch once.
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 2u);
  CHECK_EQUAL(sent->batches[1].dropped.value(), 1);
}

TEST("platform profiler ignores unattributed and internal counters") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  auto counters = std::array{external(5), external(7), external(9)};
  counters[1].connector = "from_http";
  counters[1].visibility = MetricsVisibility::internal_;
  counters[2].connector.clear();
  profiler.sample(counters);
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  auto const& flows = sent->batches[0].pipelines[0].sources;
  REQUIRE_EQUAL(flows.size(), 1u);
  CHECK_EQUAL(flows[0].connector, "from_kafka");
  CHECK_EQUAL(flows[0].events.value(), 5);
}

TEST("platform reporter bounds the final flush while HTTP is stalled") {
  auto calls = std::make_shared<int>(0);
  auto reporter = PlatformReporter{known(), SlowTransport{calls}};
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  profiler.transition("finished");
  auto start = std::chrono::steady_clock::now();
  flush(reporter);
  CHECK_EQUAL(*calls, 1);
  CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds{5});
}

TEST("platform profiler folds repeated diagnostics and accounts for "
     "overflow") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  auto sources = SourceMap{};
  // Repeats take no room of their own.
  auto repeated = diagnostic{severity::warning, "repeated"};
  for (auto index = 0; index < 600; ++index) {
    profiler.diagnostic(repeated, sources);
  }
  // Distinct ones do, and those past the room pending count as dropped.
  for (auto index = 0; index < 300; ++index) {
    profiler.diagnostic(diagnostic{severity::warning,
                                   fmt::format("distinct {}", index)},
                        sources);
  }
  for (auto index = 0; index < 10; ++index) {
    profiler.transition("running");
  }
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  auto const& batch = sent->batches.front();
  CHECK_EQUAL(batch.dropped.value(), (300 - 255) + (10 - 8));
  REQUIRE_EQUAL(batch.diagnostics.size(), 256u);
  auto occurrences = int64_t{0};
  for (auto const& diagnostic : batch.diagnostics) {
    if (diagnostic.message == "repeated") {
      occurrences = diagnostic.occurrences.value();
    }
  }
  CHECK_EQUAL(occurrences, 600);
  CHECK_EQUAL(batch.runs.size(), 8u);
}

TEST("platform profiler points a user-defined operator's diagnostic at its "
     "call") {
  auto sent = std::make_shared<Sent>();
  auto reporter = recording(sent);
  auto profiler = PlatformProfiler{reporter, "pipeline", 1};
  auto sources = SourceMap{};
  auto input = Source::new_source("from {}\nouter\n", "<pipeline>", true);
  auto body = Source::new_source("head -1\n", "<body>", false);
  auto input_id = input->index;
  auto body_id = body->index;
  sources.add_source(std::move(input));
  sources.add_source(std::move(body));
  auto call = sources.add_call_site(location{8, 13, input_id, 0});
  profiler.diagnostic(
    diagnostic::error("oops").primary(location{5, 7, body_id, call}).done(),
    sources);
  profiler.transition("failed");
  flush(reporter);
  REQUIRE_EQUAL(sent->batches.size(), 1u);
  REQUIRE_EQUAL(sent->batches[0].diagnostics.size(), 1u);
  auto const& reported = sent->batches[0].diagnostics[0];
  // The text says where the pipeline called the operator, as `tenzir` does.
  CHECK(reported.rendered.contains("called from here"));
  CHECK(reported.rendered.contains("outer"));
  // The spans are those of the definition only: the call, and not the body.
  REQUIRE_EQUAL(reported.annotations.size(), 1u);
  CHECK(reported.annotations[0].primary);
  CHECK_EQUAL(reported.annotations[0].text, "called from here");
  CHECK_EQUAL(reported.annotations[0].begin.value(), 8);
  CHECK_EQUAL(reported.annotations[0].end.value(), 13);
}

} // namespace tenzir
