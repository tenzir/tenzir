// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/controller/api_support.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/platform_profiler.hpp>
#include <tenzir/test/test.hpp>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>

#include <array>

namespace tenzir {
namespace {

class RecordingTransport final : public Transport {
public:
  explicit RecordingTransport(std::shared_ptr<std::vector<Batch>> batches,
                              std::vector<uint16_t> statuses = {})
    : batches_{std::move(batches)}, statuses_{std::move(statuses)} {
  }
  auto request(HttpRequest request)
    -> Task<Result<HttpResponse, RequestError>> override {
    auto document
      = api::Document::parse(request.body, "Batch").expect("valid JSON");
    batches_->push_back(parse_batch(document.root()).expect("valid batch"));
    auto status = batches_->size() <= statuses_.size()
                    ? statuses_[batches_->size() - 1]
                    : uint16_t{204};
    if (status == 0) {
      co_return Err{RequestError{"lost acknowledgement"}};
    }
    co_return HttpResponse{status, {}};
  }

private:
  std::shared_ptr<std::vector<Batch>> batches_;
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

class CancelOnceTransport final : public Transport {
public:
  explicit CancelOnceTransport(std::shared_ptr<std::vector<Batch>> batches)
    : inner_{std::move(batches)} {
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

} // namespace

TEST("platform wire counters retain safe numeric bounds") {
  CHECK(TelemetryCount::make(-1).is_err());
  CHECK(TelemetryCount::make(9'007'199'254'740'991).is_ok());
  CHECK(TelemetryCount::make(9'007'199'254'740'992).is_err());
  CHECK(TelemetrySmallCount::make(-1).is_err());
  CHECK(TelemetrySmallCount::make(4'294'967'295).is_ok());
  CHECK(TelemetrySmallCount::make(4'294'967'296).is_err());
  CHECK(TenzirTelemetryTelemetrySmallCount::make(4'294'967'295).is_ok());
  CHECK(TenzirTelemetryTelemetrySmallCount::make(4'294'967'296).is_err());
  auto numeric = api::Document::parse("7", "count").expect("valid JSON");
  auto parsed = parse_telemetry_count(numeric.root());
  REQUIRE(parsed.is_ok());
  CHECK_EQUAL(parsed.unwrap().value(), 7);
  auto text = api::Document::parse(R"("7")", "count").expect("valid JSON");
  CHECK(parse_telemetry_count(text.root()).is_err());
}

TEST("platform profiler differences connector counters and folds diagnostics") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto run = uint64_t{4'294'967'296};
  auto profiler
    = PlatformProfiler{{}, "pipeline", run, RecordingTransport{batches}};
  auto counters = std::array{external(5), external(9, MetricsDirection::write)};
  profiler.sample(counters);
  counters[0].value = 8;
  auto sources = SourceMap{};
  auto warning = diagnostic{severity::warning, "retrying"};
  warning.notes.emplace_back(diagnostic_note_kind::hint, "try again");
  profiler.diagnostic(warning, sources);
  profiler.diagnostic(warning, sources);
  profiler.sample(counters);
  profiler.transition("finished");
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 3u);
  CHECK_EQUAL((*batches)[0].pipelines[0].run.value(), run);
  CHECK_EQUAL((*batches)[0].pipelines[0].sources[0].events.value(), 5);
  CHECK_EQUAL((*batches)[0].pipelines[0].sinks[0].events.value(), 9);
  CHECK_EQUAL((*batches)[1].pipelines[0].sources[0].events.value(), 3);
  CHECK((*batches)[1].pipelines[0].sinks.empty());
  REQUIRE_EQUAL((*batches)[1].diagnostics.size(), 1u);
  CHECK_EQUAL((*batches)[1].diagnostics[0].run.value(), run);
  CHECK_EQUAL((*batches)[1].diagnostics[0].occurrences.value(), 2);
  REQUIRE_EQUAL((*batches)[1].diagnostics[0].notes.size(), 1u);
  CHECK_EQUAL((*batches)[1].diagnostics[0].notes[0].kind, "hint");
  CHECK_EQUAL((*batches)[1].diagnostics[0].notes[0].message, "try again");
  CHECK((*batches)[1].diagnostics[0].first_seen
        <= (*batches)[1].diagnostics[0].last_seen);
  CHECK_EQUAL((*batches)[2].runs[0].state, RunState::finished);
  CHECK_EQUAL((*batches)[2].runs[0].run.value(), run);
  CHECK((*batches)[0].operators.empty());
}

TEST("platform profiler keeps rapid samples distinct at millisecond "
     "precision") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler
    = PlatformProfiler{{}, "pipeline", 1, RecordingTransport{batches}};
  auto copy = profiler;
  for (auto events = uint64_t{1}; events <= 8; ++events) {
    auto counters = std::array{external(events)};
    (events % 2 == 0 ? copy : profiler).sample(counters);
  }
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 8u);
  for (auto index = size_t{0}; index < batches->size(); ++index) {
    auto const& samples = (*batches)[index].pipelines;
    REQUIRE_EQUAL(samples.size(), 1u);
    auto const& sample = samples[0];
    CHECK_EQUAL(sample.sources[0].events.value(), 1);
    CHECK_EQUAL(sample.window,
                floor(sample.window, std::chrono::milliseconds{1}));
    if (index > 0) {
      CHECK(sample.window >= (*batches)[index - 1].pipelines[0].window
                               + std::chrono::milliseconds{1});
    }
  }
}

TEST("platform profiler bounds pending batches and reports drops in final "
     "flush") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler
    = PlatformProfiler{{}, "pipeline", 1, RecordingTransport{batches}};
  for (auto events = uint64_t{1}; events <= 40; ++events) {
    auto counters = std::array{external(events)};
    profiler.sample(counters);
  }
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 17u);
  CHECK((*batches)[16].dropped.value() >= 24);
  auto other = PlatformProfiler{{}, "other", 2, RecordingTransport{batches}};
  other.transition("failed");
  folly::coro::blockingWait(other.flush());
  REQUIRE_EQUAL(batches->size(), 18u);
  CHECK_EQUAL((*batches)[0].boot, (*batches)[17].boot);
  CHECK((*batches)[0].seq.value() < (*batches)[17].seq.value());
  CHECK_EQUAL((*batches)[17].runs[0].pipeline_id.to_string(), "other");
}

TEST("platform profiler retries the immutable batch") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler = PlatformProfiler{
    {},
    "pipeline",
    1,
    RecordingTransport{batches, std::vector<uint16_t>{0, 429, 204}}};
  auto counters = std::array{external(5)};
  profiler.sample(counters);
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 3u);
  auto first = std::string{};
  write_json((*batches)[0], first);
  for (auto const& batch : *batches) {
    auto encoded = std::string{};
    write_json(batch, encoded);
    CHECK_EQUAL(encoded, first);
  }
}

TEST("platform profiler stops after three transient failures") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler = PlatformProfiler{
    {},
    "pipeline",
    1,
    RecordingTransport{batches, std::vector<uint16_t>{500, 429, 0}}};
  profiler.transition("running");
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 3u);
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 4u);
  CHECK_EQUAL((*batches)[3].dropped.value(), 1);
}

TEST("platform profiler flushes a canceled batch before later samples") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler
    = PlatformProfiler{{}, "pipeline", 1, CancelOnceTransport{batches}};
  for (auto events = uint64_t{1}; events <= 2; ++events) {
    auto counters = std::array{external(events)};
    profiler.sample(counters);
  }
  auto canceled = false;
  try {
    folly::coro::blockingWait(profiler.send());
  } catch (folly::OperationCancelled const&) {
    canceled = true;
  }
  REQUIRE(canceled);
  REQUIRE_EQUAL(batches->size(), 1u);
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 3u);
  auto original = std::string{};
  auto retried = std::string{};
  write_json((*batches)[0], original);
  write_json((*batches)[1], retried);
  CHECK_EQUAL(original, retried);
  CHECK((*batches)[1].seq.value() < (*batches)[2].seq.value());
  CHECK((*batches)[1].pipelines[0].window < (*batches)[2].pipelines[0].window);
}

TEST("platform profiler rejects permanent failures without retrying") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler = PlatformProfiler{
    {}, "pipeline", 1, RecordingTransport{batches, std::vector<uint16_t>{401}}};
  profiler.transition("running");
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 1u);
  // A later successful flush accounts for the lost batch once.
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 2u);
  CHECK_EQUAL((*batches)[1].dropped.value(), 1);
}

TEST("platform profiler ignores unattributed and internal counters") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler
    = PlatformProfiler{{}, "pipeline", 1, RecordingTransport{batches}};
  auto counters = std::array{external(5), external(7), external(9)};
  counters[1].connector = "from_http";
  counters[1].visibility = MetricsVisibility::internal_;
  counters[2].connector.clear();
  profiler.sample(counters);
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 1u);
  auto const& flows = (*batches)[0].pipelines[0].sources;
  REQUIRE_EQUAL(flows.size(), 1u);
  CHECK_EQUAL(flows[0].connector, "from_kafka");
  CHECK_EQUAL(flows[0].events.value(), 5);
}

TEST("platform profiler bounds the final flush while HTTP is stalled") {
  auto calls = std::make_shared<int>(0);
  auto profiler = PlatformProfiler{{}, "pipeline", 1, SlowTransport{calls}};
  profiler.transition("finished");
  auto start = std::chrono::steady_clock::now();
  folly::coro::blockingWait(profiler.flush());
  CHECK_EQUAL(*calls, 1);
  CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds{5});
}

TEST("platform profiler accounts for diagnostic and transition overflow") {
  auto batches = std::make_shared<std::vector<Batch>>();
  auto profiler
    = PlatformProfiler{{}, "pipeline", 1, RecordingTransport{batches}};
  auto sources = SourceMap{};
  auto warning = diagnostic{severity::warning, "repeated"};
  for (auto index = 0; index < 600; ++index) {
    profiler.diagnostic(warning, sources);
  }
  for (auto index = 0; index < 10; ++index) {
    profiler.transition("running");
  }
  folly::coro::blockingWait(profiler.flush());
  REQUIRE_EQUAL(batches->size(), 1u);
  auto const& batch = batches->front();
  CHECK_EQUAL(batch.dropped.value(), 346);
  REQUIRE_EQUAL(batch.diagnostics.size(), 1u);
  CHECK_EQUAL(batch.diagnostics[0].occurrences.value(), 256);
  CHECK_EQUAL(batch.runs.size(), 8u);
}

} // namespace tenzir
