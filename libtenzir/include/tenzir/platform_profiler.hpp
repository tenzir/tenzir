// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/arc.hpp>
#include <tenzir/async/stop_flag.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/controller/transport.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/platform_connection.hpp>
#include <tenzir/time.hpp>

#include <span>
#include <string>

namespace tenzir {

/// What the operators of a pipeline cost, summed over all of them.
struct PipelineResources {
  /// CPU time used since the previous sample.
  duration cpu = {};
  /// The time since the previous sample, which `cpu` covers. It is measured
  /// rather than assumed, so that a late sample does not inflate the rate.
  duration interval = {};
  /// Bytes waiting in the channels between operators at the sample.
  uint64_t queued_bytes = 0;
};

// From the generated API, which is too large to include for one name.
class ServedRequest;

class PlatformProfiler;

/// Everything a process reports to the platform: the samples, diagnostics and
/// run transitions of each pipeline run that a `PlatformProfiler` records, and
/// the requests the process answers.
///
/// It sends one batch a second for all of them, however many pipelines run,
/// and splits a batch only where the platform's limits require it. Its batches
/// share the process's boot and sequence. Pending data waits in bounded
/// queues, and delivery is best effort: a batch the platform does not take is
/// retried for up to two minutes, enough to outlast a rollout of the platform.
/// What is lost counts as dropped, and the next batch reports it. Copies share
/// the pending data and the sender.
class PlatformReporter {
public:
  /// The most ended runs whose reports wait for a batch, as they do while the
  /// platform is out of reach. Past it, the oldest one's report goes, and what
  /// it recorded counts as dropped.
  static constexpr auto max_ended_runs = std::size_t{1024};

  explicit PlatformReporter(PlatformConnection connection);
  /// Uses the same transport seam as generated clients, including fixtures.
  PlatformReporter(PlatformConnection connection, Box<Transport> transport);

  /// Keeps `served` for the next batch. Never waits: with too many pending,
  /// the request is dropped and the next batch counts it in `dropped`.
  auto request(ServedRequest served) const -> void;

  /// Batches what is pending once a second and sends it. Once `stop` is
  /// requested, it sends what is still pending, for at most a few seconds,
  /// and returns. Cancelled, it returns at once and what is pending is lost.
  ///
  /// A batch is due half a second into each second, after the runs sampled at
  /// its start, so that each batch carries one sample of every running
  /// pipeline. Stop it after the runs it reports on have ended, so that their
  /// last samples and transitions go out with its final batches.
  ///
  /// One `run` at a time: its batches have one sender.
  auto run(StopFlag stop) const -> Task<void>;

private:
  friend class PlatformProfiler;
  class State;

  Arc<State> state_;
};

/// One run of a pipeline, as the reporter it records into sees it.
///
/// It never sends anything itself: its reporter takes what it records into
/// the batch of each second. Copies share the run's counters and what is
/// pending. Once the last copy is gone, the reporter sends what is left and
/// forgets the run.
class PlatformProfiler {
public:
  /// The most samples of one run that wait for a batch. Past it, a sample
  /// merges into the last one pending, so that its counters and CPU time add
  /// up over their combined interval: a backlog costs resolution, not data.
  static constexpr auto max_pending_samples = std::size_t{8};

  PlatformProfiler(PlatformReporter const& reporter, std::string pipeline_id,
                   uint64_t run);

  auto diagnostic(tenzir::diagnostic const& value,
                  SourceMap const& sources) const -> void;
  /// Records the connector counters, and what the operators cost if known.
  ///
  /// A sample with resources is recorded even without traffic, so that a
  /// running pipeline reports regularly.
  auto sample(std::span<MetricsSnapshotEntry const> counters,
              Option<PipelineResources> resources = None{}) const -> void;
  auto transition(std::string state, Option<std::string> error = {}) const
    -> void;

  /// The reporter that sends what this run records.
  auto reporter() const -> PlatformReporter;

private:
  class State;
  Arc<State> state_;
};

/// Reports before a caller deduplicates, while retaining its local handler.
class PlatformDiagnosticHandler final : public diagnostic_handler {
public:
  PlatformDiagnosticHandler(diagnostic_handler& inner,
                            PlatformProfiler const& profiler,
                            SourceMap const& sources)
    : inner_{inner}, profiler_{profiler}, sources_{sources} {
  }
  auto report(tenzir::diagnostic const& value) -> void {
    profiler_.diagnostic(value, sources_);
  }
  auto emit(tenzir::diagnostic value) -> void override {
    if (enabled_) {
      report(value);
    }
    inner_.emit(std::move(value));
  }
  /// The runtime adapter reports before its own deduplication instead.
  auto disable() -> void {
    enabled_ = false;
  }

private:
  diagnostic_handler& inner_;
  PlatformProfiler const& profiler_;
  SourceMap const& sources_;
  bool enabled_ = true;
};

} // namespace tenzir
