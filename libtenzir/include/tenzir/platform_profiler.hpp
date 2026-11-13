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

/// Per-run state; copies share counters, diagnostics and the bounded sender.
class PlatformProfiler {
public:
  PlatformProfiler(PlatformConnection connection, std::string pipeline_id,
                   uint64_t run);
  /// Uses the same transport seam as generated clients, including fixtures.
  PlatformProfiler(PlatformConnection connection, std::string pipeline_id,
                   uint64_t run, Box<Transport> transport);
  auto diagnostic(tenzir::diagnostic const& value,
                  SourceMap const& sources) const -> void;
  /// Reports the connector counters, and what the operators cost if known.
  ///
  /// A sample with resources is sent even without traffic, so that a running
  /// pipeline reports regularly.
  auto sample(std::span<MetricsSnapshotEntry const> counters,
              Option<PipelineResources> resources = None{}) const -> void;
  auto transition(std::string state, Option<std::string> error = {}) const
    -> void;
  auto send() const -> Task<void>;
  auto flush() const -> Task<void>;

private:
  class State;
  Arc<State> state_;
};

/// What a deployment reports of itself rather than of one pipeline run: for
/// now, the requests it answers.
///
/// Its batches share the process's boot and sequence with those of the
/// pipeline runs, and are bounded and best effort in the same way. Copies
/// share the pending samples and the sender.
class PlatformReporter {
public:
  /// The most served requests one batch carries. One takes at most about
  /// 1.1 KiB on the wire, so a full batch stays well under the 256 KiB limit
  /// of a batch.
  static constexpr auto max_batch_requests = std::size_t{200};

  explicit PlatformReporter(PlatformConnection connection);
  /// Uses the same transport seam as generated clients, including fixtures.
  PlatformReporter(PlatformConnection connection, Box<Transport> transport);

  /// Keeps `sample` for the next batch. Never waits: with too many pending,
  /// the sample is dropped and the next batch counts it in `dropped`.
  auto request(ServedRequest served) const -> void;

  /// Batches what is pending once a second and sends it. Once `stop` is
  /// requested, it sends what is still pending, for at most a few seconds,
  /// and returns. Cancelled, it returns at once and what is pending is lost.
  ///
  /// One `run` at a time: its batches have one sender.
  auto run(StopFlag stop) const -> Task<void>;

private:
  /// The last part of `run`, once its sender has let go of the queue.
  auto flush() const -> Task<void>;

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
