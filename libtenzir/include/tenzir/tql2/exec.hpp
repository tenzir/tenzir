//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/async/executor.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/option.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/variant.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace tenzir {

/// Per-operator aggregated profiling data emitted each tick.
struct OperatorProfileEntry {
  std::string operator_id;
  std::string operator_type;
  double cpu = 0.0;
  uint64_t task_count = 0;
  uint64_t bytes_in = 0;
  uint64_t bytes_out = 0;
  uint64_t batches_in = 0;
  uint64_t batches_out = 0;
  uint64_t events_in = 0;
  uint64_t events_out = 0;
  uint64_t signals_in = 0;
  uint64_t signals_out = 0;
  uint64_t input_bytes = 0;
};

/// Aggregated profiler snapshot emitted each tick.
struct ProfilerSnapshot {
  time timestamp = {};
  std::vector<OperatorProfileEntry> operators;
};

struct NoProfiler {};

/// Collects operator metrics and optionally sends profiler snapshots as table
/// slices to an importer actor.
struct NodeProfiler {
  metrics_receiver_actor metrics;
  struct Importer {
    importer_actor actor;
    std::string pipeline_id;
  };
  Option<Importer> importer;
};

/// Generates a Perfetto trace file.
struct PerfettoProfiler {
  std::string path;
};

/// Profiler configuration for a pipeline execution.
using Profiler = variant<NoProfiler, NodeProfiler, PerfettoProfiler>;

/// Build table slices from a profiler snapshot, adding a pipeline_id field.
auto build_profiler_slices(ProfilerSnapshot const& snapshot,
                           std::string_view pipeline_id)
  -> std::vector<table_slice>;

auto exec2(std::string_view source, diagnostic_handler& dh,
           const exec_config& cfg, caf::actor_system& sys) -> bool;

auto compile(ast::pipeline&& pipe, session ctx) -> failure_or<pipeline>;

auto parse_and_compile(std::string_view source, session ctx)
  -> failure_or<pipeline>;

/// Run a closed pipeline from a list of operators.
auto run_plan(OperatorChain<void, void> chain, caf::actor_system& sys,
              DiagHandler& dh, Profiler profiler) -> Task<failure_or<void>>;

} // namespace tenzir
