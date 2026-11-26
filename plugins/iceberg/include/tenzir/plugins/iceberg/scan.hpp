//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Read facade isolating iceberg-cpp's scan planning, snapshot/branch
// inspection, and FileScanTaskReader mechanics behind a testable API for the
// `from_iceberg` operator.

#pragma once

#include "tenzir/plugins/iceberg/detail/arrow_read.hpp"
#include "tenzir/plugins/iceberg/types.hpp"

#include <tenzir/box.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/option.hpp>

#include <iceberg/metrics/metrics_reporter.h>
#include <iceberg/table_scan.h>
#include <iceberg/type_fwd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace iceberg {
class Table;
} // namespace iceberg

namespace tenzir::plugins::iceberg {

namespace ice = ::iceberg;

/// Target snapshot selector for a point-in-time table scan.
struct ScanTarget {
  /// Pin a specific snapshot by ID.
  Option<int64_t> snapshot_id;
  /// Resolve the latest snapshot committed at or before this Unix epoch
  /// timestamp in milliseconds.
  Option<int64_t> as_of_ms;
  /// Named branch or tag reference (`"main"` refers to the main branch).
  Option<std::string> ref;
};

/// Snapshot range for an incremental append scan `(from, to]`.
struct IncrementalRange {
  /// Exclusive starting snapshot ID. `None` scans from the oldest ancestor
  /// (inclusive) on local catalogs; REST server-side scan planning requires an
  /// explicit start snapshot.
  Option<int64_t> from_snapshot_id;
  /// Inclusive ending snapshot ID. `None` uses the current head of `branch`
  /// (or `"main"`).
  Option<int64_t> to_snapshot_id;
  /// Optional branch name (`None` or `"main"` uses the main branch).
  Option<std::string> branch;
};

/// Projection and filter pushdown options for scan planning.
struct ScanOptions {
  /// Columns to read.
  /// - `None`: select all columns.
  /// - `Some({})`: select no columns (enables the metadata-only fast path for
  ///   whole-file tasks when no delete files apply, and, without a filter and
  ///   with `count_from_metadata`, counting a snapshot from table metadata;
  ///   see `plan_snapshot_scan`).
  /// - `Some(paths)`: resolve each path against the snapshot schema, keeping
  ///   existing columns and enclosing non-struct containers (maps/lists).
  Option<ir::OptimizeProjection> projection;
  /// TQL predicates translated conservatively into Iceberg filter expressions
  /// for manifest and data-file pruning. Residual filters are always ignored
  /// because Tenzir runs exact TQL filters locally.
  ir::OptimizeFilter filter;
  /// Whether `plan_snapshot_scan` may answer a local scan without a filter and
  /// without columns from table metadata, as one synthetic task instead of one
  /// task per data file. Callers that resume from checkpoints must persist
  /// `ScanPlan::counted_from_metadata` and pass it back here on resume so the
  /// resumed scan uses the same task layout. Ignored by
  /// `plan_incremental_append_scan`.
  bool count_from_metadata = false;
  /// Plans with the schema of this ID instead of the one that the scan
  /// selects on its own, so that a restored scan resolves names, filters, and
  /// projected columns as the plan that its checkpoint refers to did. The scan
  /// itself still binds against its own schema; field names are mapped by
  /// field ID. Fails if the table metadata no longer has the schema.
  Option<int32_t> schema_id = None{};
};

/// Summary of snapshot operations in an incremental window `(from, to]`.
struct IncrementalWindowSummary {
  size_t append_snapshots = 0;
  size_t replace_snapshots = 0;
  size_t overwrite_snapshots = 0;
  size_t delete_snapshots = 0;
  size_t other_snapshots = 0;
};

/// Scan planning counters captured from iceberg-cpp's `ScanReport`.
struct ScanStats {
  int64_t total_data_manifests = 0;
  int64_t scanned_data_manifests = 0;
  int64_t skipped_data_manifests = 0;
  int64_t total_delete_manifests = 0;
  int64_t scanned_delete_manifests = 0;
  int64_t skipped_delete_manifests = 0;
  int64_t result_data_files = 0;
  int64_t skipped_data_files = 0;
  int64_t result_delete_files = 0;
  int64_t skipped_delete_files = 0;
  int64_t total_file_size_in_bytes = 0;
  int64_t total_delete_file_size_in_bytes = 0;
};

/// MetricsReporter that records the `ScanReport` emitted by a scan and logs a
/// concise summary at `TENZIR_DEBUG`.
class ScanMetricsCollector final : public ice::MetricsReporter {
public:
  auto Report(ice::MetricsReport const& report) -> ice::Status override;

  auto stats() const -> Option<ScanStats>;

private:
  mutable std::mutex mutex_;
  Option<ScanStats> stats_;
};

/// Size of the dedicated thread pool on which scan planning opens manifests
/// and the manifest lists of an incremental range. It bounds the planning
/// reads that iceberg-cpp submits to that pool, for all scan plans of this
/// process together. Reads that a planning thread issues itself are not
/// counted: the manifest list of a snapshot scan, and the manifest list that
/// the metadata count of `plan_snapshot_scan` reads.
inline constexpr auto plan_concurrency = size_t{16};

/// Planned table scan holding the resolved schemas, scan-scoped FileIO, and
/// either a lazy task stream (`DataTableScan`) or a deterministically sorted
/// task list (`IncrementalAppendScan`).
///
/// The plan's schema, mapping, and FileIO fields are immutable after planning
/// and safe to share concurrently across multiple `TaskBatchReader::open`
/// calls.
struct ScanPlan {
  /// Resolved snapshot ID, or `None` when the table has no snapshots yet.
  Option<int64_t> snapshot_id;
  /// Schema of the resolved snapshot (or table schema for current branch), or
  /// the one that `ScanOptions::schema_id` selects. Names in events, filters,
  /// and projected columns resolve against it.
  std::shared_ptr<ice::Schema> table_schema;
  /// Historical table schemas passed to `FileScanTaskReader` for MoR delete
  /// resolution across schema evolutions.
  std::vector<std::shared_ptr<ice::Schema>> schemas;
  /// Projected schema exposed by `FileScanTaskReader`.
  std::shared_ptr<ice::Schema> projected_schema;
  /// Optional default name mapping parsed from the table's
  /// `schema.name-mapping.default` property for legacy files lacking field IDs.
  std::shared_ptr<ice::NameMapping> name_mapping;
  /// FileIO obtained from `scan->io()` AFTER planning so scan-scoped vended
  /// credentials from REST scan planning are honored.
  std::shared_ptr<ice::FileIO> io;
  /// Lazy task stream populated by `plan_snapshot_scan`.
  ice::FileScanTaskStreamPtr task_stream;
  /// Deterministically ordered tasks populated by
  /// `plan_incremental_append_scan`, by server-side REST planning, and by the
  /// metadata count of `plan_snapshot_scan`.
  std::vector<std::shared_ptr<ice::FileScanTask>> tasks;
  size_t next_task_index = 0;
  /// Whether `tasks` holds a single synthetic task representing all rows of
  /// the snapshot counted from table metadata rather than per-file tasks.
  bool counted_from_metadata = false;
  /// Metrics reporter attached to the underlying scan.
  std::shared_ptr<ScanMetricsCollector> metrics;

  /// Returns the next `FileScanTask`, or `None` when exhausted.
  auto next_task() -> Result<Option<std::shared_ptr<ice::FileScanTask>>>;

  /// Returns the latest `ScanStats` reported for this plan, if available.
  auto stats() const -> Option<ScanStats>;
};

/// Streams converted `nova::Events` batches from one `ice::FileScanTask`.
///
/// Uses a zero-column metadata-only fast path (never opening the data file)
/// for whole-file tasks when `projected_schema->fields().empty()` and
/// `task.delete_files().empty()`.
class TaskBatchReader {
public:
  ~TaskBatchReader();
  TaskBatchReader(TaskBatchReader&&) noexcept;
  auto operator=(TaskBatchReader&&) noexcept -> TaskBatchReader&;
  TaskBatchReader(TaskBatchReader const&) = delete;
  auto operator=(TaskBatchReader const&) -> TaskBatchReader& = delete;

  /// Opens `task` using `plan`'s FileIO, schemas, and `read_options`.
  /// Safe to invoke concurrently against the same `ScanPlan const&` for
  /// distinct tasks (unless `plan.name_mapping` is non-null, because
  /// iceberg-cpp builds `NameMapping` lookup tables lazily without locking).
  static auto open(ScanPlan const& plan, ice::FileScanTask const& task,
                   ArrowReadOptions read_options,
                   size_t batch_size = defaults::import::table_slice_size)
    -> Result<TaskBatchReader>;

  /// Reads the next non-empty batch, or `None` when the task is exhausted.
  auto next() -> Result<Option<ArrowReadBatch>>;

  /// Whether this task used the zero-column metadata-only fast path without
  /// opening the underlying data file.
  auto used_metadata_only_fast_path() const -> bool;

  /// Estimated byte size of the task's data file (0 for the metadata-only fast
  /// path).
  auto file_size_bytes() const -> uint64_t;

private:
  struct Impl;
  Box<Impl> impl_;
  explicit TaskBatchReader(Box<Impl> impl);
};

/// Refreshes `table`'s metadata from the catalog.
auto refresh_table(ice::Table& table) -> Result<void>;

/// Returns whether `ref` names a tag (as opposed to a branch) in `table`.
/// Fails if `ref` does not exist in `table` (except `"main"`, which is always
/// treated as a branch).
auto is_tag_ref(ice::Table const& table, std::string_view ref) -> Result<bool>;

/// Resolves the current snapshot ID for `ref` (or the main branch when `None`
/// or `"main"`). Returns `None` when the table has no snapshots yet. Fails if
/// a non-main `ref` does not exist.
auto resolve_current_snapshot(ice::Table const& table,
                              Option<std::string_view> ref = None{})
  -> Result<Option<int64_t>>;

/// Inspects the snapshot chain `(from_snapshot_id_exclusive,
/// to_snapshot_id_inclusive]`. Verifies that `from_snapshot_id_exclusive` is a
/// strict ancestor of `to_snapshot_id_inclusive` and counts each snapshot
/// operation kind in the window. Fails with a not-found error if, and only if,
/// the metadata lacks `to_snapshot_id_inclusive` itself, so callers can
/// attribute it to the end of the window. Fails with a permanent error if the
/// loaded metadata proves that it is not an ancestor (the branch was
/// rewritten), or if the snapshot history in the metadata ends before reaching
/// it (snapshots expired, or the catalog returned only a part of them).
auto inspect_incremental_window(ice::Table const& table,
                                int64_t from_snapshot_id_exclusive,
                                int64_t to_snapshot_id_inclusive)
  -> Result<IncrementalWindowSummary>;

/// Plans a point-in-time snapshot scan against `table`.
///
/// For an empty table (0 snapshots) with no explicit `snapshot_id`/`as_of_ms`,
/// returns an empty `ScanPlan` with `snapshot_id == None`. Local client-side
/// scans stream lazily via `PlanFilesStream()`; server-side REST scans
/// (`RestTableScan`) sort planned tasks deterministically by `(file_path,
/// start, length)` so checkpoint restores see a stable order.
///
/// A local scan opens up to 32 manifests ahead of the task that it returns,
/// `plan_concurrency` of them at a time, and yields their files in
/// manifest-list order, then in manifest order. That matches the order of a
/// serial scan, so it does not depend on timing, and `head` still stops reading
/// manifests early.
///
/// Without a filter and without projected columns, the plan needs only the
/// number of rows. If `ScanOptions::count_from_metadata` allows it, the scan
/// is planned locally (not a server-planned `RestTableScan`, whose storage
/// credentials may only arrive with the plan), and table metadata proves that
/// number, the plan reads no manifest and holds a single synthetic whole-file
/// task that carries it as its record count
/// (`ScanPlan::counted_from_metadata`); `TaskBatchReader` turns the task into
/// empty rows like any other metadata-only task. The proof requires all of the
/// following, and falls back to reading manifests otherwise:
/// - the snapshot summary has `total-records`, `total-delete-files`,
///   `total-position-deletes`, and `total-equality-deletes` as non-negative
///   integers, with the three delete totals equal to zero;
/// - the manifest list has no delete manifest with live files;
/// - every data manifest in the manifest list states its added and existing
///   row counts, and their sum equals `total-records`.
auto plan_snapshot_scan(ice::Table const& table, ScanTarget const& target,
                        ScanOptions const& options = {}) -> Result<ScanPlan>;

/// Plans an incremental append scan over `range` against `table`.
///
/// Tasks are sorted deterministically by `(file_path, start, length)` so
/// checkpoint restores can resume at a fixed task index. Manifests are read
/// `plan_concurrency` at a time.
auto plan_incremental_append_scan(ice::Table const& table,
                                  IncrementalRange const& range,
                                  ScanOptions const& options = {})
  -> Result<ScanPlan>;

} // namespace tenzir::plugins::iceberg
