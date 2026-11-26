//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/scan.hpp"

#include "tenzir/plugins/iceberg/catalog.hpp"
#include "tenzir/plugins/iceberg/filter.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/logger.hpp>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <fmt/format.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog/rest/rest_table_scan.h>
#include <iceberg/constants.h>
#include <iceberg/data/file_scan_task_reader.h>
#include <iceberg/data_table_scan.h>
#include <iceberg/incremental_append_scan.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/name_mapping.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/snapshot.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/table_properties.h>
#include <iceberg/type.h>
#include <iceberg/util/executor.h>
#include <iceberg/util/snapshot_util.h>
#include <iceberg/util/timepoint.h>
#include <parquet/exception.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <exception>
#include <future>
#include <limits>
#include <mutex>
#include <ranges>
#include <simdjson.h>
#include <span>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace tenzir::plugins::iceberg {

namespace {

auto ensure_readers_registered() -> void {
  // Register Parquet/Avro readers and Arrow MoR batch projection explicitly so
  // scan callers (including unit tests using InMemoryCatalog) do not depend on
  // static initializers or open_catalog() having been called first.
  static std::once_flag flag;
  std::call_once(flag, [] {
    ice::arrow::RegisterAll();
    ice::parquet::RegisterAll();
    ice::avro::RegisterAll();
  });
}

auto counter_value(std::optional<ice::CounterResult> const& counter)
  -> int64_t {
  return counter ? counter->value : 0;
}

auto find_field(std::span<ice::SchemaField const> fields, std::string_view name)
  -> ice::SchemaField const* {
  for (auto const& field : fields) {
    if (field.name() == name) {
      return &field;
    }
  }
  return nullptr;
}

/// Resolves TQL projection paths against `schema` into column names suitable
/// for `TableScanBuilder::Select`.
///
/// - Returns `None` when all columns must be read (no projection or `this`).
/// - Returns `Some({})` when no columns exist or no columns were requested.
/// - Preserves enclosing non-struct containers (maps/lists/primitives) when a
///   path traverses into them, because e.g. string-keyed Iceberg maps become
///   Tenzir records at read time.
auto resolve_selected_columns(Option<ir::OptimizeProjection> const& projection,
                              ice::Schema const& schema)
  -> Option<std::vector<std::string>> {
  if (not projection) {
    return None{};
  }
  if (projection->empty()) {
    return std::vector<std::string>{};
  }
  auto selected = std::vector<std::string>{};
  auto seen = std::unordered_set<std::string>{};
  for (auto const& field_path : *projection) {
    auto const& segments = field_path.path();
    if (segments.empty()) {
      // Empty field_path stands for `this` (the entire event).
      return None{};
    }
    auto current_fields = schema.fields();
    auto resolved = std::string{};
    auto const* top_field = static_cast<ice::SchemaField const*>(nullptr);
    auto const* matched_field = static_cast<ice::SchemaField const*>(nullptr);
    for (auto i = size_t{0}; i < segments.size(); ++i) {
      auto const& seg_name = segments[i].id.name;
      auto const* field = find_field(current_fields, seg_name);
      if (not field) {
        matched_field = nullptr;
        break;
      }
      if (i == 0) {
        top_field = field;
      }
      if (not resolved.empty()) {
        resolved.push_back('.');
      }
      resolved.append(field->name());
      if (i + 1 == segments.size()) {
        matched_field = field;
        break;
      }
      if (field->type()->type_id() != ice::TypeId::kStruct) {
        // Path continues into a map, list, or primitive: keep the enclosing
        // column intact so downstream TQL evaluation sees its full value (and
        // emits any expected runtime warning on invalid field access).
        matched_field = field;
        break;
      }
      current_fields
        = static_cast<ice::StructType const&>(*field->type()).fields();
    }
    if (not matched_field) {
      continue;
    }
    // Confirm that Iceberg's dotted `FindFieldByName` resolves back to the
    // exact field ID we traversed to (guards against literal dots in field
    // names colliding with nested struct paths, e.g., `"a.b"` vs `a.b`).
    auto by_name = schema.FindFieldByName(resolved, /*case_sensitive=*/true);
    if (not by_name.has_value() or not by_name->has_value()
        or by_name->value().get().field_id() != matched_field->field_id()) {
      // For a nested path, widen to the enclosing top-level struct if its name
      // resolves unambiguously; otherwise fall back to full schema projection.
      if (top_field != nullptr and top_field != matched_field) {
        auto top_lookup
          = schema.FindFieldByName(top_field->name(), /*case_sensitive=*/true);
        if (top_lookup.has_value() and top_lookup->has_value()
            and top_lookup->value().get().field_id() == top_field->field_id()) {
          resolved = std::string{top_field->name()};
        } else {
          return None{};
        }
      } else {
        return None{};
      }
    }
    if (seen.insert(resolved).second) {
      selected.push_back(std::move(resolved));
    }
  }
  return selected;
}

auto resolve_schema_for_scan(ice::TableMetadata const& metadata,
                             Option<int64_t> snapshot_id,
                             bool use_snapshot_schema)
  -> Result<std::shared_ptr<ice::Schema>> {
  if (snapshot_id and use_snapshot_schema) {
    TRY(auto snapshot, translate(metadata.SnapshotById(*snapshot_id)));
    auto const schema_id
      = snapshot->schema_id.value_or(ice::Schema::kInitialSchemaId);
    return translate(metadata.SchemaById(schema_id));
  }
  return translate(metadata.Schema());
}

/// Returns the schema that `options.schema_id` selects, or `base` without
/// one.
auto select_scan_schema(ice::TableMetadata const& metadata,
                        std::shared_ptr<ice::Schema> base,
                        ScanOptions const& options)
  -> Result<std::shared_ptr<ice::Schema>> {
  if (not options.schema_id or base->schema_id() == *options.schema_id) {
    return base;
  }
  auto selected = metadata.SchemaById(*options.schema_id);
  if (not selected.has_value() or not *selected) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("cannot resume the scan: schema {} of the checkpointed plan "
                  "is no longer in the table metadata",
                  *options.schema_id),
    }};
  }
  return std::move(*selected);
}

/// Hands the filter and the projected columns of `options` to `builder`.
/// Paths resolve against `schema`. If the scan binds against another schema,
/// `base`, the filter names fields as `base` does, and the projection goes in
/// as a schema of `schema` instead of as column names.
template <class Builder>
auto push_scan_options(Builder& builder, ScanOptions const& options,
                       std::shared_ptr<ice::Schema> const& schema,
                       ice::Schema const& base) -> Result<void> {
  auto const same = schema.get() == &base;
  if (not options.filter.empty()) {
    auto expr = same ? translate_filter(options.filter, *schema)
                     : translate_filter(options.filter, *schema, base);
    if (expr) {
      builder.Filter(std::move(expr));
    }
  }
  auto selected = resolve_selected_columns(options.projection, *schema);
  if (same) {
    if (selected) {
      builder.Select(*selected);
    }
    return {};
  }
  if (not selected) {
    builder.Project(schema);
    return {};
  }
  TRY(auto projected, translate(schema->Select(*selected)));
  builder.Project(std::move(projected));
  return {};
}

auto resolve_as_of_on_branch(ice::TableMetadata const& metadata,
                             std::string_view branch_name, int64_t as_of_ms)
  -> Result<int64_t> {
  auto const target_time = ice::TimePointMsFromUnixMs(as_of_ms);
  if (branch_name == ice::SnapshotRef::kMainBranch) {
    return translate(
      ice::SnapshotUtil::SnapshotIdAsOfTime(metadata, target_time));
  }
  auto const it = metadata.refs.find(std::string{branch_name});
  if (it == metadata.refs.end() or not it->second) {
    return std::unexpected{Error{
      Error::Kind::not_found,
      fmt::format("reference `{}` not found", branch_name),
    }};
  }
  if (it->second->type() == ice::SnapshotRefType::kTag) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("cannot use `as_of` with tag `{}`", branch_name),
    }};
  }
  TRY(auto ancestors, translate(ice::SnapshotUtil::AncestorsOf(
                        metadata, it->second->snapshot_id)));
  for (auto const& snap : ancestors) {
    if (snap and snap->timestamp_ms <= target_time) {
      return snap->snapshot_id;
    }
  }
  return std::unexpected{Error{
    Error::Kind::permanent,
    fmt::format("cannot find a snapshot on branch `{}` at or before {}",
                branch_name, ice::FormatTimePointMs(target_time)),
  }};
}

/// RAII guard for an Arrow C Data Interface `ArrowArrayStream`.
struct ArrowStreamGuard {
  ArrowArrayStream stream{};

  ArrowStreamGuard() {
    stream.release = nullptr;
  }

  explicit ArrowStreamGuard(ArrowArrayStream s) : stream{s} {
  }

  ~ArrowStreamGuard() {
    release();
  }

  ArrowStreamGuard(ArrowStreamGuard&& other) noexcept : stream{other.stream} {
    other.stream.release = nullptr;
  }

  auto operator=(ArrowStreamGuard&& other) noexcept -> ArrowStreamGuard& {
    if (this != &other) {
      release();
      stream = other.stream;
      other.stream.release = nullptr;
    }
    return *this;
  }

  ArrowStreamGuard(ArrowStreamGuard const&) = delete;
  auto operator=(ArrowStreamGuard const&) -> ArrowStreamGuard& = delete;

  auto release() noexcept -> void {
    if (stream.release != nullptr) {
      stream.release(&stream);
      stream.release = nullptr;
    }
  }
};

struct ArrowSchemaGuard {
  ArrowSchema schema{};

  ArrowSchemaGuard() {
    schema.release = nullptr;
  }

  ~ArrowSchemaGuard() {
    if (schema.release != nullptr) {
      schema.release(&schema);
      schema.release = nullptr;
    }
  }

  ArrowSchemaGuard(ArrowSchemaGuard const&) = delete;
  auto operator=(ArrowSchemaGuard const&) -> ArrowSchemaGuard& = delete;
};

struct ArrowArrayGuard {
  ArrowArray array{};

  ArrowArrayGuard() {
    array.release = nullptr;
  }

  ~ArrowArrayGuard() {
    if (array.release != nullptr) {
      array.release(&array);
      array.release = nullptr;
    }
  }

  ArrowArrayGuard(ArrowArrayGuard const&) = delete;
  auto operator=(ArrowArrayGuard const&) -> ArrowArrayGuard& = delete;
};

auto stream_last_error(ArrowArrayStream& stream, int rc,
                       std::string_view file_path, std::string_view step)
  -> Error {
  auto const* detail = stream.get_last_error != nullptr
                         ? stream.get_last_error(&stream)
                         : nullptr;
  if (detail != nullptr and *detail != '\0') {
    return Error{
      Error::Kind::permanent,
      fmt::format("failed to {} Iceberg data file `{}`: {}", step, file_path,
                  detail),
    };
  }
  return Error{
    Error::Kind::permanent,
    fmt::format("failed to {} Iceberg data file `{}` (error code {})", step,
                file_path, rc),
  };
}

/// Parses an Iceberg `MappedFields` JSON array (`schema.name-mapping.default`).
auto parse_mapped_fields(simdjson::dom::array const& arr)
  -> Result<std::unique_ptr<ice::MappedFields>> {
  auto fields = std::vector<ice::MappedField>{};
  fields.reserve(arr.size());
  for (auto element : arr) {
    auto obj = simdjson::dom::object{};
    if (element.get(obj) != simdjson::SUCCESS) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        "expected an object entry in `schema.name-mapping.default`",
      }};
    }
    auto field_id = Option<int32_t>{None{}};
    auto raw_id = simdjson::dom::element{};
    if (obj["field-id"].get(raw_id) == simdjson::SUCCESS
        and not raw_id.is_null()) {
      auto parsed_id = int64_t{0};
      if (raw_id.get(parsed_id) != simdjson::SUCCESS
          or parsed_id < std::numeric_limits<int32_t>::min()
          or parsed_id > std::numeric_limits<int32_t>::max()) {
        return std::unexpected{Error{
          Error::Kind::permanent,
          "expected a 32-bit integer `field-id` in "
          "`schema.name-mapping.default`",
        }};
      }
      field_id = static_cast<int32_t>(parsed_id);
    }
    auto names = std::unordered_set<std::string>{};
    auto raw_names = simdjson::dom::element{};
    if (obj["names"].get(raw_names) == simdjson::SUCCESS
        and not raw_names.is_null()) {
      auto names_arr = simdjson::dom::array{};
      if (raw_names.get(names_arr) != simdjson::SUCCESS) {
        return std::unexpected{Error{
          Error::Kind::permanent,
          "expected a string array for `names` in "
          "`schema.name-mapping.default`",
        }};
      }
      for (auto name_el : names_arr) {
        auto name_sv = std::string_view{};
        if (name_el.get(name_sv) != simdjson::SUCCESS) {
          return std::unexpected{Error{
            Error::Kind::permanent,
            "expected string elements in `names` of "
            "`schema.name-mapping.default`",
          }};
        }
        names.emplace(name_sv);
      }
    }
    auto nested = std::shared_ptr<ice::MappedFields>{};
    auto raw_fields = simdjson::dom::element{};
    if (obj["fields"].get(raw_fields) == simdjson::SUCCESS
        and not raw_fields.is_null()) {
      auto child_arr = simdjson::dom::array{};
      if (raw_fields.get(child_arr) != simdjson::SUCCESS) {
        return std::unexpected{Error{
          Error::Kind::permanent,
          "expected an array for `fields` in `schema.name-mapping.default`",
        }};
      }
      TRY(auto child_mapped, parse_mapped_fields(child_arr));
      nested = std::shared_ptr<ice::MappedFields>{std::move(child_mapped)};
    }
    fields.push_back(ice::MappedField{
      .names = std::move(names),
      .field_id = field_id.to_std(),
      .nested_mapping = std::move(nested),
    });
  }
  return ice::MappedFields::Make(std::move(fields));
}

/// Parses the table's `schema.name-mapping.default` property, if present, into
/// an `iceberg::NameMapping` for reading legacy files without embedded field
/// IDs.
auto parse_default_name_mapping(ice::Table const& table)
  -> Result<std::shared_ptr<ice::NameMapping>> {
  auto const& configs = table.properties().configs();
  auto const it
    = configs.find(std::string{ice::TableProperties::kDefaultNameMapping});
  if (it == configs.end() or it->second.empty()) {
    return std::shared_ptr<ice::NameMapping>{};
  }
  auto parser = simdjson::dom::parser{};
  auto doc = simdjson::dom::element{};
  auto const err = parser.parse(it->second.data(), it->second.size()).get(doc);
  if (err != simdjson::SUCCESS) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("failed to parse `{}` table property: {}",
                  ice::TableProperties::kDefaultNameMapping,
                  simdjson::error_message(err)),
    }};
  }
  auto arr = simdjson::dom::array{};
  if (doc.get(arr) != simdjson::SUCCESS) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("expected a JSON array in `{}` table property",
                  ice::TableProperties::kDefaultNameMapping),
    }};
  }
  TRY(auto mapped, parse_mapped_fields(arr));
  return std::shared_ptr<ice::NameMapping>{
    ice::NameMapping::Make(std::move(mapped))};
}

/// Sorts planned scan tasks into a canonical order so ordinal checkpoint
/// resume is reproducible even when the underlying planner returns tasks in an
/// arbitrary order (e.g., `IncrementalAppendScan` or server-side REST `/plan`).
auto sort_tasks(std::vector<std::shared_ptr<ice::FileScanTask>>& tasks)
  -> void {
  std::ranges::sort(tasks, [](auto const& lhs, auto const& rhs) {
    auto const& lpath = lhs->data_file()->file_path;
    auto const& rpath = rhs->data_file()->file_path;
    if (lpath != rpath) {
      return lpath < rpath;
    }
    if (lhs->start() != rhs->start()) {
      return lhs->start() < rhs->start();
    }
    return lhs->length() < rhs->length();
  });
}

/// Runs the manifest reads that iceberg-cpp fans out while planning a scan.
///
/// iceberg-cpp plans synchronously: the planning thread submits one task per
/// manifest and blocks until all of them finish. A dedicated bounded pool of
/// `plan_concurrency` threads caps concurrent object-store fan-out across all
/// scans in this process while keeping blocking I/O off executor threads.
class PlanExecutor final : public ice::Executor {
public:
  PlanExecutor()
    : pool_{std::make_pair(plan_concurrency, size_t{1}),
            folly::CPUThreadPoolExecutor::makeLifoSemQueue(),
            std::make_shared<folly::NamedThreadFactory>("TenzirIcePlan")} {
    pool_.setThreadDeathTimeout(std::chrono::seconds{10});
  }

  ~PlanExecutor() override {
    pool_.join();
  }

  PlanExecutor(PlanExecutor const&) = delete;
  auto operator=(PlanExecutor const&) -> PlanExecutor& = delete;
  PlanExecutor(PlanExecutor&&) = delete;
  auto operator=(PlanExecutor&&) -> PlanExecutor& = delete;

  /// Process-wide instance outliving every scan and task stream, as required
  /// by iceberg-cpp's borrowed executor contract.
  static auto get() -> PlanExecutor& {
    static PlanExecutor instance;
    return instance;
  }

  auto Submit(ice::ExecutorTask task) -> ice::Status override {
    pool_.add([task = std::move(task)]() mutable {
      try {
        std::move(task)();
      } catch (std::exception const& e) {
        TENZIR_WARN("from_iceberg: scan planning task failed: {}", e.what());
      } catch (...) {
        TENZIR_WARN("from_iceberg: scan planning task failed");
      }
    });
    return {};
  }

private:
  folly::CPUThreadPoolExecutor pool_;
};

/// Translates a broken promise from a failed planning task into a permanent
/// `Error`.
auto planning_failure(std::future_error const& e) -> Error {
  return Error{
    Error::Kind::permanent,
    fmt::format("a scan planning task ended without a result: {}", e.what()),
  };
}

/// Parses snapshot summary entry `key` as a non-negative base-10 integer.
auto parse_summary_count(
  std::unordered_map<std::string, std::string> const& summary,
  std::string const& key) -> Option<int64_t> {
  auto const it = summary.find(key);
  if (it == summary.end()) {
    return None{};
  }
  auto const& text = it->second;
  auto const* const first = text.data();
  auto const* const last = first + text.size();
  auto value = int64_t{0};
  auto const [ptr, ec] = std::from_chars(first, last, value);
  if (ec != std::errc{} or ptr != last or value < 0) {
    return None{};
  }
  return Option<int64_t>{value};
}

/// Returns the number of rows an unfiltered scan of `snapshot` yields when
/// table metadata proves it without reading any manifest, or `None` otherwise.
///
/// The snapshot summary alone is not trusted: the count is accepted only when
/// the summary reports zero delete files/deletes and reading the manifest list
/// independently confirms zero live delete manifests and matching live row
/// counts across all data manifests.
auto row_count_from_metadata(ice::Snapshot const& snapshot,
                             std::shared_ptr<ice::FileIO> const& io)
  -> Option<int64_t> {
  if (snapshot.manifest_list.empty() or not io) {
    return None{};
  }
  auto const total_records = parse_summary_count(
    snapshot.summary, ice::SnapshotSummaryFields::kTotalRecords);
  auto const delete_files = parse_summary_count(
    snapshot.summary, ice::SnapshotSummaryFields::kTotalDeleteFiles);
  auto const position_deletes = parse_summary_count(
    snapshot.summary, ice::SnapshotSummaryFields::kTotalPosDeletes);
  auto const equality_deletes = parse_summary_count(
    snapshot.summary, ice::SnapshotSummaryFields::kTotalEqDeletes);
  if (not total_records or not delete_files or not position_deletes
      or not equality_deletes) {
    return None{};
  }
  if (*delete_files != 0 or *position_deletes != 0 or *equality_deletes != 0) {
    return None{};
  }
  try {
    auto const reader = ice::SnapshotReader{&snapshot};
    auto manifests = reader.Manifests(io);
    if (not manifests.has_value()) {
      return None{};
    }
    auto live_rows = int64_t{0};
    for (auto const& manifest : *manifests) {
      if (manifest.content != ice::ManifestContent::kData) {
        if (manifest.has_added_files() or manifest.has_existing_files()) {
          return None{};
        }
        continue;
      }
      if (not manifest.added_rows_count or not manifest.existing_rows_count) {
        return None{};
      }
      auto const added = *manifest.added_rows_count;
      auto const existing = *manifest.existing_rows_count;
      if (added < 0 or existing < 0) {
        return None{};
      }
      auto const remaining = *total_records - live_rows;
      if (added > remaining or existing > remaining - added) {
        return None{};
      }
      live_rows += added + existing;
    }
    if (live_rows != *total_records) {
      return None{};
    }
  } catch (std::exception const&) {
    return None{};
  }
  return total_records;
}

} // namespace

auto ScanMetricsCollector::Report(ice::MetricsReport const& report)
  -> ice::Status {
  if (auto const* scan = std::get_if<ice::ScanReport>(&report)) {
    auto const& m = scan->scan_metrics;
    auto captured = ScanStats{
      .total_data_manifests = counter_value(m.total_data_manifests),
      .scanned_data_manifests = counter_value(m.scanned_data_manifests),
      .skipped_data_manifests = counter_value(m.skipped_data_manifests),
      .total_delete_manifests = counter_value(m.total_delete_manifests),
      .scanned_delete_manifests = counter_value(m.scanned_delete_manifests),
      .skipped_delete_manifests = counter_value(m.skipped_delete_manifests),
      .result_data_files = counter_value(m.result_data_files),
      .skipped_data_files = counter_value(m.skipped_data_files),
      .result_delete_files = counter_value(m.result_delete_files),
      .skipped_delete_files = counter_value(m.skipped_delete_files),
      .total_file_size_in_bytes = counter_value(m.total_file_size_in_bytes),
      .total_delete_file_size_in_bytes
      = counter_value(m.total_delete_file_size_in_bytes),
    };
    {
      auto lock = std::scoped_lock{mutex_};
      stats_ = captured;
    }
    TENZIR_DEBUG("from_iceberg scan report: table=`{}` snapshot={} "
                 "data_manifests={}/{} (skipped={}) data_files={} (skipped={}) "
                 "delete_files={} bytes={}",
                 scan->table_name, scan->snapshot_id,
                 captured.scanned_data_manifests, captured.total_data_manifests,
                 captured.skipped_data_manifests, captured.result_data_files,
                 captured.skipped_data_files, captured.result_delete_files,
                 captured.total_file_size_in_bytes);
  }
  return {};
}

auto ScanMetricsCollector::stats() const -> Option<ScanStats> {
  auto lock = std::scoped_lock{mutex_};
  return stats_;
}

auto ScanPlan::next_task()
  -> Result<Option<std::shared_ptr<ice::FileScanTask>>> {
  if (task_stream) {
    // Advancing the stream may open the next batch of manifests on the plan
    // executor.
    auto advanced = [&]() -> decltype(translate(task_stream->Next())) {
      try {
        return translate(task_stream->Next());
      } catch (std::future_error const& e) {
        return std::unexpected{planning_failure(e)};
      }
    }();
    TRY(auto next, std::move(advanced));
    if (not next.has_value()) {
      task_stream.reset();
      return Option<std::shared_ptr<ice::FileScanTask>>{None{}};
    }
    return Option<std::shared_ptr<ice::FileScanTask>>{std::move(*next)};
  }
  if (next_task_index < tasks.size()) {
    return Option<std::shared_ptr<ice::FileScanTask>>{tasks[next_task_index++]};
  }
  return Option<std::shared_ptr<ice::FileScanTask>>{None{}};
}

auto ScanPlan::stats() const -> Option<ScanStats> {
  return metrics ? metrics->stats() : Option<ScanStats>{None{}};
}

struct TaskBatchReader::Impl {
  std::string file_path;
  ArrowReadOptions read_options;
  size_t batch_size = defaults::import::table_slice_size;
  bool metadata_only = false;
  uint64_t remaining_rows = 0;
  uint64_t file_size = 0;
  // Declared before `stream` so the task reader outlives the Arrow C Data
  // Interface stream on destruction.
  std::shared_ptr<ice::FileScanTaskReader> task_reader;
  ArrowStreamGuard stream;
  std::shared_ptr<arrow::Schema> arrow_schema;
  bool eof = false;
};

TaskBatchReader::TaskBatchReader(Box<Impl> impl) : impl_{std::move(impl)} {
}

TaskBatchReader::~TaskBatchReader() = default;
TaskBatchReader::TaskBatchReader(TaskBatchReader&&) noexcept = default;
auto TaskBatchReader::operator=(TaskBatchReader&&) noexcept
  -> TaskBatchReader& = default;

auto TaskBatchReader::open(ScanPlan const& plan, ice::FileScanTask const& task,
                           ArrowReadOptions read_options, size_t batch_size)
  -> Result<TaskBatchReader> {
  ensure_readers_registered();
  if (not task.data_file()) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      "Iceberg scan task has no data file",
    }};
  }
  auto const effective_batch_size
    = batch_size > 0 ? batch_size : defaults::import::table_slice_size;
  auto impl = Box<Impl>{std::in_place};
  impl->file_path = task.data_file()->file_path;
  impl->read_options = std::move(read_options);
  impl->batch_size = effective_batch_size;
  // Zero-column fast path: when no columns are projected, no delete files
  // apply, and the task covers the whole data file (partial-file splits do not
  // carry an exact split-level row count), emit empty records directly from
  // manifest metadata without opening the underlying Parquet/Avro file.
  if (plan.projected_schema and plan.projected_schema->fields().empty()
      and task.delete_files().empty() and not task.is_split()) {
    impl->metadata_only = true;
    auto const count = task.data_file()->record_count;
    impl->remaining_rows = count > 0 ? static_cast<uint64_t>(count) : 0;
    impl->file_size = 0;
    return TaskBatchReader{std::move(impl)};
  }
  auto reader_opts = ice::FileScanTaskReader::Options{
    .io = plan.io,
    .table_schema = plan.table_schema,
    .schemas = plan.schemas,
    .projected_schema = plan.projected_schema,
    .name_mapping = plan.name_mapping,
    .properties = {{"read.batch-size", fmt::to_string(effective_batch_size)}},
  };
  TRY(auto reader,
      translate(ice::FileScanTaskReader::Make(std::move(reader_opts))));
  impl->task_reader = std::move(reader);
  auto opened = [&]() -> Result<ArrowArrayStream> {
    try {
      return translate(impl->task_reader->Open(task));
    } catch (::parquet::ParquetException const& e) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("failed to open Iceberg data file `{}`: {}",
                    impl->file_path, e.what()),
      }};
    }
  }();
  if (not opened) {
    return std::unexpected{std::move(opened).error()};
  }
  impl->stream = ArrowStreamGuard{std::move(*opened)};
  auto const size_bytes = task.size_bytes();
  impl->file_size = size_bytes > 0 ? static_cast<uint64_t>(size_bytes) : 0;
  auto c_schema = ArrowSchemaGuard{};
  auto schema_rc = int{0};
  try {
    schema_rc
      = impl->stream.stream.get_schema(&impl->stream.stream, &c_schema.schema);
  } catch (::parquet::ParquetException const& e) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("failed to read schema from Iceberg data file `{}`: {}",
                  impl->file_path, e.what()),
    }};
  }
  if (schema_rc != 0) {
    return std::unexpected{stream_last_error(
      impl->stream.stream, schema_rc, impl->file_path, "read schema from")};
  }
  auto imported_schema = arrow::ImportSchema(&c_schema.schema);
  if (not imported_schema.ok()) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("failed to import Arrow schema from `{}`: {}",
                  impl->file_path,
                  imported_schema.status().ToStringWithoutContextLines()),
    }};
  }
  impl->arrow_schema = std::move(*imported_schema);
  return TaskBatchReader{std::move(impl)};
}

auto TaskBatchReader::next() -> Result<Option<ArrowReadBatch>> {
  if (impl_->eof) {
    return Option<ArrowReadBatch>{None{}};
  }
  if (impl_->metadata_only) {
    if (impl_->remaining_rows == 0) {
      impl_->eof = true;
      return Option<ArrowReadBatch>{None{}};
    }
    auto const chunk = std::min<uint64_t>(
      impl_->remaining_rows,
      std::min<uint64_t>(impl_->batch_size,
                         static_cast<uint64_t>(
                           std::numeric_limits<nova::storage::Index>::max())));
    impl_->remaining_rows -= chunk;
    auto const rows = detail::narrow<nova::storage::Index>(chunk);
    return Option<ArrowReadBatch>{ArrowReadBatch{
      .events = nova::Events{
        nova::Array<nova::Record>::make_empty(rows),
        nova::storage::BitMap{rows, true},
        nova::Events::Meta::make_empty(rows, impl_->read_options.schema_name),
      },
      .warnings = {},
    }};
  }
  while (true) {
    auto c_array = ArrowArrayGuard{};
    auto rc = int{0};
    try {
      rc = impl_->stream.stream.get_next(&impl_->stream.stream, &c_array.array);
    } catch (::parquet::ParquetException const& e) {
      impl_->eof = true;
      impl_->stream.release();
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("failed to read Iceberg data file `{}`: {}",
                    impl_->file_path, e.what()),
      }};
    }
    if (rc != 0) {
      impl_->eof = true;
      auto err
        = stream_last_error(impl_->stream.stream, rc, impl_->file_path, "read");
      impl_->stream.release();
      return std::unexpected{std::move(err)};
    }
    if (c_array.array.release == nullptr) {
      impl_->eof = true;
      impl_->stream.release();
      return Option<ArrowReadBatch>{None{}};
    }
    auto imported
      = arrow::ImportRecordBatch(&c_array.array, impl_->arrow_schema);
    if (not imported.ok()) {
      impl_->eof = true;
      impl_->stream.release();
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("failed to import Arrow batch from `{}`: {}",
                    impl_->file_path,
                    imported.status().ToStringWithoutContextLines()),
      }};
    }
    auto batch = std::move(*imported);
    if (batch->num_rows() == 0) {
      continue;
    }
    auto converted
      = import_iceberg_batch(std::move(batch), impl_->read_options);
    if (not converted) {
      impl_->eof = true;
      impl_->stream.release();
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("failed to convert batch from `{}`: {}", impl_->file_path,
                    converted.unwrap_err()),
      }};
    }
    return Option<ArrowReadBatch>{std::move(converted).unwrap()};
  }
}

auto TaskBatchReader::used_metadata_only_fast_path() const -> bool {
  return impl_->metadata_only;
}

auto TaskBatchReader::file_size_bytes() const -> uint64_t {
  return impl_->file_size;
}

auto refresh_table(ice::Table& table) -> Result<void> {
  return translate(table.Refresh());
}

auto is_tag_ref(ice::Table const& table, std::string_view ref) -> Result<bool> {
  if (ref == ice::SnapshotRef::kMainBranch) {
    return false;
  }
  auto const& metadata = table.metadata();
  if (not metadata) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      "table metadata is null",
    }};
  }
  auto const it = metadata->refs.find(std::string{ref});
  if (it == metadata->refs.end() or not it->second) {
    return std::unexpected{Error{
      Error::Kind::not_found,
      fmt::format("reference `{}` not found in table `{}`", ref,
                  table.full_name()),
    }};
  }
  return it->second->type() == ice::SnapshotRefType::kTag;
}

auto resolve_current_snapshot(ice::Table const& table,
                              Option<std::string_view> ref)
  -> Result<Option<int64_t>> {
  auto const& metadata = table.metadata();
  if (not metadata) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      "table metadata is null",
    }};
  }
  if (not ref or *ref == ice::SnapshotRef::kMainBranch) {
    if (metadata->current_snapshot_id == ice::kInvalidSnapshotId) {
      return Option<int64_t>{None{}};
    }
    return Option<int64_t>{metadata->current_snapshot_id};
  }
  auto const it = metadata->refs.find(std::string{*ref});
  if (it == metadata->refs.end() or not it->second) {
    return std::unexpected{Error{
      Error::Kind::not_found,
      fmt::format("reference `{}` not found in table `{}`", *ref,
                  table.full_name()),
    }};
  }
  if (it->second->snapshot_id == ice::kInvalidSnapshotId) {
    return Option<int64_t>{None{}};
  }
  return Option<int64_t>{it->second->snapshot_id};
}

auto inspect_incremental_window(ice::Table const& table,
                                int64_t from_snapshot_id_exclusive,
                                int64_t to_snapshot_id_inclusive)
  -> Result<IncrementalWindowSummary> {
  auto const& metadata = table.metadata();
  if (not metadata) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      "table metadata is null",
    }};
  }
  // Ensure the end snapshot exists first. This is the only not-found error of
  // this function, allowing callers to attribute it to `to_snapshot_id`.
  if (auto const end = metadata->SnapshotById(to_snapshot_id_inclusive);
      not end.has_value()) {
    return std::unexpected{Error{
      Error::Kind::not_found,
      end.error().message,
    }};
  }
  if (from_snapshot_id_exclusive == to_snapshot_id_inclusive) {
    return IncrementalWindowSummary{};
  }
  // Verify that `from_snapshot_id_exclusive` is a strict ancestor of
  // `to_snapshot_id_inclusive`. `IsParentAncestorOf` succeeds even if `from`
  // itself was just expired while its child still references it, and returns
  // false if the chain broke earlier or the branch was rewritten.
  TRY(auto is_ancestor,
      translate(ice::SnapshotUtil::IsParentAncestorOf(
        *metadata, to_snapshot_id_inclusive, from_snapshot_id_exclusive)));
  if (not is_ancestor) {
    // Distinguish a truncated parent chain (expired snapshot or partial
    // catalog response) from a complete chain reaching the root snapshot
    // (rewritten branch or snapshot from another branch).
    TRY(auto ancestors, translate(ice::SnapshotUtil::AncestorsOf(
                          *metadata, to_snapshot_id_inclusive)));
    auto const* oldest = ancestors.empty() ? nullptr : ancestors.back().get();
    if (oldest != nullptr and oldest->parent_snapshot_id.has_value()) {
      return std::unexpected{Error{
        Error::Kind::permanent,
        fmt::format("cannot verify that snapshot {} is an ancestor of snapshot "
                    "{}: the history in the table metadata ends at snapshot "
                    "{}, whose parent {} is missing (the snapshot may have "
                    "expired, or the catalog returned only a part of the "
                    "snapshots)",
                    from_snapshot_id_exclusive, to_snapshot_id_inclusive,
                    oldest->snapshot_id, *oldest->parent_snapshot_id),
      }};
    }
    return std::unexpected{Error{
      Error::Kind::permanent,
      fmt::format("snapshot {} is not an ancestor of snapshot {} (the branch "
                  "may have been rewritten, or the snapshot belongs to "
                  "another branch)",
                  from_snapshot_id_exclusive, to_snapshot_id_inclusive),
    }};
  }
  TRY(auto snapshots,
      translate(ice::SnapshotUtil::AncestorsBetween(
        *metadata, to_snapshot_id_inclusive, from_snapshot_id_exclusive)));
  auto summary = IncrementalWindowSummary{};
  for (auto const& snap : snapshots) {
    if (not snap) {
      continue;
    }
    auto const op = snap->Operation();
    if (not op) {
      summary.other_snapshots += 1;
    } else if (*op == ice::DataOperation::kAppend) {
      summary.append_snapshots += 1;
    } else if (*op == ice::DataOperation::kReplace) {
      summary.replace_snapshots += 1;
    } else if (*op == ice::DataOperation::kOverwrite) {
      summary.overwrite_snapshots += 1;
    } else if (*op == ice::DataOperation::kDelete) {
      summary.delete_snapshots += 1;
    } else {
      summary.other_snapshots += 1;
    }
  }
  return summary;
}

auto plan_snapshot_scan(ice::Table const& table, ScanTarget const& target,
                        ScanOptions const& options) -> Result<ScanPlan> {
  ensure_readers_registered();
  auto const& metadata = table.metadata();
  if (not metadata) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      "table metadata is null",
    }};
  }
  auto resolved_snapshot_id = Option<int64_t>{None{}};
  auto use_snapshot_schema = false;
  auto use_explicit_snapshot = false;
  if (target.snapshot_id) {
    TRY(std::ignore, translate(metadata->SnapshotById(*target.snapshot_id)));
    resolved_snapshot_id = *target.snapshot_id;
    use_snapshot_schema = true;
    use_explicit_snapshot = true;
  } else if (target.as_of_ms) {
    auto const branch = target.ref ? std::string_view{*target.ref}
                                   : ice::SnapshotRef::kMainBranch;
    TRY(auto snap_id,
        resolve_as_of_on_branch(*metadata, branch, *target.as_of_ms));
    resolved_snapshot_id = snap_id;
    use_snapshot_schema = true;
    use_explicit_snapshot = true;
  } else if (target.ref and *target.ref != ice::SnapshotRef::kMainBranch) {
    auto const it = metadata->refs.find(*target.ref);
    if (it == metadata->refs.end() or not it->second) {
      return std::unexpected{Error{
        Error::Kind::not_found,
        fmt::format("reference `{}` not found in table `{}`", *target.ref,
                    table.full_name()),
      }};
    }
    TRY(std::ignore,
        translate(metadata->SnapshotById(it->second->snapshot_id)));
    resolved_snapshot_id = it->second->snapshot_id;
    use_snapshot_schema = it->second->type() == ice::SnapshotRefType::kTag;
  } else {
    if (metadata->current_snapshot_id == ice::kInvalidSnapshotId) {
      // Empty table with 0 snapshots: return an empty plan without sending a
      // REST `/plan` request. It still uses the selected schema, so that a
      // schema that is gone fails the resume.
      TRY(auto current, translate(metadata->Schema()));
      TRY(auto schema, select_scan_schema(*metadata, current, options));
      return ScanPlan{
        .snapshot_id = None{},
        .table_schema = schema,
        .schemas = metadata->schemas,
        .projected_schema = schema,
        .name_mapping = nullptr,
        .io = table.io(),
        .task_stream = nullptr,
        .tasks = {},
        .next_task_index = 0,
        .counted_from_metadata = false,
        .metrics = std::make_shared<ScanMetricsCollector>(),
      };
    }
    resolved_snapshot_id = metadata->current_snapshot_id;
    use_snapshot_schema = false;
  }
  TRY(auto name_mapping, parse_default_name_mapping(table));
  // `base_schema` is the schema that the scan builder selects on its own.
  TRY(auto base_schema, resolve_schema_for_scan(*metadata, resolved_snapshot_id,
                                                use_snapshot_schema));
  TRY(auto snapshot_schema,
      select_scan_schema(*metadata, base_schema, options));
  TRY(auto builder, translate(table.NewScan()));
  auto metrics = std::make_shared<ScanMetricsCollector>();
  builder->ReportWith(metrics);
  builder->IgnoreResiduals();
  // Open manifests concurrently. `PlanFilesStream()` still yields files in
  // manifest-list order, preserving deterministic checkpoint resume.
  builder->PlanWith(PlanExecutor::get());
  if (use_explicit_snapshot) {
    builder->UseSnapshot(*resolved_snapshot_id);
  } else if (target.ref) {
    builder->UseRef(*target.ref);
  }
  // Do not forward `MinRowsRequested`: Iceberg counts raw manifest row counts
  // before row-level position/equality deletes are applied, so a REST `/plan`
  // server could truncate tasks before enough live rows exist. `FromIceberg`
  // enforces `opt::Limit` locally and stops pulling from lazy
  // `PlanFilesStream()` as soon as the limit is reached.
  TRY(push_scan_options(*builder, options, snapshot_schema, *base_schema));
  TRY(auto scan, translate(builder->Build()));
  TRY(auto projected_schema, translate(scan->schema()));
  auto const server_planned
    = dynamic_cast<ice::rest::RestTableScan const*>(scan.get()) != nullptr;
  // Without a filter and without projected columns, answer the row count from
  // table metadata when permitted and proven, emitting one synthetic task
  // named after the manifest list so `TaskBatchReader` streams empty rows
  // without reading any manifest or data file.
  if (options.count_from_metadata and not server_planned
      and options.filter.empty() and projected_schema->fields().empty()) {
    TRY(auto snapshot,
        translate(metadata->SnapshotById(*resolved_snapshot_id)));
    if (auto const rows = row_count_from_metadata(*snapshot, table.io())) {
      TENZIR_DEBUG("from_iceberg: counted {} rows of snapshot {} in table `{}` "
                   "from table metadata",
                   *rows, *resolved_snapshot_id, table.full_name());
      auto counted = std::vector<std::shared_ptr<ice::FileScanTask>>{};
      if (*rows > 0) {
        auto file = std::make_shared<ice::DataFile>();
        file->file_path = snapshot->manifest_list;
        file->record_count = *rows;
        counted.push_back(std::make_shared<ice::FileScanTask>(std::move(file)));
      }
      return ScanPlan{
        .snapshot_id = resolved_snapshot_id,
        .table_schema = std::move(snapshot_schema),
        .schemas = metadata->schemas,
        .projected_schema = std::move(projected_schema),
        .name_mapping = std::move(name_mapping),
        .io = scan->io(),
        .task_stream = nullptr,
        .tasks = std::move(counted),
        .next_task_index = 0,
        .counted_from_metadata = true,
        .metrics = std::move(metrics),
      };
    }
  }
  auto task_stream = ice::FileScanTaskStreamPtr{};
  auto tasks = std::vector<std::shared_ptr<ice::FileScanTask>>{};
  // Server-side REST `/plan` does not guarantee deterministic task order
  // across calls, so materialize and sort its tasks by `(file_path, start)`
  // for reproducible checkpoint resume. Local client-side scans stay lazy via
  // `PlanFilesStream()` (manifest-list order is deterministic even with
  // parallel manifest reading, allowing `head` to short-circuit manifests).
  try {
    if (server_planned) {
      TRY(tasks, translate(scan->PlanFiles()));
      sort_tasks(tasks);
    } else {
      TRY(task_stream, translate(scan->PlanFilesStream()));
    }
  } catch (std::future_error const& e) {
    return std::unexpected{planning_failure(e)};
  }
  // Capture FileIO AFTER planning so scan-scoped vended credentials from
  // RestTableScan are honored.
  auto io = scan->io();
  return ScanPlan{
    .snapshot_id = resolved_snapshot_id,
    .table_schema = std::move(snapshot_schema),
    .schemas = metadata->schemas,
    .projected_schema = std::move(projected_schema),
    .name_mapping = std::move(name_mapping),
    .io = std::move(io),
    .task_stream = std::move(task_stream),
    .tasks = std::move(tasks),
    .next_task_index = 0,
    .counted_from_metadata = false,
    .metrics = std::move(metrics),
  };
}

auto plan_incremental_append_scan(ice::Table const& table,
                                  IncrementalRange const& range,
                                  ScanOptions const& options)
  -> Result<ScanPlan> {
  ensure_readers_registered();
  auto const& metadata = table.metadata();
  if (not metadata) {
    return std::unexpected{Error{
      Error::Kind::permanent,
      "table metadata is null",
    }};
  }
  // The builder of an incremental scan always binds against the current table
  // schema. Select the plan's schema before the early returns below, so that
  // they use it too and a schema that is gone fails the resume.
  TRY(auto current_schema, translate(metadata->Schema()));
  TRY(auto table_schema,
      select_scan_schema(*metadata, current_schema, options));
  auto resolved_to = range.to_snapshot_id;
  if (not resolved_to) {
    auto ref = range.branch
                 ? Option<std::string_view>{std::string_view{*range.branch}}
                 : Option<std::string_view>{None{}};
    TRY(resolved_to, resolve_current_snapshot(table, ref));
  }
  if (not resolved_to) {
    if (range.from_snapshot_id) {
      return std::unexpected{Error{
        Error::Kind::not_found,
        fmt::format("snapshot {} not found: table `{}` has no snapshots",
                    *range.from_snapshot_id, table.full_name()),
      }};
    }
    return ScanPlan{
      .snapshot_id = None{},
      .table_schema = table_schema,
      .schemas = metadata->schemas,
      .projected_schema = table_schema,
      .name_mapping = nullptr,
      .io = table.io(),
      .task_stream = nullptr,
      .tasks = {},
      .next_task_index = 0,
      .counted_from_metadata = false,
      .metrics = std::make_shared<ScanMetricsCollector>(),
    };
  }
  if (range.from_snapshot_id and *range.from_snapshot_id == *resolved_to) {
    TRY(std::ignore, translate(metadata->SnapshotById(*resolved_to)));
    return ScanPlan{
      .snapshot_id = resolved_to,
      .table_schema = table_schema,
      .schemas = metadata->schemas,
      .projected_schema = table_schema,
      .name_mapping = nullptr,
      .io = table.io(),
      .task_stream = nullptr,
      .tasks = {},
      .next_task_index = 0,
      .counted_from_metadata = false,
      .metrics = std::make_shared<ScanMetricsCollector>(),
    };
  }
  TRY(auto name_mapping, parse_default_name_mapping(table));
  TRY(auto builder, translate(table.NewIncrementalAppendScan()));
  auto metrics = std::make_shared<ScanMetricsCollector>();
  builder->ReportWith(metrics);
  builder->IgnoreResiduals();
  // Read the manifest lists of the snapshots in the range and their manifests
  // concurrently; the tasks are sorted deterministically below.
  builder->PlanWith(PlanExecutor::get());
  if (range.branch and not range.branch->empty()
      and *range.branch != ice::SnapshotRef::kMainBranch) {
    builder->UseBranch(*range.branch);
  }
  if (range.from_snapshot_id) {
    builder->FromSnapshot(*range.from_snapshot_id, /*inclusive=*/false);
  }
  if (range.to_snapshot_id) {
    builder->ToSnapshot(*range.to_snapshot_id);
  }
  // Do not forward `MinRowsRequested`; see `plan_snapshot_scan`.
  TRY(push_scan_options(*builder, options, table_schema, *current_schema));
  TRY(auto scan, translate(builder->Build()));
  TRY(auto projected_schema, translate(scan->schema()));
  auto planned
    = [&]() -> Result<std::vector<std::shared_ptr<ice::FileScanTask>>> {
    try {
      return translate(scan->PlanFiles());
    } catch (std::future_error const& e) {
      return std::unexpected{planning_failure(e)};
    }
  }();
  TRY(auto tasks, std::move(planned));
  // Capture FileIO AFTER PlanFiles() so scan-scoped vended credentials from
  // RestIncrementalAppendScan are honored.
  auto io = scan->io();
  // IncrementalAppendScan collects manifests in an unordered_set; sort tasks
  // deterministically so checkpoint resume by task index is reproducible.
  sort_tasks(tasks);
  return ScanPlan{
    .snapshot_id = resolved_to,
    .table_schema = std::move(table_schema),
    .schemas = metadata->schemas,
    .projected_schema = std::move(projected_schema),
    .name_mapping = std::move(name_mapping),
    .io = std::move(io),
    .task_stream = nullptr,
    .tasks = std::move(tasks),
    .next_task_index = 0,
    .counted_from_metadata = false,
    .metrics = std::move(metrics),
  };
}

} // namespace tenzir::plugins::iceberg
