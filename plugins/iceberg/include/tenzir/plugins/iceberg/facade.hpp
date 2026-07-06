//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The facade is the only place that talks to iceberg-cpp. The library is
// pre-1.0 and moves quickly; isolating it behind this interface keeps API
// churn out of the operator implementation. No iceberg-cpp type appears in
// this header.

#pragma once

#include <tenzir/type.hpp>

#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

struct ArrowArray;
struct ArrowSchema;

namespace tenzir::plugins::iceberg {

/// Error surfaced from the catalog, file IO, or commit path. The kind
/// distinguishes how the operator should react per the retry policy.
struct Error {
  enum class Kind {
    /// Retry with backoff (network errors, 5xx, throttling).
    transient,
    /// Concurrent table update; handled by iceberg-cpp's rebase-retry, only
    /// surfaced when that gives up.
    conflict,
    /// The referenced table or namespace does not exist.
    not_found,
    /// The table or namespace to create already exists.
    already_exists,
    /// Do not retry (auth rejected, invalid schema).
    permanent,
  };

  Kind kind = Kind::permanent;
  std::string message;
};

template <class T>
using Result = std::expected<T, Error>;

/// A partition transform per the Iceberg spec. Transform computation stays
/// inside the facade (iceberg-cpp implements the spec's semantics, notably
/// bucket's Murmur3 hash); this enum only describes a spec symbolically.
enum class PartitionTransform {
  identity,
  year,
  month,
  day,
  hour,
  bucket,
  truncate,
};

/// One field of a partition spec: a transform over a source column.
struct PartitionField {
  /// Dotted path of the source column in the table schema.
  std::string source;
  PartitionTransform transform = PartitionTransform::identity;
  /// Bucket count or truncate width; unused for the other transforms.
  std::optional<int64_t> parameter;
};

/// Options for creating a new table.
struct CreateTableOptions {
  /// Partition spec fields, in order. Empty for an unpartitioned table.
  /// Source columns must exist in the created schema and support the
  /// transform (e.g. temporal transforms need a timestamp source).
  std::vector<PartitionField> partition_by;

  /// Name of a top-level column to register as the table's default sort
  /// order (identity transform, ascending, nulls first). Ignored unless the
  /// column exists and derives to a timestamp. Data files written by this
  /// plugin do not claim the sort order until in-file sorting lands; the
  /// registration informs other writers and compaction jobs.
  std::optional<std::string> sort_column;
};

struct CatalogConfig {
  /// REST catalog endpoint, e.g. "http://localhost:8181".
  std::string uri;
  /// Warehouse identifier passed to the catalog.
  std::string warehouse;
  /// Client-side catalog name; shows up in metrics and error messages only.
  std::string name = "tenzir";
  /// Additional catalog and FileIO properties (auth, "s3.endpoint", ...).
  std::unordered_map<std::string, std::string> properties;
};

class Table;

class Catalog {
public:
  /// Connects to a REST catalog. Fails fast on unreachable endpoints or
  /// rejected credentials.
  static auto open(CatalogConfig config) -> Result<Catalog>;

  /// Creates a namespace if it does not exist yet.
  auto ensure_namespace(std::span<const std::string> ns) -> Result<void>;

  auto load_table(std::span<const std::string> ns, std::string_view name)
    -> Result<Table>;

  /// Creates a table whose schema derives from a Tenzir record type. Records
  /// map to nested structs; ip, subnet, and enumeration columns map to
  /// strings; timestamps map to microsecond timestamptz; durations and
  /// unsigned integers map to long. Fields that cannot be represented are
  /// skipped and reported in `dropped_fields` as `path: reason` strings.
  auto
  create_table(std::span<const std::string> ns, std::string_view name,
               const record_type& schema, const CreateTableOptions& options,
               std::vector<std::string>& dropped_fields) -> Result<Table>;

private:
  struct Impl;
  explicit Catalog(std::shared_ptr<Impl> impl);
  // Not Arc<T>: its converting constructor's constraints require a complete
  // type, which defeats the pimpl that keeps iceberg-cpp out of this header.
  std::shared_ptr<Impl> impl_;
};

/// Opaque handle to the partition tuple shared by a group of rows.
class PartitionTuple {
public:
  /// The empty tuple, for writing into unpartitioned tables.
  PartitionTuple();

  friend class Table;

private:
  struct Impl;
  explicit PartitionTuple(std::shared_ptr<Impl> impl);
  // Not Arc<T>: its converting constructor's constraints require a complete
  // type, which defeats the pimpl that keeps iceberg-cpp out of this header.
  std::shared_ptr<Impl> impl_;
};

/// Rows of one batch that share a partition tuple.
struct PartitionGroup {
  /// An opaque key that uniquely identifies the partition tuple, stable
  /// across batches. Unlike `path`, distinct tuples never collide (the
  /// human-readable rendering maps e.g. a null and the string "null" to the
  /// same text).
  std::string key;
  /// The human-readable partition path, e.g. `class_uid=1001/time_day=
  /// 2026-07-06`; empty for unpartitioned tables.
  std::string path;
  /// Row indices of the batch that belong to this partition. An empty vector
  /// means the group covers every row of the batch.
  std::vector<int64_t> rows;
  /// The partition tuple to open a file writer with.
  PartitionTuple partition;
};

/// Opaque handle to a written-but-uncommitted data file.
class DataFile {
public:
  friend class FileWriter;
  friend class Table;

private:
  struct Impl;
  explicit DataFile(std::shared_ptr<Impl> impl);
  // Not Arc<T>: its converting constructor's constraints require a complete
  // type, which defeats the pimpl that keeps iceberg-cpp out of this header.
  std::shared_ptr<Impl> impl_;
};

/// Writes one Parquet data file into the table's data location.
class FileWriter {
public:
  friend class Table;

  /// Appends a batch. The batch must match the table schema; ownership of the
  /// Arrow array is transferred.
  auto write(ArrowArray* batch) -> Result<void>;

  /// Number of bytes already written to the open file.
  auto bytes_written() -> Result<int64_t>;

  /// Flushes and closes the file, returning the handle to commit.
  auto finish() -> Result<DataFile>;

private:
  struct Impl;
  explicit FileWriter(std::shared_ptr<Impl> impl);
  // Not Arc<T>: its converting constructor's constraints require a complete
  // type, which defeats the pimpl that keeps iceberg-cpp out of this header.
  std::shared_ptr<Impl> impl_;
};

class Table {
public:
  friend class Catalog;

  /// The table's base location as reported by the catalog.
  auto location() const -> std::string;

  /// Exports the table's schema as an Arrow schema; the caller assumes
  /// ownership of `out`. Writers must be fed arrays matching this schema
  /// exactly, including nested structs and lists. Fails when the table
  /// schema uses types the plugin cannot map (e.g. uuid, map, decimal).
  auto export_arrow_schema(ArrowSchema* out) const -> Result<void>;

  /// Adds columns for fields of `schema` that the table does not have yet,
  /// recursing into nested records and lists of records (a metadata-only
  /// schema-update commit). Existing columns are never modified; fields whose
  /// type conflicts with an existing column stay untouched, and fields that
  /// cannot be represented are skipped and reported in `dropped_fields` as
  /// `path: reason` strings. Returns the updated table, or `std::nullopt`
  /// when the table already covers every representable field. An error of
  /// kind `conflict` means a concurrent writer updated the table; callers
  /// should reload the table and retry against the fresh schema.
  auto evolve_schema(const record_type& schema,
                     std::vector<std::string>& dropped_fields)
    -> Result<std::optional<Table>>;

  /// Checks that this operator can compute the table's partition spec:
  /// every source column resolves and its transform is supported. Fails with
  /// a permanent error otherwise (e.g. an unknown future transform).
  auto validate_partitioning() -> Result<void>;

  /// Checks that the table's partition spec matches `fields` exactly
  /// (source, transform, and parameter, in order). Fails with a permanent
  /// error describing both specs otherwise.
  auto check_partition_spec(std::span<const PartitionField> fields)
    -> Result<void>;

  /// Splits a batch matching the table schema into per-partition row groups
  /// by evaluating the partition spec's transforms. Returns exactly one
  /// group, covering all rows, for unpartitioned tables. Ownership of the
  /// Arrow array is transferred.
  auto split_by_partition(ArrowArray* batch)
    -> Result<std::vector<PartitionGroup>>;

  /// Opens a writer for a new data file of the given partition in the
  /// table's data location.
  auto new_file_writer(const PartitionTuple& partition) -> Result<FileWriter>;

  /// Commits the given data files as one new snapshot (FastAppend) and
  /// returns the refreshed table. Callers must route subsequent writers and
  /// commits through the returned handle; a stale handle loses the race
  /// against its own previous snapshot. The commit is verified to have taken
  /// effect: an error of kind `conflict` means a concurrent update won the
  /// race and the snapshot did not land; callers should reload the table and
  /// retry with the same files.
  auto commit_append(std::span<DataFile> files) -> Result<Table>;

private:
  struct Impl;
  explicit Table(std::shared_ptr<Impl> impl);
  // Not Arc<T>: its converting constructor's constraints require a complete
  // type, which defeats the pimpl that keeps iceberg-cpp out of this header.
  std::shared_ptr<Impl> impl_;
};

} // namespace tenzir::plugins::iceberg
