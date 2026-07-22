//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Helpers around iceberg-cpp for the to_iceberg operator. The operator holds
// library handles (`ice::Table`, `ice::DataWriter`, ...) directly and calls
// the library itself for plain accessors; the functions here cover the parts
// that need Tenzir-side policy or work around library behavior:
// - error translation into the operator's retry taxonomy,
// - schema derivation and evolution from Tenzir types,
// - partition binding, per-row value computation, and batch grouping,
// - commit tagging plus post-commit verification for exactly-once writes,
// - auth managers and FileIO factories the library lacks (SigV4 through a
//   live credentials provider, native GCS).

#pragma once

#include "tenzir/plugins/iceberg/types.hpp"

#include <tenzir/type.hpp>

#include <iceberg/result.h>
#include <iceberg/row/partition_values.h>
#include <iceberg/type_fwd.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace iceberg {
class DataWriter;
} // namespace iceberg

namespace arrow {
class Schema;
class StructArray;
} // namespace arrow

namespace tenzir::plugins::iceberg {

// Our namespace ends in `iceberg`, so the library must be qualified from the
// global namespace throughout the plugin.
namespace ice = ::iceberg;

/// Maps an iceberg-cpp error onto the operator's retry taxonomy.
auto translate_error(ice::Error const& error) -> Error;

/// Unwraps an iceberg-cpp result, translating the error on failure.
template <class T>
auto translate(ice::Result<T> result) -> Result<T> {
  if (not result.has_value()) {
    return std::unexpected{translate_error(result.error())};
  }
  if constexpr (std::same_as<T, void>) {
    return {};
  } else {
    return std::move(result).value();
  }
}

/// Initializes the AWS SDK lifecycle used by Iceberg REST SigV4 sessions.
/// Call before constructing credentials providers that may resolve eagerly.
[[nodiscard]] auto ensure_aws_sdk_initialized() -> Result<void>;

/// Connects to a REST catalog. Fails fast on unreachable endpoints or
/// rejected credentials. The returned pointer keeps the registered AWS
/// credentials provider alive until the last catalog handle drops.
auto open_catalog(CatalogConfig config)
  -> Result<std::shared_ptr<ice::Catalog>>;

/// Creates a namespace if it does not exist yet.
[[nodiscard]] auto
ensure_namespace(ice::Catalog& catalog, std::span<std::string const> ns)
  -> Result<void>;

auto load_table(ice::Catalog& catalog, std::span<std::string const> ns,
                std::string_view name) -> Result<std::shared_ptr<ice::Table>>;

/// Creates a table whose schema derives from a Tenzir record type. Records
/// map to nested structs; ip, subnet, and enumeration columns map to
/// strings; timestamps map to microsecond timestamptz; durations and
/// unsigned integers map to long. Fields that cannot be represented are
/// skipped and reported in `dropped_fields` as `path: reason` strings.
auto create_table(ice::Catalog& catalog, std::span<std::string const> ns,
                  std::string_view name, record_type const& schema,
                  CreateTableOptions const& options,
                  std::vector<std::string>& dropped_fields)
  -> Result<std::shared_ptr<ice::Table>>;

/// Maps the table's schema to an Arrow schema. Writers must be fed arrays
/// matching this schema exactly, including nested structs and lists. Fails
/// when the table schema uses types the plugin cannot map (e.g. uuid, map,
/// decimal).
auto table_arrow_schema(ice::Table const& table)
  -> Result<std::shared_ptr<arrow::Schema>>;

/// Whether both tables use the same current schema and default partition
/// spec. Open file writers remain valid across metadata refreshes only when
/// this holds.
auto same_write_layout(ice::Table const& lhs, ice::Table const& rhs) -> bool;

/// Adds columns for fields of `schema` that the table does not have yet,
/// recursing into nested records and lists of records (a metadata-only
/// schema-update commit). Existing columns whose type must widen to hold
/// the incoming values are promoted where the spec allows it (int to long,
/// float to double); other type conflicts stay untouched, and fields that
/// cannot be represented are skipped and reported in `dropped_fields` as
/// `path: reason` strings. Returns the updated table, or `std::nullopt`
/// when the table already covers every representable field. An error of
/// kind `conflict` means a concurrent writer updated the table; callers
/// should reload the table and retry against the fresh schema.
auto evolve_schema(std::shared_ptr<ice::Table> const& table,
                   record_type const& schema,
                   std::vector<std::string>& dropped_fields)
  -> Result<std::optional<std::shared_ptr<ice::Table>>>;

/// One partition spec field bound against the table schema, ready to compute
/// partition values.
struct BoundPartitionField {
  /// Path of the source column, one entry per struct level.
  std::vector<std::string> segments;
  std::shared_ptr<ice::PrimitiveType> source_type;
  std::shared_ptr<ice::Transform> transform;
  std::shared_ptr<ice::TransformFunction> function;
};

/// Binds the table's default partition spec against its schema: every source
/// column must resolve and its transform must be supported (an unknown
/// future transform is a permanent error). Returns one entry per partition
/// field; empty for unpartitioned tables. The result is only valid for this
/// table's current schema and default spec; rebind after adopting a table
/// with a different write layout.
auto bind_partitioning(ice::Table const& table)
  -> Result<std::vector<BoundPartitionField>>;

/// Checks that the table's partition spec matches `fields` exactly
/// (source, transform, and parameter, in order). Fails with a permanent
/// error describing both specs otherwise.
[[nodiscard]] auto check_partition_spec(ice::Table const& table,
                                        std::span<PartitionField const> fields)
  -> Result<void>;

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
  ice::PartitionValues partition;
};

/// Splits a batch matching the table schema into per-partition row groups
/// by evaluating the bound partition fields. Returns exactly one group,
/// covering all rows, for unpartitioned tables. The batch's Arrow type must
/// be the one `table_arrow_schema` derives for the table schema.
auto split_by_partition(ice::Table const& table,
                        std::span<BoundPartitionField const> bound,
                        std::shared_ptr<arrow::StructArray> batch)
  -> Result<std::vector<PartitionGroup>>;

/// Opens a writer for a new data file of the given partition in the
/// table's data location. `omit` flags top-level columns (by position in
/// the table schema) that hold only nulls in every batch the caller will
/// write; the data file leaves them out entirely, and readers restore
/// them as nulls through the same field-id projection that serves files
/// written before a schema evolution. Columns that must stay in the file
/// (required fields, partition-spec sources) are cleared from `omit`, so
/// on return it flags exactly the columns the caller must drop from
/// every batch fed to the writer. An empty `omit` writes the full
/// schema.
auto new_file_writer(ice::Table const& table,
                     ice::PartitionValues const& partition,
                     std::vector<bool>& omit)
  -> Result<std::shared_ptr<ice::DataWriter>>;

/// Flushes and closes the file, returning the handle to commit.
auto finish_data_file(ice::DataWriter& writer)
  -> Result<std::shared_ptr<ice::DataFile>>;

/// Converts a data file handle into its checkpoint-persistable form. Fails
/// for files this plugin's writers cannot have produced.
auto serialize_data_file(ice::DataFile const& file)
  -> Result<SerializedDataFile>;

/// Restores a data file handle from its checkpoint-persistable form.
auto deserialize_data_file(SerializedDataFile const& serialized)
  -> Result<std::shared_ptr<ice::DataFile>>;

/// Commits the given data files as one new snapshot (FastAppend) tagged
/// with the commit tag, and returns the refreshed table. Callers must
/// route subsequent writers and commits through the returned handle; a
/// stale handle loses the race against its own previous snapshot. The
/// commit is verified to have taken effect: an error of kind `conflict`
/// means a concurrent update won the race and the snapshot did not land;
/// callers should reload the table, check `has_commit` (the retried commit
/// may have landed after all), and otherwise retry with the same files.
auto commit_append(std::shared_ptr<ice::Table> table,
                   std::span<std::shared_ptr<ice::DataFile> const> files,
                   CommitTag const& tag) -> Result<std::shared_ptr<ice::Table>>;

/// Whether the table has a snapshot carrying the given commit tag. Reads
/// the handle's metadata; load or reload the table first for a current
/// answer.
auto has_commit(ice::Table const& table, CommitTag const& tag) -> bool;

/// Whether the table's current snapshot references any of the given
/// data-file paths. Restart reconciliation falls back to this when
/// snapshot expiration has erased the tagged snapshot that proves a
/// commit: paths carry per-file UUIDs and files commit atomically, so one
/// live file proves the whole commit landed.
auto references_any_data_file(ice::Table const& table,
                              std::span<std::string const> paths)
  -> Result<bool>;

} // namespace tenzir::plugins::iceberg
