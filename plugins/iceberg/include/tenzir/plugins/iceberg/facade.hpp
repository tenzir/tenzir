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

#include <expected>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>

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
    /// Do not retry (auth rejected, missing table, invalid schema).
    permanent,
  };

  Kind kind = Kind::permanent;
  std::string message;
};

template <class T>
using Result = std::expected<T, Error>;

/// Minimal column model for Phase 0 table creation. Phase 2 replaces this
/// with the full Tenzir-type → Iceberg-schema derivation.
struct ColumnSpec {
  enum class Kind {
    boolean,
    int64,
    double_,
    string,
    /// Microsecond precision, UTC ("timestamptz" in Iceberg terms).
    timestamp,
  };

  std::string name;
  Kind kind = Kind::string;
  bool required = false;
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

  auto create_table(std::span<const std::string> ns, std::string_view name,
                    std::span<const ColumnSpec> columns) -> Result<Table>;

private:
  struct Impl;
  explicit Catalog(std::shared_ptr<Impl> impl);
  // Not Arc<T>: its converting constructor's constraints require a complete
  // type, which defeats the pimpl that keeps iceberg-cpp out of this header.
  std::shared_ptr<Impl> impl_;
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

  /// Opens a writer for a new data file in the table's data location.
  auto new_file_writer() -> Result<FileWriter>;

  /// Commits the given data files as one new snapshot (FastAppend).
  auto commit_append(std::span<DataFile> files) -> Result<void>;

private:
  struct Impl;
  explicit Table(std::shared_ptr<Impl> impl);
  // Not Arc<T>: its converting constructor's constraints require a complete
  // type, which defeats the pimpl that keeps iceberg-cpp out of this header.
  std::shared_ptr<Impl> impl_;
};

} // namespace tenzir::plugins::iceberg
