//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Plain data types shared between the operator and the iceberg-cpp helpers.
// Everything in this header is either persisted in checkpoints or drives the
// operator's retry policy, so none of it may depend on iceberg-cpp: the
// library is pre-1.0 and its enumerator values are unpinned, while these
// values must survive restarts, including upgrades.

#pragma once

#include <tenzir/option.hpp>

#include <cstdint>
#include <expected>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace Aws::Auth {
class AWSCredentialsProvider;
} // namespace Aws::Auth

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
  /// Whether the failed operation may nevertheless have taken effect, such
  /// as a commit whose success response was lost.
  bool uncertain = false;
};

template <class T>
using Result = std::expected<T, Error>;

/// A partition transform per the Iceberg spec. Transform computation stays
/// with iceberg-cpp (it implements the spec's semantics, notably bucket's
/// Murmur3 hash); this enum only describes a spec symbolically.
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
  Option<int64_t> parameter;
};

/// Options for creating a new table.
struct CreateTableOptions {
  /// Explicit base location for the new table. Empty lets the catalog choose.
  std::string location;

  /// Partition spec fields, in order. Empty for an unpartitioned table.
  /// Source columns must exist in the created schema and support the
  /// transform (e.g. temporal transforms need a timestamp source).
  std::vector<PartitionField> partition_by;

  /// Name of a top-level column to register as the table's default sort
  /// order (identity transform, ascending, nulls first). Ignored unless the
  /// column exists and derives to a timestamp. Data files written by this
  /// plugin do not claim the sort order until in-file sorting lands; the
  /// registration informs other writers and compaction jobs.
  Option<std::string> sort_column;
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
  /// Select S3 FileIO even when AWS credentials do not materialize as
  /// `s3.*` properties, for example for profiles and assumed roles.
  bool use_s3_file_io = false;
  /// AWS SigV4 service name for catalog requests, for example `glue` or
  /// `s3tables`. Empty disables AWS catalog authentication.
  std::string aws_catalog_signing_name;
  /// AWS region used to sign catalog requests.
  std::string aws_signing_region;
  /// Signs AWS catalog and S3 requests through the same live credentials
  /// provider. Profiles, workload identities, and STS-backed IAM modes can
  /// refresh credentials without reopening the catalog or pipeline.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> aws_credentials_provider;
  /// Authenticate catalog requests with Google OAuth2 bearer tokens, minted
  /// from `gcp_credentials_json` or from Application Default Credentials
  /// when empty. Tokens refresh automatically before they expire. Unless
  /// `properties` configures S3 access, the same credentials also back
  /// `gs://` table locations through a native GCS filesystem.
  bool gcp_auth = false;
  /// Google service-account key JSON.
  std::string gcp_credentials_json;
  /// Project id for the `x-goog-user-project` header; empty omits it.
  std::string gcp_user_project;
};

/// Stable type tags for checkpoint-persisted partition values, covering the
/// result types of the supported transforms over the supported source
/// types. Deliberately not iceberg-cpp's own type ids: their enumerator
/// values are unpinned and may shift between library versions, while these
/// values persist across restarts, including upgrades.
enum class LiteralType : int32_t {
  boolean = 1,
  int32 = 2,
  int64 = 3,
  float32 = 4,
  float64 = 5,
  date = 6,
  time = 7,
  timestamp = 8,
  timestamp_tz = 9,
  string = 10,
  binary = 11,
};

/// One partition value in checkpoint-persistable form: a stable type tag
/// (a `LiteralType` value) plus the Iceberg spec's binary single-value
/// serialization. Null values carry the type only.
struct SerializedLiteral {
  int32_t type = 0;
  bool is_null = true;
  std::vector<uint8_t> value;

  friend auto operator==(SerializedLiteral const&, SerializedLiteral const&)
    -> bool
    = default;

  friend auto inspect(auto& f, SerializedLiteral& x) -> bool {
    return f.object(x).fields(f.field("type", x.type),
                              f.field("is_null", x.is_null),
                              f.field("value", x.value));
  }
};

/// A written-but-uncommitted data file in checkpoint-persistable form, so
/// that a restarted operator can commit files its previous incarnation
/// uploaded but did not get to commit. Covers exactly what this plugin's
/// writers produce (plain Parquet data files); delete files and encryption
/// metadata are not representable.
struct SerializedDataFile {
  /// Full URI of the file in the table's data location.
  std::string path;
  int64_t record_count = 0;
  int64_t file_size = 0;
  /// The partition spec the partition values belong to.
  std::optional<int32_t> spec_id;
  std::vector<SerializedLiteral> partition;
  std::map<int32_t, int64_t> column_sizes;
  std::map<int32_t, int64_t> value_counts;
  std::map<int32_t, int64_t> null_value_counts;
  std::map<int32_t, int64_t> nan_value_counts;
  std::map<int32_t, std::vector<uint8_t>> lower_bounds;
  std::map<int32_t, std::vector<uint8_t>> upper_bounds;
  std::vector<int64_t> split_offsets;
  std::optional<int32_t> sort_order_id;

  friend auto operator==(SerializedDataFile const&, SerializedDataFile const&)
    -> bool
    = default;

  friend auto inspect(auto& f, SerializedDataFile& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("record_count", x.record_count),
                              f.field("file_size", x.file_size),
                              f.field("spec_id", x.spec_id),
                              f.field("partition", x.partition),
                              f.field("column_sizes", x.column_sizes),
                              f.field("value_counts", x.value_counts),
                              f.field("null_value_counts", x.null_value_counts),
                              f.field("nan_value_counts", x.nan_value_counts),
                              f.field("lower_bounds", x.lower_bounds),
                              f.field("upper_bounds", x.upper_bounds),
                              f.field("split_offsets", x.split_offsets),
                              f.field("sort_order_id", x.sort_order_id));
  }
};

/// Identifies one commit of one writer for exactly-once deduplication. Both
/// parts are stamped into the snapshot summary; a restarted operator searches
/// the table's snapshot history for the pair to decide whether the data files
/// restored from its checkpoint already committed before the crash.
struct CommitTag {
  /// Random id minted once per pipeline, stable across restarts.
  std::string writer_id;
  /// Strictly increasing per writer; each value commits at most once.
  uint64_t sequence = 0;
};

} // namespace tenzir::plugins::iceberg
