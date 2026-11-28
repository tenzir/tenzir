//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/option.hpp>
#include <tenzir/result.hpp>
#include <tenzir/time.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::loki {

/// Wire encoding for Grafana Loki push requests.
enum class Encoding {
  protobuf,
  json,
};

/// Failure reason when serializing a protobuf PushRequest.
enum class SerializeError {
  exceeds_max_bytes,
  serialization_failed,
};

/// Maximum serialized protobuf wire size supported by libprotobuf.
inline constexpr auto max_protobuf_message_bytes
  = static_cast<size_t>(std::numeric_limits<int32_t>::max());

/// Maximum Loki tenant ID length in bytes per Loki's multi-tenancy rules.
inline constexpr auto max_tenant_id_bytes = size_t{150};

using LabelPair = std::pair<std::string, std::string>;
using Labels = std::vector<LabelPair>;

/// Normalized Loki push endpoint URL and origin-form request target.
struct NormalizedPushUrl {
  std::string url;
  std::string target;

  friend auto operator==(NormalizedPushUrl const&, NormalizedPushUrl const&)
    -> bool
    = default;
};

/// One log entry belonging to a Loki stream.
struct Entry {
  int64_t timestamp_ns = 0;
  std::string line;
  Labels structured_metadata;
};

/// One Loki stream carrying its canonical sorted label set and entries.
struct Stream {
  Labels labels;
  std::vector<Entry> entries;
};

/// A ready-to-send batch scoped to a single Loki tenant (or none).
struct TenantBatch {
  Option<std::string> tenant;
  std::vector<Stream> streams;
  uint64_t entry_count = 0;
};

/// Seconds and non-negative nanoseconds representing a protobuf Timestamp.
struct SplitTimestamp {
  int64_t seconds = 0;
  int32_t nanos = 0;

  friend auto operator==(SplitTimestamp const&, SplitTimestamp const&) -> bool
    = default;
};

/// Parses `encoding=` argument values (`protobuf` or `json`).
auto parse_encoding(std::string_view value) -> Option<Encoding>;

/// Normalizes a Loki push endpoint URL.
///
/// Validates that `url` is an `http` or `https` URI with a non-empty host.
/// Strips any client-only fragment (`#...`), appends `/loki/api/v1/push` when
/// the URL path is empty or `/`, and preserves explicit paths and query
/// parameters. Never includes the raw URL in error messages.
auto normalize_push_url(std::string_view url)
  -> Result<NormalizedPushUrl, std::string>;

/// Returns whether `name` is a valid RFC 9110 HTTP header field name (token).
auto is_valid_header_name(std::string_view name) -> bool;

/// Returns whether `name` is managed by `to_loki` and cannot be set in
/// `headers=`. When `has_tenant_expr` is true, `X-Scope-OrgID` is also
/// reserved because the operator sets it per batch from `tenant=`.
auto is_reserved_header(std::string_view name, bool has_tenant_expr) -> bool;

/// Returns whether `value` is safe to place in an HTTP header field (forbids
/// CR, LF, NUL, and non-tab ASCII control characters).
auto is_valid_header_value(std::string_view value) -> bool;

/// Returns whether `tenant` conforms to Loki's official tenant ID rules:
/// 1..150 bytes, allowed ASCII characters `[a-zA-Z0-9!\-_.*'()]`, and not
/// `.` or `..`.
auto is_valid_tenant_id(std::string_view tenant) -> bool;

/// Returns whether `status_code` indicates a successful Loki push response.
/// Only HTTP 204 No Content is accepted; HTTP 200 (e.g. a misrouted proxy or
/// web page) and HTTP 260 (Loki's default blocked-ingestion status) are
/// rejected.
auto is_push_success_status(uint16_t status_code) -> bool;

/// Maps an arbitrary HTTP transport error message to a safe, secret-free
/// diagnostic summary.
auto classify_transport_error(std::string_view error) -> std::string_view;

/// Rounds a non-negative duration up to milliseconds so positive sub-
/// millisecond timeouts never truncate to `0ms`.
auto ceil_milliseconds(duration value) -> std::chrono::milliseconds;

/// Returns whether `name` matches Loki's stream label name grammar
/// `[a-zA-Z_][a-zA-Z0-9_]*`.
auto is_valid_label_name(std::string_view name) -> bool;

/// Formats a label value string using Go-style double-quoted escaping
/// compatible with Loki's label parser.
auto quote_label_string(std::string_view value) -> std::string;

/// Formats canonical sorted labels into Loki's protobuf stream label syntax,
/// e.g. `{app="api", env="prod"}`.
auto format_labels(Labels const& labels) -> std::string;

/// Splits a signed Unix epoch nanosecond timestamp into seconds and
/// non-negative nanoseconds in `[0, 999'999'999]` using floor division.
auto split_timestamp_ns(int64_t timestamp_ns) -> SplitTimestamp;

/// Stably sorts entries within each stream by ascending timestamp.
auto sort_entries(std::vector<Stream>& streams) -> void;

/// Serializes streams into an uncompressed Loki protobuf `PushRequest`.
///
/// Checks `ByteSizeLong()` against both `max_bytes` and protobuf's `INT32_MAX`
/// limit before allocating, and checks `SerializeToString()` for failure.
auto serialize_protobuf(std::vector<Stream> const& streams,
                        size_t max_bytes = max_protobuf_message_bytes)
  -> Result<std::string, SerializeError>;

/// Compresses `input` into a single raw Snappy block as required by Loki's
/// protobuf push endpoint.
auto snappy_compress(std::string_view input)
  -> Result<std::string, std::string>;

/// Serializes streams into Loki's JSON push request body.
auto serialize_json(std::vector<Stream> const& streams) -> std::string;

/// Estimates the uncompressed wire byte footprint of appending `entry`.
auto estimate_entry_bytes(Entry const& entry, bool is_new_stream,
                          Labels const& labels) -> uint64_t;

/// Splits a non-singleton stream batch roughly in half while preserving
/// per-stream entry ordering.
auto split_streams(std::vector<Stream>& streams) -> std::vector<Stream>;

/// Bounded multi-tenant batch buffer that groups entries by `(tenant, labels)`
/// and flushes on entry count, estimated byte size, global memory cap, or
/// per-tenant flush deadline.
class BatchBuffer {
public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  BatchBuffer(uint64_t max_batch_size, uint64_t max_batch_bytes,
              duration flush_interval, uint64_t max_total_entries,
              uint64_t max_total_bytes);

  // `PendingTenant` stores cross-referencing iterators into `by_deadline_` and
  // `pending_`. Copying is deleted; moving preserves node iterators for
  // default-allocated `std::map` and `std::multimap`.
  BatchBuffer(BatchBuffer const&) = delete;
  auto operator=(BatchBuffer const&) -> BatchBuffer& = delete;
  BatchBuffer(BatchBuffer&&) noexcept = default;
  auto operator=(BatchBuffer&&) noexcept -> BatchBuffer& = default;
  ~BatchBuffer() = default;

  /// Appends one entry and returns any batches that became ready to send.
  auto add(Option<std::string> tenant, Labels labels, Entry entry,
           TimePoint now) -> std::vector<TenantBatch>;

  /// Returns the earliest flush deadline across all open tenant batches in O(1).
  auto next_deadline() const -> Option<TimePoint>;

  /// Extracts all open tenant batches whose deadline has reached `now`.
  auto take_expired(TimePoint now) -> std::vector<TenantBatch>;

  /// Extracts every open tenant batch in deterministic tenant order.
  auto take_all() -> std::vector<TenantBatch>;

  auto empty() const -> bool;
  auto total_entries() const -> uint64_t;
  auto total_bytes() const -> uint64_t;

private:
  struct PendingTenant;
  using TenantMap = std::map<Option<std::string>, PendingTenant>;
  using DeadlineIndex = std::multimap<TimePoint, TenantMap::iterator>;

  struct PendingTenant {
    std::map<Labels, std::vector<Entry>> streams;
    uint64_t entry_count = 0;
    uint64_t estimated_bytes = 0;
    TimePoint deadline{};
    DeadlineIndex::iterator deadline_it{};
  };

  auto extract(TenantMap::iterator it) -> TenantBatch;
  auto compute_deadline(TimePoint now) const -> TimePoint;

  uint64_t max_batch_size_;
  uint64_t max_batch_bytes_;
  duration flush_interval_;
  uint64_t max_total_entries_;
  uint64_t max_total_bytes_;
  TenantMap pending_;
  DeadlineIndex by_deadline_;
  uint64_t total_entries_ = 0;
  uint64_t total_bytes_ = 0;
};

} // namespace tenzir::plugins::loki
