//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/facade.hpp"
#include "tenzir/plugins/iceberg/restore.hpp"

#include <tenzir/amazon.hpp>
#include <tenzir/any.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/aws_credentials.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/detail/enum.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/to_string.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/util/byte_size.h>
#include <folly/coro/UnboundedQueue.h>

#include <algorithm>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

constexpr auto default_max_size = uint64_t{512} * 1024 * 1024;
constexpr auto default_timeout = std::chrono::minutes{15};
constexpr auto commit_max_attempts = 5;
constexpr auto commit_initial_backoff = std::chrono::milliseconds{250};
/// Total in-memory bytes buffered across all partitions before the largest
/// buffer closes into a data file early. The budget bounds buffered data:
/// with N partitions buffering, the size floor of early-closed files
/// degrades gracefully as budget/N instead of cliffing at a small writer
/// cap. `max_open_partitions` separately bounds the per-partition overhead
/// the budget cannot see.
constexpr auto default_buffer_size = uint64_t{1024} * 1024 * 1024;
/// A partition graduates from buffering batches to a streaming Parquet
/// writer once it accumulates this much data; hot partitions stream while
/// cold ones only hold cheap Arrow buffers.
constexpr auto stream_threshold = uint64_t{64} * 1024 * 1024;
/// Every streaming writer holds Parquet row-group encoder state, so their
/// count is capped separately from buffering partitions. When exceeded, the
/// largest open file closes early: it frees the most encoder state, and it
/// is the file that was going to rotate soonest anyway. Every file it
/// produces already carries at least `stream_threshold` bytes.
constexpr auto max_streaming_writers = size_t{64};
/// Upper bound on simultaneously open partitions. The byte budget bounds
/// buffered data, but every open partition also costs a map entry, its key
/// strings, and a rotation-timer task that the budget cannot see; a burst
/// of high-cardinality partition keys with tiny rows could otherwise grow
/// that overhead without bound until `timeout`.
constexpr auto max_open_partitions = size_t{4096};

TENZIR_ENUM(mode, create_append, create, append);
TENZIR_ENUM(aws_catalog_service, glue, s3tables);

/// The column that becomes the table's registered sort order when a created
/// table has a matching top-level timestamp (the OCSF event time convention).
constexpr auto default_sort_column = std::string_view{"time"};

struct ToIcebergArgs {
  located<std::string> table_id;
  located<secret> catalog = {secret::make_literal(""), location::unknown};
  Option<located<std::string>> mode;
  Option<located<secret>> warehouse;
  Option<located<secret>> table_location;
  Option<located<record>> aws_iam;
  Option<located<std::string>> catalog_aws_service;
  Option<located<secret>> s3_endpoint;
  Option<located<bool>> s3_path_style;
  Option<located<secret>> token;
  Option<located<bool>> gcp_auth;
  Option<located<secret>> gcp_service_account_key;
  Option<located<std::string>> gcp_project;
  Option<ast::expression> partition_by;
  Option<located<uint64_t>> max_size;
  Option<located<uint64_t>> buffer_size;
  Option<located<duration>> timeout;
  location operator_location;
};

/// Splits "ns.sub_ns.table" into namespace levels and the table name.
auto split_table_id(std::string_view table_id)
  -> std::pair<std::vector<std::string>, std::string> {
  auto parts = detail::split(table_id, ".");
  auto ns = std::vector<std::string>{};
  ns.reserve(parts.size() - 1);
  for (const auto& part : parts | std::views::take(parts.size() - 1)) {
    ns.emplace_back(part);
  }
  return {std::move(ns), std::string{parts.back()}};
}

/// The partition transforms addressable from `partition_by`, by their
/// Iceberg names. These are matched symbolically and never evaluated, so
/// they do not collide with TQL functions of the same name (`day` the
/// transform truncates to a day, `day()` the function extracts the day of
/// the month).
constexpr auto partition_transforms
  = std::array<std::pair<std::string_view, PartitionTransform>, 6>{{
    {"year", PartitionTransform::year},
    {"month", PartitionTransform::month},
    {"day", PartitionTransform::day},
    {"hour", PartitionTransform::hour},
    {"bucket", PartitionTransform::bucket},
    {"truncate", PartitionTransform::truncate},
  }};

auto has_parameter(PartitionTransform transform) -> bool {
  return transform == PartitionTransform::bucket
         or transform == PartitionTransform::truncate;
}

/// Casts `source` to the target type of `options`, collecting the pieces
/// into `out`. Arrow casts are all-or-nothing per array, so a single bad
/// value would fail the whole column; this bisects around the rows the cast
/// kernel rejects (numeric overflow, unparsable strings) in
/// O(failures * log rows) casts and nulls only those.
auto cast_valid_rows(const std::shared_ptr<arrow::Array>& source,
                     const arrow::compute::CastOptions& options,
                     std::vector<std::shared_ptr<arrow::Array>>& out,
                     int64_t& failures) -> void {
  auto cast = arrow::compute::Cast(source, options);
  if (cast.ok()) {
    out.push_back(cast->make_array());
    return;
  }
  if (source->length() == 1) {
    failures += 1;
    out.push_back(
      check(arrow::MakeArrayOfNull(options.to_type.GetSharedPtr(), 1)));
    return;
  }
  const auto half = source->length() / 2;
  cast_valid_rows(source->Slice(0, half), options, out, failures);
  cast_valid_rows(source->Slice(half), options, out, failures);
}

/// Counts nulls in `array` at rows where `parent` is valid. Nulls under a
/// null parent row are legal even for a required child field: the row's
/// struct is null as a whole, so the child carries no value of its own.
auto nulls_under_valid_parent(const std::shared_ptr<arrow::Array>& array,
                              const std::shared_ptr<arrow::Array>& parent)
  -> int64_t {
  if (array->null_count() == 0) {
    return 0;
  }
  if (not parent or parent->null_count() == 0) {
    return array->null_count();
  }
  auto nulls = check(arrow::compute::IsNull(array));
  auto valid = check(arrow::compute::IsValid(parent));
  auto conflict = check(arrow::compute::And(nulls, valid));
  return static_cast<const arrow::BooleanArray&>(*conflict.make_array())
    .true_count();
}

/// Extracts the dotted path of a field expression, e.g. `metadata.version`.
auto parse_partition_source(const ast::expression& expr, diagnostic_handler& dh)
  -> failure_or<std::string> {
  auto path = ast::field_path::try_from(expr);
  if (not path or path->path().empty()) {
    diagnostic::error("expected a field").primary(expr).emit(dh);
    return failure::promise();
  }
  auto segments = std::vector<std::string_view>{};
  segments.reserve(path->path().size());
  for (const auto& segment : path->path()) {
    segments.push_back(segment.id.name);
  }
  return fmt::to_string(fmt::join(segments, "."));
}

/// Parses one element of `partition_by`: either a bare field (identity
/// transform) or a symbolic transform call like `day(time)` or
/// `bucket(class_uid, 16)`.
auto parse_partition_field(const ast::expression& expr, diagnostic_handler& dh)
  -> failure_or<PartitionField> {
  const auto* call = try_as<ast::function_call>(expr);
  if (not call) {
    TRY(auto source, parse_partition_source(expr, dh));
    return PartitionField{
      .source = std::move(source),
      .transform = PartitionTransform::identity,
      .parameter = {},
    };
  }
  auto transform = std::optional<PartitionTransform>{};
  if (call->fn.path.size() == 1) {
    for (const auto& [name, candidate] : partition_transforms) {
      if (call->fn.path[0].name == name) {
        transform = candidate;
        break;
      }
    }
  }
  if (not transform) {
    diagnostic::error("unknown partition transform")
      .primary(call->fn.get_location())
      .note("supported transforms are `year`, `month`, `day`, `hour`, "
            "`bucket`, and `truncate`")
      .emit(dh);
    return failure::promise();
  }
  const auto arity = has_parameter(*transform) ? size_t{2} : size_t{1};
  if (call->args.size() != arity) {
    diagnostic::error("`{}` expects exactly {} argument{}",
                      call->fn.path[0].name, arity, arity == 1 ? "" : "s")
      .primary(expr)
      .emit(dh);
    return failure::promise();
  }
  TRY(auto source, parse_partition_source(call->args[0], dh));
  auto parameter = std::optional<int64_t>{};
  if (has_parameter(*transform)) {
    const auto* constant = try_as<ast::constant>(call->args[1]);
    auto value = std::optional<int64_t>{};
    if (constant) {
      match(
        constant->value,
        [&](int64_t x) {
          value = x;
        },
        [&](uint64_t x) {
          if (std::cmp_less_equal(x, std::numeric_limits<int64_t>::max())) {
            value = static_cast<int64_t>(x);
          }
        },
        [](const auto&) {});
    }
    if (not value or *value <= 0
        or *value > std::numeric_limits<int32_t>::max()) {
      diagnostic::error("`{}` expects a positive integer",
                        call->fn.path[0].name)
        .primary(call->args[1])
        .emit(dh);
      return failure::promise();
    }
    parameter = *value;
  }
  return PartitionField{
    .source = std::move(source),
    .transform = *transform,
    .parameter = parameter,
  };
}

/// Parses the `partition_by` argument: a list of partition fields.
auto parse_partition_by(const ast::expression& expr, diagnostic_handler& dh)
  -> failure_or<std::vector<PartitionField>> {
  const auto* list = try_as<ast::list>(expr);
  if (not list) {
    diagnostic::error("`partition_by` expects a list of fields or partition "
                      "transforms, e.g. `[class_uid, day(time)]`")
      .primary(expr)
      .emit(dh);
    return failure::promise();
  }
  auto result = std::vector<PartitionField>{};
  result.reserve(list->items.size());
  for (const auto& item : list->items) {
    const auto* item_expr = try_as<ast::expression>(item);
    if (not item_expr) {
      diagnostic::error("expected a field or a partition transform")
        .primary(into_location(item))
        .emit(dh);
      return failure::promise();
    }
    TRY(auto field, parse_partition_field(*item_expr, dh));
    result.push_back(std::move(field));
  }
  return result;
}

class ToIceberg final : public Operator<table_slice, void> {
public:
  struct RotateRequested {
    /// The `PartitionGroup::key` of the open partition to rotate.
    std::string partition;
    int64_t generation;
  };
  struct CommitRequested {
    int64_t generation;
  };
  using Message = variant<RotateRequested, CommitRequested>;

  /// Rows accumulating toward one partition's next data file. A partition
  /// starts out buffering Arrow batches in memory — cheap to hold, so any
  /// number of partitions may buffer under the shared `buffer_size` budget —
  /// and graduates to a streaming Parquet writer once it accumulates
  /// `stream_threshold` bytes.
  struct OpenPartition {
    /// Buffered batches, before graduation; empty once `writer` is engaged.
    std::vector<std::shared_ptr<arrow::Array>> buffered;
    int64_t buffered_bytes = 0;
    /// The streaming writer, once the partition graduates.
    Option<FileWriter> writer;
    /// Top-level columns the open data file omits because every row seen
    /// at graduation held null; empty while buffering or when the file
    /// carries the full table schema.
    std::vector<bool> omitted;
    /// The partition tuple, for opening the data file at graduation or
    /// close.
    PartitionTuple partition;
    /// The human-readable partition path, for diagnostics.
    std::string path;
    /// Bytes written to the open data file; 0 while buffering.
    int64_t bytes = 0;
    int64_t generation = 0;
    folly::CancellationSource timer_cancel;
  };

  /// A closed data file staged for a later commit, together with its
  /// checkpoint-persistable form. The latter is computed at close time
  /// because `snapshot` runs synchronously.
  struct StagedFile {
    DataFile file;
    SerializedDataFile serialized;
  };

  explicit ToIceberg(ToIcebergArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // A non-empty writer id this early means `snapshot` ran with restored
    // checkpoint state before `start`; commits then align with checkpoints
    // from the beginning.
    const auto restored = not writer_id_.empty();
    if (not restored) {
      writer_id_ = fmt::to_string(uuid::random());
    }
    checkpointing_ = restored;
    TENZIR_DEBUG("to_iceberg: starting writer `{}`{}", writer_id_,
                 restored ? " (restored from checkpoint)" : "");
    if (done_) {
      co_return;
    }
    auto& dh = ctx.dh();
    events_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_iceberg"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    bytes_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_iceberg"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    auto config = CatalogConfig{};
    auto warehouse = std::string{};
    auto s3_endpoint = std::string{};
    auto token = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("catalog", args_.catalog, config.uri, dh),
    };
    if (args_.warehouse) {
      requests.push_back(
        make_secret_request("warehouse", *args_.warehouse, warehouse, dh));
    }
    if (args_.table_location) {
      requests.push_back(make_secret_request("location", *args_.table_location,
                                             table_location_, dh));
    }
    if (args_.s3_endpoint) {
      requests.push_back(make_secret_request("s3_endpoint", *args_.s3_endpoint,
                                             s3_endpoint, dh));
    }
    if (args_.token) {
      requests.push_back(make_secret_request("token", *args_.token, token, dh));
    }
    if (args_.gcp_service_account_key) {
      requests.push_back(make_secret_request("gcp_service_account_key",
                                             *args_.gcp_service_account_key,
                                             config.gcp_credentials_json, dh));
    }
    if (not co_await ctx.resolve_secrets(std::move(requests))) {
      done_ = true;
      co_return;
    }
    auto aws_iam = args_.aws_iam
                     ? std::optional<located<record>>{*args_.aws_iam}
                     : std::nullopt;
    auto auth = co_await resolve_aws_iam_auth(aws_iam, std::nullopt, ctx);
    if (not auth) {
      done_ = true;
      co_return;
    }
    config.warehouse = std::move(warehouse);
    const auto uses_managed_aws_catalog
      = static_cast<bool>(args_.catalog_aws_service);
    config.use_s3_file_io
      = auth->credentials.has_value() or uses_managed_aws_catalog;
    if (args_.catalog_aws_service) {
      const auto service = *from_string<enum aws_catalog_service>(
        args_.catalog_aws_service->inner);
      config.aws_catalog_signing_name
        = service == aws_catalog_service::glue ? "glue" : "s3tables";
    }
    if (not uses_managed_aws_catalog and auth->credentials
        and not auth->credentials->region.empty()) {
      config.properties["client.region"] = auth->credentials->region;
    }
    if (not token.empty()) {
      config.properties["rest.auth.type"] = "oauth2";
      config.properties["token"] = std::move(token);
    }
    config.gcp_auth = (args_.gcp_auth and args_.gcp_auth->inner)
                      or static_cast<bool>(args_.gcp_service_account_key);
    if (args_.gcp_project) {
      config.gcp_user_project = args_.gcp_project->inner;
    }
    const auto needs_provider
      = uses_managed_aws_catalog
        or (auth->credentials
            and (not auth->credentials->role.empty()
                 or not auth->credentials->profile.empty()
                 or auth->credentials->web_identity.has_value()));
    if (needs_provider) {
      // Profile, assume-role, and web-identity providers talk to STS and
      // the shared config machinery, so the AWS SDK must be initialized
      // for every provider, not just for managed AWS catalogs.
      auto initialized = ensure_aws_sdk_initialized();
      if (not initialized) {
        const auto source = args_.aws_iam ? args_.aws_iam->source
                                          : args_.catalog_aws_service->source;
        diagnostic::error("failed to initialize AWS authentication: {}",
                          initialized.error().message)
          .primary(source)
          .emit(dh);
        done_ = true;
        co_return;
      }
      if (uses_managed_aws_catalog) {
        // Signing-region resolution consults a named profile's config file,
        // which requires the initialized SDK; that is why it happens here
        // and not alongside the other catalog properties above.
        auto credentials = Option<resolved_aws_credentials>{None{}};
        if (auth->credentials) {
          credentials = *auth->credentials;
        }
        config.aws_signing_region = amazon::resolve_region(None{}, credentials);
        config.properties["client.region"] = config.aws_signing_region;
      }
      // Use the shared live provider for catalog signing and S3 access. The
      // default chain and STS-backed providers refresh expiring credentials,
      // including profiles backed by credential_process.
      auto region = std::optional<std::string>{};
      if (not config.aws_signing_region.empty()) {
        region = config.aws_signing_region;
      } else if (auth->credentials and not auth->credentials->region.empty()) {
        region = auth->credentials->region;
      }
      auto provider = co_await spawn_blocking(
        [credentials = auth->credentials, region = std::move(region)]() {
          return make_aws_credentials_provider(credentials, region);
        });
      if (not provider) {
        const auto source = args_.aws_iam ? args_.aws_iam->source
                                          : args_.catalog_aws_service->source;
        diagnostic::error(provider.error()).primary(source).emit(dh);
        done_ = true;
        co_return;
      }
      config.aws_credentials_provider = std::move(*provider);
    } else if (auth->credentials) {
      const auto& creds = *auth->credentials;
      if (not creds.access_key_id.empty()) {
        config.properties["s3.access-key-id"] = creds.access_key_id;
      }
      if (not creds.secret_access_key.empty()) {
        config.properties["s3.secret-access-key"] = creds.secret_access_key;
      }
      if (not creds.session_token.empty()) {
        config.properties["s3.session-token"] = creds.session_token;
      }
    }
    if (not s3_endpoint.empty()) {
      config.properties["s3.endpoint"] = std::move(s3_endpoint);
      // Custom S3-compatible endpoints practically always require path-style
      // addressing; virtual-hosted style remains opt-out via `s3_path_style`.
      const auto path_style
        = not args_.s3_path_style or args_.s3_path_style->inner;
      config.properties["s3.path-style-access"] = path_style ? "true" : "false";
    }
    mode_ = args_.mode ? *from_string<enum mode>(args_.mode->inner)
                       : mode::create_append;
    if (args_.partition_by) {
      auto parsed = parse_partition_by(*args_.partition_by, dh);
      if (parsed.is_error()) {
        done_ = true;
        co_return;
      }
      partition_fields_ = std::move(*parsed);
    }
    std::tie(ns_, table_name_) = split_table_id(args_.table_id.inner);
    auto catalog
      = co_await spawn_blocking([config = std::move(config)]() mutable {
          return Catalog::open(std::move(config));
        });
    if (not catalog) {
      diagnostic::error("failed to open Iceberg catalog: {}",
                        catalog.error().message)
        .primary(args_.catalog.source)
        .emit(dh);
      done_ = true;
      co_return;
    }
    catalog_ = std::move(*catalog);
    auto table = co_await spawn_blocking(
      [catalog = *catalog_, ns = ns_, name = table_name_]() mutable {
        return catalog.load_table(ns, name);
      });
    if (table) {
      // After a restore, an existing table is our own previous work only
      // when the checkpoint proves this operator created or wrote it. An
      // empty checkpoint can predate the first input, and an externally
      // created table must still raise the create-mode conflict then.
      const auto resumable = may_resume_existing_table({
        .restored = restored,
        .created_table = created_table_,
        .commit_seq = commit_seq_,
        .restored_files = epoch_snapshot_.size(),
      });
      if (mode_ == mode::create and not resumable) {
        diagnostic::error("Iceberg table `{}` already exists",
                          args_.table_id.inner)
          .primary(args_.table_id.source)
          .note("use `mode=\"create_append\"` or `mode=\"append\"` to write "
                "into an existing table")
          .emit(dh);
        done_ = true;
        co_return;
      }
      // A matching name is not enough after a restore: if the table was
      // dropped and recreated since the checkpoint, the rows committed
      // before it are gone, and resuming would silently continue on the
      // impostor.
      if (restored_table_identity_conflict(table_uuid_, table->uuid())) {
        diagnostic::error("Iceberg table `{}` is not the table the "
                          "checkpoint wrote to",
                          args_.table_id.inner)
          .primary(args_.table_id.source)
          .note("the table was dropped and recreated since the checkpoint; "
                "the rows committed before the checkpoint are lost")
          .emit(dh);
        done_ = true;
        co_return;
      }
      TENZIR_DEBUG("to_iceberg: loaded existing table `{}`",
                   args_.table_id.inner);
      co_await adopt_table(std::move(*table), ctx);
      if (done_) {
        co_return;
      }
      co_await reconcile(ctx);
      co_return;
    }
    if (table.error().kind != Error::Kind::not_found) {
      diagnostic::error("failed to open Iceberg table `{}`: {}",
                        args_.table_id.inner, table.error().message)
        .primary(args_.table_id.source)
        .emit(dh);
      done_ = true;
      co_return;
    }
    // Once the checkpoint records committed epochs or holds uncommitted
    // file handles, recreating a fresh table from post-checkpoint input
    // would silently lose the rows written before the checkpoint.
    const auto missing_is_fatal = missing_table_is_fatal({
      .restored = restored,
      .created_table = created_table_,
      .commit_seq = commit_seq_,
      .restored_files = epoch_snapshot_.size(),
    });
    if (missing_is_fatal) {
      diagnostic::error("Iceberg table `{}` no longer exists, but the "
                        "checkpoint records {} committed epochs and {} "
                        "uncommitted data files",
                        args_.table_id.inner, commit_seq_,
                        epoch_snapshot_.size())
        .primary(args_.table_id.source)
        .note("recreating the table would silently lose the rows committed "
              "before the checkpoint")
        .emit(dh);
      done_ = true;
      co_return;
    }
    if (mode_ == mode::append) {
      diagnostic::error("Iceberg table `{}` does not exist",
                        args_.table_id.inner)
        .primary(args_.table_id.source)
        .note("use `mode=\"create_append\"` to create missing tables")
        .emit(dh);
      done_ = true;
      co_return;
    }
    // The table is created from the schema of the first arriving events; see
    // `ensure_table`.
    TENZIR_DEBUG("to_iceberg: table `{}` does not exist; creating it from the "
                 "first input schema",
                 args_.table_id.inner);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_ or input.rows() == 0) {
      co_return;
    }
    if (not table_) {
      co_await ensure_table(input, ctx);
      if (done_) {
        co_return;
      }
    }
    co_await ensure_schema(input, ctx);
    if (done_) {
      co_return;
    }
    auto batch = project(std::move(input), ctx);
    if (not batch) {
      co_return;
    }
    auto groups
      = co_await spawn_blocking([table = *table_, batch = *batch]() mutable {
          return table.split_by_partition(std::move(batch));
        });
    if (not groups) {
      fail(groups.error(), "failed to compute partition values", ctx);
      co_return;
    }
    for (const auto& group : *groups) {
      co_await write_partition(group, *batch, ctx);
      if (done_) {
        co_return;
      }
    }
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await control_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto msg = std::move(result).as<Message>();
    co_await co_match(
      msg,
      [&](RotateRequested& r) -> Task<void> {
        auto it = partitions_.find(r.partition);
        if (it == partitions_.end() or it->second.generation != r.generation) {
          co_return;
        }
        TENZIR_DEBUG("to_iceberg: rotating partition `{}` after `timeout`",
                     r.partition);
        co_await close_partition(r.partition, ctx);
        if (not done_ and not checkpointing_) {
          co_await commit_staged(pending_files_, ctx);
        }
      },
      [&](CommitRequested& r) -> Task<void> {
        if (not commit_timer_cancel_
            or r.generation != commit_timer_generation_) {
          co_return;
        }
        commit_timer_cancel_.reset();
        TENZIR_DEBUG("to_iceberg: committing evicted partition files after "
                     "`timeout`");
        if (not done_ and not checkpointing_) {
          co_await commit_staged(pending_files_, ctx);
        }
      });
  }

  auto snapshot(Serde& serde) -> void override {
    serde("writer_id", writer_id_);
    serde("commit_seq", commit_seq_);
    serde("files", epoch_snapshot_);
    serde("done", done_);
    serde("created_table", created_table_);
    serde("table_uuid", table_uuid_);
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    // The first checkpoint switches commits from per-rotation to
    // checkpoint-aligned: files close here, their handles persist in
    // `snapshot`, and `post_commit` appends them once the checkpoint is
    // durable. Committing before durability would duplicate the data when a
    // crash replays input from the previous checkpoint.
    if (not checkpointing_) {
      TENZIR_DEBUG("to_iceberg: first checkpoint; commits now align with "
                   "checkpoints");
    }
    checkpointing_ = true;
    cancel_commit_timer();
    co_await close_all(ctx);
    if (done_) {
      co_return;
    }
    for (auto& staged : pending_files_) {
      epoch_snapshot_.push_back(staged.serialized);
      epoch_files_.push_back(std::move(staged));
    }
    pending_files_.clear();
    TENZIR_DEBUG("to_iceberg: checkpoint carries {} uncommitted data files",
                 epoch_snapshot_.size());
  }

  auto post_commit(OpCtx& ctx) -> Task<void> override {
    // The checkpoint carrying `epoch_snapshot_` is durable, so this commit
    // is the acknowledgment: a crash from here on restores a checkpoint that
    // knows these files, and `reconcile` settles whether the commit landed.
    // Files closed after `snapshot` stay in `pending_files_` for the next
    // epoch; committing them now would make data visible that a restored
    // checkpoint knows nothing about.
    co_await commit_staged(epoch_files_, ctx);
    if (not done_) {
      epoch_snapshot_.clear();
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await close_all(ctx);
    if (done_) {
      co_return FinalizeBehavior::done;
    }
    TENZIR_DEBUG("to_iceberg: finalizing with {} epoch and {} pending data "
                 "files",
                 epoch_files_.size(), pending_files_.size());
    // The last checkpoint's files commit under their persisted sequence
    // number; folding newer files into that commit would break restart
    // reconciliation, which trusts the checkpoint to describe the tagged
    // snapshot exactly.
    co_await commit_staged(epoch_files_, ctx);
    if (done_) {
      co_return FinalizeBehavior::done;
    }
    epoch_snapshot_.clear();
    co_await commit_staged(pending_files_, ctx);
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  /// Imports the table's Arrow schema. Emits a diagnostic and sets `done_`
  /// on failure, returning nullptr.
  auto import_table_schema(Table& table, OpCtx& ctx)
    -> std::shared_ptr<arrow::Schema> {
    auto c_schema = ArrowSchema{};
    if (auto result = table.export_arrow_schema(&c_schema); not result) {
      diagnostic::error("failed to map Iceberg table schema: {}",
                        result.error().message)
        .primary(args_.table_id.source)
        .emit(ctx.dh());
      done_ = true;
      return nullptr;
    }
    auto imported = arrow::ImportSchema(&c_schema);
    if (not imported.ok()) {
      diagnostic::error("failed to import Iceberg table schema: {}",
                        imported.status().ToString())
        .primary(args_.table_id.source)
        .emit(ctx.dh());
      done_ = true;
      return nullptr;
    }
    return imported.MoveValueUnsafe();
  }

  /// Imports the table's Arrow schema and makes `table` the write target.
  auto set_table(Table table, OpCtx& ctx) -> void {
    if (auto result = table.validate_partitioning(); not result) {
      diagnostic::error("cannot write to partitioned Iceberg table `{}`: {}",
                        args_.table_id.inner, result.error().message)
        .primary(args_.table_id.source)
        .emit(ctx.dh());
      done_ = true;
      return;
    }
    auto schema = import_table_schema(table, ctx);
    if (not schema) {
      return;
    }
    // A different table schema invalidates the fingerprint cache: inputs it
    // declared covered may need re-evolution, e.g. re-adding a column that
    // a concurrent writer dropped.
    if (target_schema_ and not schema->Equals(*target_schema_)) {
      evolved_schemas_.clear();
    }
    table_ = std::move(table);
    target_schema_ = std::move(schema);
    // The UUID persists in checkpoints so that a restore can tell a
    // dropped-and-recreated table apart from the one it wrote to.
    table_uuid_ = table_->uuid();
  }

  /// Verifies that a supplied `partition_by` matches an existing table's
  /// spec: the table's spec governs the fanout, and the user's expectation
  /// and reality must not diverge silently. Emits a diagnostic and sets
  /// `done_` on a mismatch. No-op without `partition_by`.
  auto check_partition_spec(Table table, OpCtx& ctx) -> Task<void> {
    if (not args_.partition_by) {
      co_return;
    }
    auto checked = co_await spawn_blocking(
      [table = std::move(table), fields = partition_fields_]() mutable {
        return table.check_partition_spec(fields);
      });
    if (not checked) {
      diagnostic::error("{}", checked.error().message)
        .primary(args_.partition_by->get_location())
        .note("the partition spec of an existing table cannot be changed "
              "by this operator; drop `partition_by` to append to the "
              "table as-is")
        .emit(ctx.dh());
      done_ = true;
    }
  }

  /// Makes `table` the write target mid-run. When its write layout differs
  /// from the current one, a supplied `partition_by` is re-checked against
  /// the adopted spec: a concurrent change of the table's default spec must
  /// not silently override the user's requested partitioning when the same
  /// divergence at startup is a hard error.
  auto adopt_table(Table table, OpCtx& ctx) -> Task<void> {
    const auto layout_changed
      = not table_ or not table_->has_same_write_layout(table);
    set_table(std::move(table), ctx);
    if (done_ or not layout_changed) {
      co_return;
    }
    co_await check_partition_spec(*table_, ctx);
  }

  /// Creates the table from the schema of the first arriving events. Only
  /// reached in the create modes when the table did not exist at start.
  auto ensure_table(const table_slice& input, OpCtx& ctx) -> Task<void> {
    const auto& schema = as<record_type>(input.schema());
    auto options = CreateTableOptions{};
    options.location = table_location_;
    options.partition_by = partition_fields_;
    for (const auto& field : schema.fields()) {
      if (field.name == default_sort_column and is<time_type>(field.type)) {
        options.sort_column = std::string{default_sort_column};
        break;
      }
    }
    auto dropped = std::make_shared<std::vector<std::string>>();
    auto fell_back = std::make_shared<bool>(false);
    auto table = co_await spawn_blocking(
      [catalog = *catalog_, ns = ns_, name = table_name_, ty = input.schema(),
       options = std::move(options), dropped, fell_back,
       fall_back_to_load
       = mode_ == mode::create_append]() mutable -> Result<Table> {
        if (auto result = catalog.ensure_namespace(ns); not result) {
          return std::unexpected{result.error()};
        }
        auto created = catalog.create_table(ns, name, as<record_type>(ty),
                                            options, *dropped);
        if (not created and fall_back_to_load
            and created.error().kind == Error::Kind::already_exists) {
          // Another writer won the race between our existence check and the
          // creation; appending to their table is exactly what this mode
          // promises.
          *fell_back = true;
          return catalog.load_table(ns, name);
        }
        return created;
      });
    for (const auto& reason : *dropped) {
      warn_once(fmt::format("cannot represent column in Iceberg: {}", reason),
                ctx);
    }
    if (not table) {
      fail(table.error(),
           fmt::format("failed to create Iceberg table `{}`",
                       args_.table_id.inner),
           ctx);
      co_return;
    }
    TENZIR_DEBUG("to_iceberg: ensured table `{}` exists ({} partition fields)",
                 args_.table_id.inner, partition_fields_.size());
    if (*fell_back) {
      // The winner's table governs; the same `partition_by` compatibility
      // check as for tables that already existed at start applies.
      co_await check_partition_spec(*table, ctx);
      if (done_) {
        co_return;
      }
    } else {
      created_table_ = true;
    }
    set_table(std::move(*table), ctx);
  }

  /// Evolves the table schema to cover the input: fields the table does not
  /// have yet become new columns, and existing columns too narrow for the
  /// input widen where the spec allows it (a metadata-only commit). Runs
  /// once per distinct input schema. The schema commit lands at the catalog
  /// *before* any data file carries the new columns, so Parquet files are
  /// only ever stamped with field IDs the catalog has confirmed. When a
  /// concurrent writer updates the table underneath us, the commit conflicts;
  /// we reload the table and re-derive the diff against their changes.
  auto ensure_schema(const table_slice& input, OpCtx& ctx) -> Task<void> {
    auto fingerprint = input.schema().make_fingerprint();
    if (evolved_schemas_.contains(fingerprint)) {
      co_return;
    }
    // Evolution retries run against a local handle so that the operator's
    // projection target and open data file stay untouched unless the table
    // schema actually changes.
    auto current = *table_;
    auto backoff = duration{commit_initial_backoff};
    for (auto attempt = 1;; ++attempt) {
      auto dropped = std::make_shared<std::vector<std::string>>();
      auto evolved = co_await spawn_blocking(
        [table = current, ty = input.schema(), dropped]() mutable {
          return table.evolve_schema(as<record_type>(ty), *dropped);
        });
      for (const auto& reason : *dropped) {
        warn_once(fmt::format("cannot represent column in Iceberg: {}", reason),
                  ctx);
      }
      if (evolved) {
        if (*evolved) {
          TENZIR_DEBUG("to_iceberg: evolved schema of table `{}` for input "
                       "schema `{}` (attempt {})",
                       args_.table_id.inner, input.schema().name(), attempt);
          // The projection target changes: open data files stay valid under
          // the old schema, but must not receive new-schema batches. Close
          // them, then adopt the evolved schema from the update response.
          co_await close_all(ctx);
          if (done_) {
            co_return;
          }
          co_await adopt_table(std::move(**evolved), ctx);
          if (done_) {
            co_return;
          }
          if (not checkpointing_) {
            co_await commit_staged(pending_files_, ctx);
            if (done_) {
              co_return;
            }
          }
        } else {
          // The table already covers the input without an update of our
          // own. That is no proof that the cached projection schema does: a
          // concurrent writer may have added the very fields this input
          // carries, observed either through a conflict reload or through
          // the table handle refreshing its metadata. Adopt the table's
          // current schema when it differs, or projection would silently
          // drop those fields from this writer's rows.
          auto schema = import_table_schema(current, ctx);
          if (done_) {
            co_return;
          }
          if (not schema->Equals(*target_schema_)) {
            TENZIR_DEBUG("to_iceberg: adopting concurrently evolved schema "
                         "of table `{}` for input schema `{}` (attempt {})",
                         args_.table_id.inner, input.schema().name(), attempt);
            co_await close_all(ctx);
            if (done_) {
              co_return;
            }
            co_await adopt_table(std::move(current), ctx);
            if (done_) {
              co_return;
            }
            if (not checkpointing_) {
              co_await commit_staged(pending_files_, ctx);
              if (done_) {
                co_return;
              }
            }
          }
        }
        evolved_schemas_.insert(std::move(fingerprint));
        co_return;
      }
      const auto kind = evolved.error().kind;
      if (attempt >= commit_max_attempts
          or (kind != Error::Kind::conflict
              and kind != Error::Kind::transient)) {
        fail(evolved.error(),
             fmt::format("failed to evolve schema of Iceberg table `{}`",
                         args_.table_id.inner),
             ctx);
        co_return;
      }
      diagnostic::warning("{} schema update (attempt {}/{}): {}",
                          kind == Error::Kind::conflict ? "conflicting"
                                                        : "failed",
                          attempt, commit_max_attempts, evolved.error().message)
        .primary(args_.operator_location)
        .emit(ctx.dh());
      // Schema evolution is a metadata commit on the write path, so a brief
      // catalog hiccup gets the same backoff as append commits.
      if (kind == Error::Kind::transient) {
        co_await sleep_for(backoff);
        backoff *= 2;
        continue;
      }
      // A concurrent writer updated the table between our load and the
      // schema commit; re-derive the diff against their changes.
      auto reloaded = co_await spawn_blocking(
        [catalog = *catalog_, ns = ns_, name = table_name_]() mutable {
          return catalog.load_table(ns, name);
        });
      if (not reloaded) {
        // The reload is part of the same idempotent conflict recovery as
        // the evolution commit, so a transient catalog error consumes
        // another attempt instead of terminating the sink.
        if (reloaded.error().kind == Error::Kind::transient
            and attempt < commit_max_attempts) {
          diagnostic::warning("failed to reload Iceberg table (attempt "
                              "{}/{}): {}",
                              attempt, commit_max_attempts,
                              reloaded.error().message)
            .primary(args_.operator_location)
            .emit(ctx.dh());
          co_await sleep_for(backoff);
          backoff *= 2;
          continue;
        }
        fail(reloaded.error(),
             fmt::format("failed to reload Iceberg table `{}`",
                         args_.table_id.inner),
             ctx);
        co_return;
      }
      // Evolving the schema of a table that was dropped and recreated
      // underneath us would commit metadata to the impostor and adopt it.
      if (restored_table_identity_conflict(table_uuid_, reloaded->uuid())) {
        diagnostic::error("Iceberg table `{}` was dropped and recreated "
                          "while writing to it",
                          args_.table_id.inner)
          .primary(args_.table_id.source)
          .note("the rows already committed to the previous table are "
                "gone; refusing to continue on its replacement")
          .emit(ctx.dh());
        done_ = true;
        co_return;
      }
      current = std::move(*reloaded);
    }
  }

  /// Converts Tenzir-specific extension arrays into their Iceberg
  /// representation: ip, subnet, and enumeration become strings. Returns
  /// nullptr for arrays that must not be written.
  auto normalize(std::shared_ptr<arrow::Array> array, OpCtx& ctx)
    -> std::shared_ptr<arrow::Array> {
    if (array->type()->id() != arrow::Type::EXTENSION) {
      return array;
    }
    auto ty = type::from_arrow(*array->type());
    return match(
      ty,
      [&](const enumeration_type& t) -> std::shared_ptr<arrow::Array> {
        return resolve_enumerations(
                 t,
                 std::static_pointer_cast<enumeration_type::array_type>(array))
          .second;
      },
      [&](const concepts::one_of<ip_type, subnet_type> auto&)
        -> std::shared_ptr<arrow::Array> {
        return to_string(multi_series{series{ty, array}},
                         args_.operator_location, ctx.dh())
          .array;
      },
      [&](const auto&) -> std::shared_ptr<arrow::Array> {
        return nullptr;
      });
  }

  /// Recursively projects a source column onto a field of the table schema.
  /// Missing and unrepresentable data is null-filled with a one-time
  /// warning; nested structs project field-by-field so that records written
  /// into struct columns line up by name, not by position. Required (non-
  /// null) table fields must not be null-filled — a data file carrying
  /// nulls for them would be invalid or fail late in the writer — so any
  /// null that would land under a valid `parent` row raises an error
  /// instead, returning nullptr with `done_` set.
  auto project_column(std::shared_ptr<arrow::Array> source,
                      const std::shared_ptr<arrow::Field>& target,
                      const std::string& path, int64_t rows,
                      const std::shared_ptr<arrow::Array>& parent, OpCtx& ctx)
    -> std::shared_ptr<arrow::Array> {
    const auto required = not target->nullable();
    auto fail_required
      = [&](const std::string& reason) -> std::shared_ptr<arrow::Array> {
      diagnostic::error("column `{}` is required in the Iceberg table but {}",
                        path, reason)
        .primary(args_.operator_location)
        .emit(ctx.dh());
      done_ = true;
      return nullptr;
    };
    auto null_column = [&] {
      return check(arrow::MakeArrayOfNull(target->type(), rows));
    };
    // Null-filling a required field is only legal when every affected row
    // sits under a null parent row.
    auto may_null_fill = [&] {
      if (not required) {
        return true;
      }
      return parent and parent->null_count() == parent->length();
    };
    const auto had_source = static_cast<bool>(source);
    if (source) {
      source = normalize(std::move(source), ctx);
    }
    if (not source) {
      if (not may_null_fill()) {
        return fail_required(had_source
                               ? "its values cannot be represented in Iceberg"
                               : "the input does not provide it");
      }
      return null_column();
    }
    auto type_mismatch = [&]() -> std::shared_ptr<arrow::Array> {
      if (not may_null_fill()) {
        return fail_required(
          fmt::format("the input column has type `{}` instead of `{}`",
                      source->type()->ToString(), target->type()->ToString()));
      }
      warn_once(fmt::format("column `{}` has type `{}` but the table "
                            "expects `{}`; writing nulls instead",
                            path, source->type()->ToString(),
                            target->type()->ToString()),
                ctx);
      return null_column();
    };
    switch (target->type()->id()) {
      case arrow::Type::STRUCT: {
        auto struct_source
          = std::dynamic_pointer_cast<arrow::StructArray>(source);
        if (not struct_source) {
          return type_mismatch();
        }
        const auto& target_fields = target->type()->fields();
        auto children = arrow::ArrayVector{};
        children.reserve(target_fields.size());
        auto used = std::unordered_set<std::string>{};
        for (const auto& field : target_fields) {
          auto child = struct_source->GetFieldByName(field->name());
          if (child) {
            used.insert(field->name());
          }
          auto projected
            = project_column(std::move(child), field,
                             fmt::format("{}.{}", path, field->name()), rows,
                             struct_source, ctx);
          if (not projected) {
            return nullptr;
          }
          children.push_back(std::move(projected));
        }
        for (const auto& field : struct_source->struct_type()->fields()) {
          if (not used.contains(field->name())) {
            warn_once(fmt::format("column `{}.{}` does not exist in the "
                                  "table and will be dropped",
                                  path, field->name()),
                      ctx);
          }
        }
        // A fresh validity bitmap sidesteps offset bookkeeping for sliced
        // source arrays.
        auto bitmap = std::shared_ptr<arrow::Buffer>{};
        auto null_count = struct_source->null_count();
        if (null_count > 0) {
          auto valid = check(arrow::compute::IsValid(struct_source)).array();
          bitmap = valid->buffers[1];
        }
        auto result = check(arrow::StructArray::Make(
          children, target_fields, std::move(bitmap), null_count));
        if (required and nulls_under_valid_parent(result, parent) > 0) {
          return fail_required("the input holds null records for it");
        }
        return result;
      }
      case arrow::Type::LIST: {
        auto list_source = std::dynamic_pointer_cast<arrow::ListArray>(source);
        if (not list_source) {
          return type_mismatch();
        }
        // The values child covers the array's entire buffer range, so the
        // original offsets remain valid for the converted values. That
        // range may also hold values no list slot references, so the
        // element's required check runs on the flattened result below, not
        // inside the recursion.
        const auto& element = target->type()->field(0);
        auto values
          = project_column(list_source->values(), element->WithNullable(true),
                           path + "[]", list_source->values()->length(),
                           nullptr, ctx);
        if (not values) {
          return nullptr;
        }
        auto result = std::make_shared<arrow::ListArray>(
          arrow::list(element), list_source->length(),
          list_source->data()->buffers[1], std::move(values),
          list_source->null_bitmap(), list_source->data()->null_count,
          list_source->offset());
        if (not element->nullable() and result->values()->null_count() > 0) {
          auto flat = check(result->Flatten());
          if (flat->null_count() > 0) {
            diagnostic::error("column `{}[]` is required in the Iceberg "
                              "table but the input holds null elements",
                              path)
              .primary(args_.operator_location)
              .emit(ctx.dh());
            done_ = true;
            return nullptr;
          }
        }
        if (required and nulls_under_valid_parent(result, parent) > 0) {
          return fail_required("the input holds null lists for it");
        }
        return result;
      }
      default: {
        if (source->type()->Equals(target->type())) {
          if (required and nulls_under_valid_parent(source, parent) > 0) {
            return fail_required("the input holds null values for it");
          }
          return source;
        }
        if (not arrow::compute::CanCast(*source->type(), *target->type())) {
          return type_mismatch();
        }
        auto options = arrow::compute::CastOptions::Safe(target->type());
        options.allow_time_truncate = true;
        auto cast = arrow::compute::Cast(source, options);
        if (cast.ok()) {
          auto result = cast->make_array();
          if (required and nulls_under_valid_parent(result, parent) > 0) {
            return fail_required("the input holds null values for it");
          }
          return result;
        }
        // The cast exists but some values cannot convert, e.g. numeric
        // overflow or unparsable strings; null only the offending rows.
        // Failing values were non-null, so for a required field they always
        // count against valid rows.
        if (required) {
          return fail_required(fmt::format(
            "some of its `{}` values cannot convert to the "
            "table's `{}`",
            source->type()->ToString(), target->type()->ToString()));
        }
        auto pieces = std::vector<std::shared_ptr<arrow::Array>>{};
        auto failures = int64_t{0};
        cast_valid_rows(source, options, pieces, failures);
        warn_once(fmt::format("column `{}` has `{}` values that cannot "
                              "convert to the table's `{}`; writing nulls "
                              "for those rows",
                              path, source->type()->ToString(),
                              target->type()->ToString()),
                  ctx);
        return check(arrow::Concatenate(pieces));
      }
    }
  }

  /// Reorders, converts, and casts a slice's columns to the table schema,
  /// recursing into nested records. Missing columns are null-filled; unknown
  /// columns are dropped with a one-time warning per input schema.
  auto project(table_slice input, OpCtx& ctx)
    -> Option<std::shared_ptr<arrow::StructArray>> {
    const auto rows = detail::narrow_cast<int64_t>(input.rows());
    auto batch = to_record_batch(input);
    auto arrays = arrow::ArrayVector{};
    arrays.reserve(target_schema_->num_fields());
    auto used = std::unordered_set<std::string>{};
    for (const auto& field : target_schema_->fields()) {
      auto column = batch->GetColumnByName(field->name());
      if (column) {
        used.insert(field->name());
      }
      auto projected = project_column(std::move(column), field, field->name(),
                                      rows, nullptr, ctx);
      if (not projected) {
        return None{};
      }
      arrays.push_back(std::move(projected));
    }
    for (const auto& field : batch->schema()->fields()) {
      if (not used.contains(field->name())) {
        warn_once(fmt::format("column `{}` does not exist in the table and "
                              "will be dropped",
                              field->name()),
                  ctx);
      }
    }
    auto result = arrow::StructArray::Make(arrays, target_schema_->fields());
    if (not result.ok()) {
      diagnostic::error("failed to assemble batch: {}",
                        result.status().ToString())
        .emit(ctx.dh());
      done_ = true;
      return None{};
    }
    return result.MoveValueUnsafe();
  }

  /// Flags the top-level columns that hold only nulls in every given
  /// batch. Wide tables make this worthwhile: Parquet encodes
  /// definition levels for every column of every row group, so a table
  /// column the input never uses still costs per-row work — omitting it
  /// from the data file removes that cost, and readers restore it as
  /// nulls through the same field-id projection that serves files written
  /// before a schema evolution.
  static auto
  all_null_columns(std::span<const std::shared_ptr<arrow::Array>> pieces)
    -> std::vector<bool> {
    auto mask = std::vector<bool>{};
    for (const auto& piece : pieces) {
      const auto& array = static_cast<const arrow::StructArray&>(*piece);
      const auto fields = detail::narrow_cast<size_t>(array.num_fields());
      mask.resize(fields, true);
      for (auto i = size_t{0}; i < fields; ++i) {
        if (not mask[i]) {
          continue;
        }
        const auto& child = *array.field(detail::narrow_cast<int>(i));
        mask[i] = child.null_count() == child.length();
      }
    }
    return mask;
  }

  /// Whether a projected batch holds only nulls in every column the open
  /// data file omits.
  static auto
  covered(const std::vector<bool>& omitted, const arrow::Array& batch) -> bool {
    const auto& array = static_cast<const arrow::StructArray&>(batch);
    for (auto i = size_t{0}; i < omitted.size(); ++i) {
      if (not omitted[i]) {
        continue;
      }
      const auto& child = *array.field(detail::narrow_cast<int>(i));
      if (child.null_count() != child.length()) {
        return false;
      }
    }
    return true;
  }

  /// Drops the omitted columns from a projected batch, matching it to the
  /// data file's pruned schema.
  static auto drop_omitted(const std::shared_ptr<arrow::Array>& piece,
                           const std::vector<bool>& omitted)
    -> std::shared_ptr<arrow::Array> {
    if (not std::ranges::contains(omitted, true)) {
      return piece;
    }
    const auto& array = static_cast<const arrow::StructArray&>(*piece);
    auto children = arrow::ArrayVector{};
    auto fields = arrow::FieldVector{};
    for (auto i = 0; i < array.num_fields(); ++i) {
      if (omitted[detail::narrow_cast<size_t>(i)]) {
        continue;
      }
      children.push_back(array.field(i));
      fields.push_back(array.struct_type()->field(i));
    }
    return check(arrow::StructArray::Make(children, fields));
  }

  /// Buffers one partition group of a projected batch, graduating the
  /// partition to a streaming writer at `stream_threshold`. Rotates and
  /// commits when the open file reaches `max_size`, and closes the largest
  /// buffers early when the shared buffer budget runs over.
  auto write_partition(const PartitionGroup& group,
                       const std::shared_ptr<arrow::StructArray>& batch,
                       OpCtx& ctx) -> Task<void> {
    auto sub = std::static_pointer_cast<arrow::Array>(batch);
    if (not group.rows.empty()) {
      auto builder = arrow::Int64Builder{};
      check(builder.AppendValues(group.rows));
      auto indices = check(builder.Finish());
      auto taken = arrow::compute::Take(sub, indices);
      if (not taken.ok()) {
        diagnostic::error("failed to split batch: {}",
                          taken.status().ToString())
          .primary(args_.operator_location)
          .emit(ctx.dh());
        done_ = true;
        co_return;
      }
      sub = taken->make_array();
    }
    auto it = partitions_.find(group.key);
    if (it != partitions_.end() and it->second.writer
        and not covered(it->second.omitted, *sub)) {
      // The open data file omits columns this batch has values for; close
      // it so that the next file's schema covers them again.
      co_await close_partition(group.key, ctx);
      if (done_) {
        co_return;
      }
      if (not checkpointing_) {
        co_await commit_staged(pending_files_, ctx);
        if (done_) {
          co_return;
        }
      }
      it = partitions_.find(group.key);
    }
    if (it == partitions_.end()) {
      it = partitions_
             .try_emplace(group.key,
                          OpenPartition{
                            .buffered = {},
                            .buffered_bytes = 0,
                            .writer = {},
                            .omitted = {},
                            .partition = group.partition,
                            .path = group.path,
                            .bytes = 0,
                            .generation = ++writer_generation_,
                            .timer_cancel = {},
                          })
             .first;
      arm_rotation_timer(group.key, it->second, ctx);
      TENZIR_TRACE("to_iceberg: opened partition `{}`", group.key);
    }
    auto& partition = it->second;
    events_counter_.add(sub->length());
    const auto max_size
      = args_.max_size ? args_.max_size->inner : default_max_size;
    if (partition.writer) {
      auto pieces = std::vector<std::shared_ptr<arrow::Array>>{};
      pieces.push_back(std::move(sub));
      co_await stream_into(partition, std::move(pieces), ctx);
      if (done_) {
        co_return;
      }
    } else {
      const auto sub_bytes
        = static_cast<int64_t>(arrow::util::TotalBufferSize(*sub->data()));
      partition.buffered.push_back(std::move(sub));
      partition.buffered_bytes += sub_bytes;
      total_buffered_ += sub_bytes;
      if (std::cmp_greater_equal(partition.buffered_bytes,
                                 std::min(stream_threshold, max_size))) {
        co_await graduate(partition, ctx);
        if (done_) {
          co_return;
        }
      }
    }
    if (partition.writer
        and std::cmp_greater_equal(partition.bytes, max_size)) {
      co_await close_partition(group.key, ctx);
      if (done_) {
        co_return;
      }
      if (not checkpointing_) {
        co_await commit_staged(pending_files_, ctx);
        if (done_) {
          co_return;
        }
      }
    }
    co_await enforce_buffer_budget(ctx);
  }

  /// Appends batches to the partition's streaming writer, tracking written
  /// bytes.
  auto stream_into(OpenPartition& partition,
                   std::vector<std::shared_ptr<arrow::Array>> pieces,
                   OpCtx& ctx) -> Task<void> {
    auto written = co_await spawn_blocking(
      [writer = *partition.writer, omitted = partition.omitted,
       pieces = std::move(pieces)]() mutable -> Result<int64_t> {
        for (const auto& piece : pieces) {
          auto c_array = ArrowArray{};
          if (auto status
              = arrow::ExportArray(*drop_omitted(piece, omitted), &c_array);
              not status.ok()) {
            return std::unexpected{Error{
              Error::Kind::permanent,
              fmt::format("failed to export batch: {}", status.ToString()),
            }};
          }
          if (auto result = writer.write(&c_array); not result) {
            return std::unexpected{result.error()};
          }
        }
        return writer.bytes_written();
      });
    if (not written) {
      fail(written.error(), "failed to write data file", ctx);
      co_return;
    }
    if (*written > partition.bytes) {
      bytes_counter_.add(*written - partition.bytes);
    }
    partition.bytes = *written;
  }

  /// Opens the partition's data file and streams the buffered batches into
  /// it. Only streaming writers hold Parquet encoder state, so at the cap
  /// the largest open file closes first: it frees the most encoder state,
  /// and it is the file that was going to rotate soonest anyway.
  auto graduate(OpenPartition& partition, OpCtx& ctx) -> Task<void> {
    if (streaming_count_ >= max_streaming_writers) {
      warn_once(fmt::format("exceeded {} streaming partition writers; "
                            "closing the largest data file early",
                            max_streaming_writers),
                ctx);
      auto victim = partitions_.end();
      for (auto it = partitions_.begin(); it != partitions_.end(); ++it) {
        if (it->second.writer
            and (victim == partitions_.end()
                 or it->second.bytes > victim->second.bytes)) {
          victim = it;
        }
      }
      TENZIR_ASSERT(victim != partitions_.end());
      TENZIR_DEBUG("to_iceberg: closing largest open file for partition `{}` "
                   "({} bytes) early to stay under {} streaming writers",
                   victim->first, victim->second.bytes, max_streaming_writers);
      co_await close_partition(victim->first, ctx);
      if (done_) {
        co_return;
      }
    }
    auto opened = co_await spawn_blocking(
      [table = *table_, partition = partition.partition,
       omitted = all_null_columns(partition.buffered)]() mutable
        -> Result<std::pair<FileWriter, std::vector<bool>>> {
        auto writer = table.new_file_writer(partition, omitted);
        if (not writer) {
          return std::unexpected{writer.error()};
        }
        return std::pair{std::move(*writer), std::move(omitted)};
      });
    if (not opened) {
      fail(opened.error(), "failed to open data file writer", ctx);
      co_return;
    }
    TENZIR_DEBUG("to_iceberg: partition graduates to a streaming writer at {} "
                 "buffered bytes",
                 partition.buffered_bytes);
    partition.writer = std::move(opened->first);
    partition.omitted = std::move(opened->second);
    streaming_count_ += 1;
    total_buffered_ -= partition.buffered_bytes;
    partition.buffered_bytes = 0;
    co_await stream_into(partition, std::move(partition.buffered), ctx);
    partition.buffered = {};
  }

  /// Closes the largest buffering partitions into data files until the
  /// total buffered bytes fit the budget again. All files closed in one
  /// eviction pass commit together in non-checkpointed pipelines. This keeps
  /// them visible even when the evicted partition owned the only timer.
  auto enforce_buffer_budget(OpCtx& ctx) -> Task<void> {
    const auto budget
      = args_.buffer_size ? args_.buffer_size->inner : default_buffer_size;
    auto closed_any = false;
    while (std::cmp_greater(total_buffered_, budget)
           or partitions_.size() > max_open_partitions) {
      auto victim = partitions_.end();
      for (auto it = partitions_.begin(); it != partitions_.end(); ++it) {
        if (not it->second.writer
            and (victim == partitions_.end()
                 or it->second.buffered_bytes
                      > victim->second.buffered_bytes)) {
          victim = it;
        }
      }
      if (victim == partitions_.end()) {
        co_return;
      }
      if (std::cmp_greater(total_buffered_, budget)) {
        warn_once(fmt::format("buffered partition data exceeds `buffer_size` "
                              "({} bytes); closing the largest buffer early, "
                              "which may produce small files",
                              budget),
                  ctx);
      } else {
        warn_once(fmt::format("exceeded {} open partitions; closing the "
                              "largest buffer early, which may produce "
                              "small files",
                              max_open_partitions),
                  ctx);
      }
      TENZIR_DEBUG("to_iceberg: {} buffered bytes exceed budget {}; closing "
                   "largest buffer `{}` ({} bytes) early",
                   total_buffered_, budget, victim->first,
                   victim->second.buffered_bytes);
      co_await close_partition(victim->first, ctx);
      if (done_) {
        co_return;
      }
      closed_any = true;
    }
    if (closed_any and not checkpointing_) {
      arm_commit_timer(ctx);
    }
  }

  /// Closes the given partition into a data file and stages it for the next
  /// commit: a buffering partition opens, writes, and finalizes its file in
  /// one step, a streaming partition finishes its open writer. Does not
  /// commit itself, so an eviction cascade under high partition cardinality
  /// cannot turn into a commit storm.
  auto close_partition(const std::string& key, OpCtx& ctx) -> Task<void> {
    auto node = partitions_.extract(key);
    if (node.empty() or done_) {
      co_return;
    }
    auto& partition = node.mapped();
    partition.timer_cancel.requestCancellation();
    auto closed = Option<Result<DataFile>>{};
    if (partition.writer) {
      streaming_count_ -= 1;
      closed = co_await spawn_blocking([writer = *partition.writer]() mutable {
        return writer.finish();
      });
    } else {
      // The one-shot writer never outlives this call, so it does not count
      // against the streaming cap.
      total_buffered_ -= partition.buffered_bytes;
      closed = co_await spawn_blocking(
        [table = *table_, tuple = partition.partition,
         pieces = std::move(partition.buffered)]() mutable -> Result<DataFile> {
          auto omitted = all_null_columns(pieces);
          auto writer = table.new_file_writer(tuple, omitted);
          if (not writer) {
            return std::unexpected{writer.error()};
          }
          for (const auto& piece : pieces) {
            auto c_array = ArrowArray{};
            if (auto status
                = arrow::ExportArray(*drop_omitted(piece, omitted), &c_array);
                not status.ok()) {
              return std::unexpected{Error{
                Error::Kind::permanent,
                fmt::format("failed to export batch: {}", status.ToString()),
              }};
            }
            if (auto result = writer->write(&c_array); not result) {
              return std::unexpected{result.error()};
            }
          }
          return writer->finish();
        });
    }
    auto& file = *closed;
    if (not file) {
      fail(file.error(), "failed to finalize data file", ctx);
      co_return;
    }
    auto serialized = file->serialize();
    if (not serialized) {
      fail(serialized.error(), "failed to persist data file handle", ctx);
      co_return;
    }
    if (not partition.writer) {
      bytes_counter_.add(serialized->file_size);
    }
    TENZIR_DEBUG("to_iceberg: staged data file `{}` ({} events, {} bytes)",
                 serialized->path, serialized->record_count,
                 serialized->file_size);
    pending_files_.push_back(StagedFile{
      .file = std::move(*file),
      .serialized = std::move(*serialized),
    });
  }

  /// Closes all open partitions into data files, staging them for the next
  /// commit.
  auto close_all(OpCtx& ctx) -> Task<void> {
    auto keys = std::vector<std::string>{};
    keys.reserve(partitions_.size());
    for (const auto& [key, unused] : partitions_) {
      keys.push_back(key);
    }
    for (const auto& key : keys) {
      co_await close_partition(key, ctx);
      if (done_) {
        co_return;
      }
    }
  }

  /// Settles data files restored from a checkpoint: the previous incarnation
  /// uploaded them and persisted their handles, but may or may not have
  /// committed them before it stopped. The commit tag decides: if the table
  /// has the tagged snapshot, the files are already visible and dropped
  /// here; otherwise they commit now, under the same sequence number.
  /// Upstream replays everything after the checkpoint, so either way no row
  /// is lost or duplicated.
  auto reconcile(OpCtx& ctx) -> Task<void> {
    co_await settle_epoch_files(ctx);
    if (done_) {
      co_return;
    }
    // Commits may also have landed *after* the restored sequence: rotation-
    // or finalize-driven commits between the last checkpoint and the crash.
    // Upstream replays their rows, and without a durable record of those
    // commits' contents the replay cannot be deduplicated safely — rotation
    // points can differ across runs, so dropping replayed rows could lose
    // data instead. Skip past the orphaned sequence numbers so commit tags
    // stay unambiguous, and surface the potential duplication.
    auto orphaned = uint64_t{0};
    while (table_->has_commit(CommitTag{writer_id_, commit_seq_})) {
      commit_seq_ += 1;
      orphaned += 1;
    }
    if (orphaned > 0) {
      diagnostic::warning("Iceberg table `{}` has {} commits from before the "
                          "restart that the restored checkpoint does not "
                          "cover",
                          args_.table_id.inner, orphaned)
        .primary(args_.table_id.source)
        .note("rows written between the last checkpoint and the restart may "
              "appear twice after the replay")
        .emit(ctx.dh());
    }
  }

  /// Settles the data files restored from the checkpoint; see `reconcile`.
  auto settle_epoch_files(OpCtx& ctx) -> Task<void> {
    if (epoch_snapshot_.empty()) {
      co_return;
    }
    if (table_->has_commit(CommitTag{writer_id_, commit_seq_})) {
      TENZIR_DEBUG("to_iceberg: restart reconciliation: commit seq {} already "
                   "landed; dropping {} restored file handles",
                   commit_seq_, epoch_snapshot_.size());
      epoch_snapshot_.clear();
      commit_seq_ += 1;
      co_return;
    }
    // The tagged snapshot is the primary proof of the previous commit, but
    // Iceberg snapshot expiration may have erased it while the committed
    // rows live on. The restored file paths carry per-file UUIDs and commit
    // atomically, so any of them being live in the current snapshot proves
    // the commit landed. (A compaction rewriting all of them away inside
    // the same crash window would still slip through; that stacked
    // coincidence remains unhandled.)
    auto paths = std::vector<std::string>{};
    paths.reserve(epoch_snapshot_.size());
    for (const auto& serialized : epoch_snapshot_) {
      paths.push_back(serialized.path);
    }
    auto referenced = co_await spawn_blocking(
      [table = *table_, paths = std::move(paths)]() mutable {
        return table.references_any_data_file(paths);
      });
    if (not referenced) {
      fail(referenced.error(), "failed to inspect table data files", ctx);
      co_return;
    }
    if (*referenced) {
      TENZIR_DEBUG("to_iceberg: restart reconciliation: restored data files "
                   "are live in the table without the tagged snapshot; "
                   "dropping {} file handles",
                   epoch_snapshot_.size());
      epoch_snapshot_.clear();
      commit_seq_ += 1;
      co_return;
    }
    for (const auto& serialized : epoch_snapshot_) {
      auto file = DataFile::deserialize(serialized);
      if (not file) {
        fail(file.error(), "failed to restore data file from checkpoint", ctx);
        co_return;
      }
      epoch_files_.push_back(StagedFile{
        .file = std::move(*file),
        .serialized = serialized,
      });
    }
    TENZIR_DEBUG("to_iceberg: restart reconciliation: committing {} restored "
                 "data files under seq {}",
                 epoch_files_.size(), commit_seq_);
    co_await commit_staged(epoch_files_, ctx);
    if (not done_) {
      epoch_snapshot_.clear();
    }
  }

  /// Commits the given staged files as one FastAppend snapshot tagged with
  /// the current commit sequence, clearing them and advancing the sequence
  /// on success. Transient failures retry with exponential backoff; a
  /// conflicting concurrent update reloads the table and retries the commit
  /// with the same files, unless the reloaded table shows the tagged
  /// snapshot after all (a commit whose success got lost) — appending again
  /// would duplicate the rows. On success, the table handle is replaced with
  /// the refreshed one so that the next commit does not race against our own
  /// snapshot.
  auto commit_staged(std::vector<StagedFile>& staged, OpCtx& ctx)
    -> Task<void> {
    if (staged.empty() or done_) {
      co_return;
    }
    if (not checkpointing_) {
      cancel_commit_timer();
    }
    const auto staged_count = staged.size();
    auto files = std::vector<DataFile>{};
    files.reserve(staged_count);
    for (auto it = staged.begin(); it != staged.begin() + staged_count; ++it) {
      files.push_back(it->file);
    }
    const auto tag = CommitTag{writer_id_, commit_seq_};
    auto backoff = duration{commit_initial_backoff};
    for (auto attempt = 1;; ++attempt) {
      auto result
        = co_await spawn_blocking([table = *table_, files, tag]() mutable {
            return table.commit_append(files, tag);
          });
      if (result) {
        auto bytes = int64_t{0};
        for (auto it = staged.begin(); it != staged.begin() + staged_count;
             ++it) {
          bytes += it->serialized.file_size;
        }
        TENZIR_DEBUG("to_iceberg: committed {} data files ({} bytes) as seq "
                     "{} of writer `{}` on attempt {}",
                     staged_count, bytes, tag.sequence, tag.writer_id, attempt);
        const auto layout_changed = not table_->has_same_write_layout(*result);
        if (layout_changed) {
          TENZIR_DEBUG("to_iceberg: table write layout changed after commit; "
                       "closing {} open partitions",
                       partitions_.size());
          co_await close_all(ctx);
          if (done_) {
            co_return;
          }
        }
        staged.erase(staged.begin(), staged.begin() + staged_count);
        commit_seq_ += 1;
        co_await adopt_table(std::move(*result), ctx);
        if (not done_ and not checkpointing_ and not staged.empty()) {
          co_await commit_staged(staged, ctx);
        }
        co_return;
      }
      if (attempt >= commit_max_attempts
          or (result.error().kind != Error::Kind::transient
              and result.error().kind != Error::Kind::conflict)) {
        fail(result.error(), "failed to commit snapshot", ctx);
        co_return;
      }
      diagnostic::warning("commit failure (attempt {}/{}): {}", attempt,
                          commit_max_attempts, result.error().message)
        .primary(args_.operator_location)
        .emit(ctx.dh());
      if (result.error().kind == Error::Kind::conflict) {
        // A concurrent update won the race; commit again on top of it.
        auto reloaded = co_await spawn_blocking(
          [catalog = *catalog_, ns = ns_, name = table_name_]() mutable {
            return catalog.load_table(ns, name);
          });
        if (not reloaded) {
          // The reload is part of conflict recovery and idempotent with
          // respect to the staged files, so a transient catalog error
          // consumes another attempt instead of terminating the sink.
          if (reloaded.error().kind == Error::Kind::transient
              and attempt < commit_max_attempts) {
            diagnostic::warning("failed to reload Iceberg table (attempt "
                                "{}/{}): {}",
                                attempt, commit_max_attempts,
                                reloaded.error().message)
              .primary(args_.operator_location)
              .emit(ctx.dh());
            co_await sleep_for(backoff);
            backoff *= 2;
            continue;
          }
          fail(reloaded.error(), "failed to reload Iceberg table", ctx);
          co_return;
        }
        // A conflict can also mean the table was dropped and recreated
        // underneath us; appending the staged files to the impostor would
        // silently abandon the rows already committed to the old table.
        if (restored_table_identity_conflict(table_uuid_, reloaded->uuid())) {
          diagnostic::error("Iceberg table `{}` was dropped and recreated "
                            "while writing to it",
                            args_.table_id.inner)
            .primary(args_.table_id.source)
            .note("the rows already committed to the previous table are "
                  "gone; refusing to continue on its replacement")
            .emit(ctx.dh());
          done_ = true;
          co_return;
        }
        const auto landed = reloaded->has_commit(tag);
        const auto layout_changed
          = not table_->has_same_write_layout(*reloaded);
        if (layout_changed) {
          TENZIR_DEBUG("to_iceberg: table write layout changed while "
                       "reloading; closing {} open partitions",
                       partitions_.size());
          co_await close_all(ctx);
          if (done_) {
            co_return;
          }
        }
        if (landed) {
          TENZIR_DEBUG("to_iceberg: commit seq {} had already landed; "
                       "dropping {} staged files",
                       tag.sequence, staged_count);
          staged.erase(staged.begin(), staged.begin() + staged_count);
          commit_seq_ += 1;
        }
        co_await adopt_table(std::move(*reloaded), ctx);
        if (done_ or landed) {
          if (not done_ and not checkpointing_ and not staged.empty()) {
            co_await commit_staged(staged, ctx);
          }
          co_return;
        }
        continue;
      }
      co_await sleep_for(backoff);
      backoff *= 2;
    }
  }

  /// Spawns a timer that requests rotation of the partition after
  /// `timeout`. Cancelled when the partition closes for another reason.
  auto arm_rotation_timer(const std::string& key, OpenPartition& partition,
                          OpCtx& ctx) -> void {
    const auto timeout
      = args_.timeout ? args_.timeout->inner : duration{default_timeout};
    std::ignore = ctx.spawn_task(
      [this, key, generation = partition.generation, timeout,
       token = partition.timer_cancel.getToken()]() -> Task<void> {
        auto merged = folly::cancellation_token_merge(
          co_await folly::coro::co_current_cancellation_token, token);
        co_await folly::coro::co_withCancellation(merged, sleep_for(timeout));
        control_queue_->enqueue(RotateRequested{key, generation});
      });
  }

  /// Schedules one commit for files closed by buffer eviction. Partition
  /// timers cannot drive this commit because eviction cancels them.
  auto arm_commit_timer(OpCtx& ctx) -> void {
    if (commit_timer_cancel_ or checkpointing_) {
      return;
    }
    const auto timeout
      = args_.timeout ? args_.timeout->inner : duration{default_timeout};
    auto cancel = std::make_shared<folly::CancellationSource>();
    commit_timer_cancel_ = cancel;
    const auto generation = ++commit_timer_generation_;
    std::ignore = ctx.spawn_task(
      [this, generation, timeout, token = cancel->getToken()]() -> Task<void> {
        auto merged = folly::cancellation_token_merge(
          co_await folly::coro::co_current_cancellation_token, token);
        co_await folly::coro::co_withCancellation(merged, sleep_for(timeout));
        control_queue_->enqueue(CommitRequested{generation});
      });
  }

  auto cancel_commit_timer() -> void {
    if (not commit_timer_cancel_) {
      return;
    }
    commit_timer_cancel_->requestCancellation();
    commit_timer_cancel_.reset();
  }

  auto fail(const Error& error, std::string_view what, OpCtx& ctx) -> void {
    diagnostic::error("{}: {}", what, error.message)
      .primary(args_.operator_location)
      .emit(ctx.dh());
    done_ = true;
  }

  auto warn_once(std::string message, OpCtx& ctx) -> void {
    if (warned_.insert(message).second) {
      diagnostic::warning("{}", message)
        .primary(args_.operator_location)
        .emit(ctx.dh());
    }
  }

  ToIcebergArgs args_;
  enum mode mode_ = mode::create_append;
  std::vector<std::string> ns_;
  std::string table_name_;
  Option<Catalog> catalog_;
  Option<Table> table_;
  std::shared_ptr<arrow::Schema> target_schema_;
  /// The parsed `partition_by` argument; empty when not partitioning.
  std::vector<PartitionField> partition_fields_;
  /// Open partitions, keyed by `PartitionGroup::key`.
  std::unordered_map<std::string, OpenPartition> partitions_;
  /// Total bytes buffered across all buffering partitions; bounded by the
  /// `buffer_size` budget.
  int64_t total_buffered_ = 0;
  /// Number of partitions holding a streaming writer; bounded by
  /// `max_streaming_writers`.
  size_t streaming_count_ = 0;
  /// Closed data files not yet assigned to a checkpoint; the next commit
  /// picks them up.
  std::vector<StagedFile> pending_files_;
  /// Closed data files frozen into the last checkpoint, awaiting the
  /// post-checkpoint commit.
  std::vector<StagedFile> epoch_files_;
  /// The checkpoint-persistable mirror of `epoch_files_`, kept in lockstep;
  /// after a restart, it holds the restored handles until `reconcile`
  /// settles them.
  std::vector<SerializedDataFile> epoch_snapshot_;
  /// Identifies this pipeline's commits across restarts.
  std::string writer_id_;
  /// UUID of the table this operator writes to, recorded when the table is
  /// first loaded or created and persisted across restarts.
  std::string table_uuid_;
  /// Sequence number of the next commit; each value commits at most once.
  uint64_t commit_seq_ = 0;
  /// Whether this operator created the table itself; restored across
  /// restarts so that create-mode conflict detection stays intact for
  /// checkpoints that predate the first input.
  bool created_table_ = false;
  /// Whether checkpoints drive commits. Without checkpoints, every rotation
  /// commits directly (there is no replay to guard against); the first
  /// checkpoint switches to one commit per checkpoint, which `post_commit`
  /// issues after the checkpoint is durable.
  bool checkpointing_ = false;
  int64_t writer_generation_ = 0;
  int64_t commit_timer_generation_ = 0;
  std::shared_ptr<folly::CancellationSource> commit_timer_cancel_;
  bool done_ = false;
  std::unordered_set<std::string> warned_;
  /// Explicit location used only if this operator creates the table.
  std::string table_location_;
  /// Fingerprints of input schemas the table schema is known to cover.
  std::unordered_set<std::string> evolved_schemas_;
  MetricsCounter events_counter_;
  MetricsCounter bytes_counter_;
  mutable Box<folly::coro::UnboundedQueue<Message>> control_queue_{
    std::in_place,
  };
};

/// Registers `bucket` and `truncate` as function names so that the symbolic
/// partition transforms in `partition_by` resolve. Unlike `day` and friends,
/// which are ordinary TQL functions that `partition_by` merely reinterprets,
/// these two have no evaluatable semantics; using them anywhere else fails
/// at compile time.
class PartitionTransformPlugin final : public virtual function_plugin {
public:
  explicit PartitionTransformPlugin(std::string name) : name_{std::move(name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    diagnostic::error("`{}` is an Iceberg partition transform and can only "
                      "be used in the `partition_by` argument of "
                      "`to_iceberg`",
                      name_)
      .primary(inv.call.fn.get_location())
      .emit(ctx);
    return failure::promise();
  }

private:
  std::string name_;
};

class ToIcebergPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_iceberg";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToIcebergArgs, ToIceberg>{};
    auto table_arg = d.positional("table", &ToIcebergArgs::table_id);
    d.named("catalog", &ToIcebergArgs::catalog);
    auto mode_arg = d.named("mode", &ToIcebergArgs::mode);
    d.named("warehouse", &ToIcebergArgs::warehouse);
    d.named("location", &ToIcebergArgs::table_location);
    d.named("aws_iam", &ToIcebergArgs::aws_iam);
    auto aws_catalog_arg
      = d.named("catalog_aws_service", &ToIcebergArgs::catalog_aws_service);
    auto endpoint_arg = d.named("s3_endpoint", &ToIcebergArgs::s3_endpoint);
    auto path_style_arg
      = d.named("s3_path_style", &ToIcebergArgs::s3_path_style);
    auto token_arg = d.named("token", &ToIcebergArgs::token);
    auto gcp_auth_arg = d.named("gcp_auth", &ToIcebergArgs::gcp_auth);
    auto gcp_key_arg = d.named("gcp_service_account_key",
                               &ToIcebergArgs::gcp_service_account_key);
    auto gcp_project_arg = d.named("gcp_project", &ToIcebergArgs::gcp_project);
    auto partition_by_arg
      = d.named("partition_by", &ToIcebergArgs::partition_by, "list<any>");
    auto max_size_arg = d.named("max_size", &ToIcebergArgs::max_size);
    auto buffer_size_arg = d.named("buffer_size", &ToIcebergArgs::buffer_size);
    auto timeout_arg = d.named("timeout", &ToIcebergArgs::timeout);
    d.operator_location(&ToIcebergArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto table = ctx.get(table_arg)) {
        auto parts = detail::split(table->inner, ".");
        const auto valid
          = parts.size() >= 2
            and std::ranges::none_of(parts, &std::string_view::empty);
        if (not valid) {
          diagnostic::error(
            "`table` must be of the form `namespace.table_name`")
            .primary(table->source, "got `{}`", table->inner)
            .emit(ctx);
        }
      }
      if (auto mode_opt = ctx.get(mode_arg)) {
        if (not from_string<enum mode>(mode_opt->inner)) {
          diagnostic::error(
            "`mode` must be one of `create`, `append`, or `create_append`")
            .primary(mode_opt->source, "got `{}`", mode_opt->inner)
            .emit(ctx);
        }
      }
      if (auto partition_by = ctx.get(partition_by_arg)) {
        parse_partition_by(*partition_by, ctx).ignore();
      }
      if (auto max_size = ctx.get(max_size_arg)) {
        if (max_size->inner == 0) {
          diagnostic::error("`max_size` must be a positive number")
            .primary(max_size->source)
            .emit(ctx);
        }
      }
      if (auto buffer_size = ctx.get(buffer_size_arg)) {
        if (buffer_size->inner == 0) {
          diagnostic::error("`buffer_size` must be a positive number")
            .primary(buffer_size->source)
            .emit(ctx);
        }
      }
      if (auto timeout = ctx.get(timeout_arg)) {
        if (timeout->inner <= duration::zero()) {
          diagnostic::error("`timeout` must be a positive duration")
            .primary(timeout->source)
            .emit(ctx);
        }
      }
      if (ctx.get(path_style_arg) and not ctx.get(endpoint_arg)) {
        diagnostic::error("`s3_path_style` requires `s3_endpoint`")
          .primary(*ctx.get_location(path_style_arg))
          .emit(ctx);
      }
      if (auto service = ctx.get(aws_catalog_arg)) {
        if (not from_string<enum aws_catalog_service>(service->inner)) {
          diagnostic::error(
            "`catalog_aws_service` must be `glue` or `s3tables`")
            .primary(service->source, "got `{}`", service->inner)
            .emit(ctx);
        }
        if (ctx.get(token_arg)) {
          diagnostic::error(
            "`catalog_aws_service` cannot be combined with `token`")
            .primary(service->source)
            .emit(ctx);
        }
      }
      const auto gcp_auth_opt = ctx.get(gcp_auth_arg);
      const auto uses_gcp = (gcp_auth_opt and gcp_auth_opt->inner)
                            or static_cast<bool>(ctx.get(gcp_key_arg));
      if (ctx.get(aws_catalog_arg) and uses_gcp) {
        diagnostic::error("`catalog_aws_service` cannot be combined with "
                          "Google authentication")
          .primary(*ctx.get_location(aws_catalog_arg))
          .emit(ctx);
      }
      if (ctx.get(token_arg) and uses_gcp) {
        diagnostic::error("`token` cannot be combined with Google "
                          "authentication")
          .primary(*ctx.get_location(token_arg))
          .note("`gcp_auth` and `gcp_service_account_key` mint and refresh "
                "tokens automatically")
          .emit(ctx);
      }
      if (gcp_auth_opt and not gcp_auth_opt->inner and ctx.get(gcp_key_arg)) {
        diagnostic::error("`gcp_service_account_key` requires `gcp_auth` to "
                          "be enabled")
          .primary(gcp_auth_opt->source)
          .emit(ctx);
      }
      if (ctx.get(gcp_project_arg) and not uses_gcp) {
        diagnostic::error("`gcp_project` requires `gcp_auth` or "
                          "`gcp_service_account_key`")
          .primary(*ctx.get_location(gcp_project_arg))
          .emit(ctx);
      }
#ifndef TENZIR_ICEBERG_GCS
      if (uses_gcp) {
        auto location = ctx.get_location(gcp_auth_arg);
        if (not location) {
          location = ctx.get_location(gcp_key_arg);
        }
        diagnostic::error("this build of `to_iceberg` lacks Google Cloud "
                          "support")
          .primary(*location)
          .note("rebuild against an Arrow with `ARROW_GCS=ON` and "
                "google-cloud-cpp installed")
          .emit(ctx);
      }
#endif
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::iceberg

TENZIR_REGISTER_PLUGIN(tenzir::plugins::iceberg::ToIcebergPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::iceberg::PartitionTransformPlugin{"buck"
                                                                          "et"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::iceberg::PartitionTransformPlugin{
  "truncate"})
