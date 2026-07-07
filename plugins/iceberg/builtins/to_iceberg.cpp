//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/facade.hpp"

#include <tenzir/any.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/detail/enum.hpp>
#include <tenzir/detail/string.hpp>
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
#include <folly/coro/UnboundedQueue.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

constexpr auto default_max_size = uint64_t{512} * 1024 * 1024;
constexpr auto default_timeout = std::chrono::minutes{15};
constexpr auto commit_max_attempts = 5;
constexpr auto commit_initial_backoff = std::chrono::milliseconds{250};
/// Every open partition holds an open Parquet writer with its own row-group
/// buffer, so memory scales with this cap. When exceeded, the largest open
/// file closes early: it frees the most buffered bytes, and it is the file
/// that was going to rotate soonest anyway.
constexpr auto max_open_partitions = size_t{64};

TENZIR_ENUM(mode, create_append, create, append);

/// The column that becomes the table's registered sort order when a created
/// table has a matching top-level timestamp (the OCSF event time convention).
constexpr auto default_sort_column = std::string_view{"time"};

struct ToIcebergArgs {
  located<std::string> table_id;
  located<secret> catalog = {secret::make_literal(""), location::unknown};
  Option<located<std::string>> mode;
  Option<located<secret>> warehouse;
  Option<located<record>> aws_iam;
  Option<located<secret>> s3_endpoint;
  Option<located<bool>> s3_path_style;
  Option<located<secret>> token;
  Option<ast::expression> partition_by;
  Option<located<uint64_t>> max_size;
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
  using Message = variant<RotateRequested>;

  /// An open data file accepting one partition's rows.
  struct PartitionWriter {
    FileWriter writer;
    /// The human-readable partition path, for diagnostics.
    std::string path;
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
    if (args_.s3_endpoint) {
      requests.push_back(make_secret_request("s3_endpoint", *args_.s3_endpoint,
                                             s3_endpoint, dh));
    }
    if (args_.token) {
      requests.push_back(make_secret_request("token", *args_.token, token, dh));
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
    if (not token.empty()) {
      config.properties["rest.auth.type"] = "oauth2";
      config.properties["token"] = std::move(token);
    }
    if (auth->credentials) {
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
      if (not creds.region.empty()) {
        config.properties["s3.region"] = creds.region;
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
      // After a restore, an existing table is our own previous work, not a
      // conflict; `mode="create"` resumes by appending.
      if (mode_ == mode::create and not restored) {
        diagnostic::error("Iceberg table `{}` already exists",
                          args_.table_id.inner)
          .primary(args_.table_id.source)
          .note("use `mode=\"create_append\"` or `mode=\"append\"` to write "
                "into an existing table")
          .emit(dh);
        done_ = true;
        co_return;
      }
      set_table(std::move(*table), ctx);
      if (done_) {
        co_return;
      }
      if (args_.partition_by) {
        // The existing table's partition spec governs the fanout; a supplied
        // `partition_by` must match it so that the user's expectation and
        // reality do not diverge silently.
        auto checked = co_await spawn_blocking(
          [table = *table_, fields = partition_fields_]() mutable {
            return table.check_partition_spec(fields);
          });
        if (not checked) {
          diagnostic::error("{}", checked.error().message)
            .primary(args_.partition_by->get_location())
            .note("the partition spec of an existing table cannot be changed "
                  "by this operator; drop `partition_by` to append to the "
                  "table as-is")
            .emit(dh);
          done_ = true;
          co_return;
        }
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
    if (not epoch_snapshot_.empty()) {
      diagnostic::error("Iceberg table `{}` no longer exists, but the "
                        "checkpoint references {} uncommitted data files",
                        args_.table_id.inner, epoch_snapshot_.size())
        .primary(args_.table_id.source)
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
    auto groups = co_await spawn_blocking(
      [table = *table_,
       batch = *batch]() mutable -> Result<std::vector<PartitionGroup>> {
        auto c_array = ArrowArray{};
        if (auto status = arrow::ExportArray(*batch, &c_array);
            not status.ok()) {
          return std::unexpected{Error{
            Error::Kind::permanent,
            fmt::format("failed to export batch: {}", status.ToString()),
          }};
        }
        return table.split_by_partition(&c_array);
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
    co_await co_match(msg, [&](RotateRequested& r) -> Task<void> {
      auto it = writers_.find(r.partition);
      if (it == writers_.end() or it->second.generation != r.generation) {
        co_return;
      }
      co_await close_partition(r.partition, ctx);
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
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    // The first checkpoint switches commits from per-rotation to
    // checkpoint-aligned: files close here, their handles persist in
    // `snapshot`, and `post_commit` appends them once the checkpoint is
    // durable. Committing before durability would duplicate the data when a
    // crash replays input from the previous checkpoint.
    checkpointing_ = true;
    co_await close_all(ctx);
    if (done_) {
      co_return;
    }
    for (auto& staged : pending_files_) {
      epoch_snapshot_.push_back(staged.serialized);
      epoch_files_.push_back(std::move(staged));
    }
    pending_files_.clear();
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
    auto c_schema = ArrowSchema{};
    if (auto result = table.export_arrow_schema(&c_schema); not result) {
      diagnostic::error("failed to map Iceberg table schema: {}",
                        result.error().message)
        .primary(args_.table_id.source)
        .emit(ctx.dh());
      done_ = true;
      return;
    }
    auto imported = arrow::ImportSchema(&c_schema);
    if (not imported.ok()) {
      diagnostic::error("failed to import Iceberg table schema: {}",
                        imported.status().ToString())
        .primary(args_.table_id.source)
        .emit(ctx.dh());
      done_ = true;
      return;
    }
    table_ = std::move(table);
    target_schema_ = imported.MoveValueUnsafe();
  }

  /// Creates the table from the schema of the first arriving events. Only
  /// reached in the create modes when the table did not exist at start.
  auto ensure_table(const table_slice& input, OpCtx& ctx) -> Task<void> {
    const auto& schema = as<record_type>(input.schema());
    auto options = CreateTableOptions{};
    options.partition_by = partition_fields_;
    for (const auto& field : schema.fields()) {
      if (field.name == default_sort_column and is<time_type>(field.type)) {
        options.sort_column = std::string{default_sort_column};
        break;
      }
    }
    auto dropped = std::make_shared<std::vector<std::string>>();
    auto table = co_await spawn_blocking(
      [catalog = *catalog_, ns = ns_, name = table_name_, ty = input.schema(),
       options = std::move(options), dropped,
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
    set_table(std::move(*table), ctx);
  }

  /// Evolves the table schema to cover the input: fields the table does not
  /// have yet become new columns, and existing columns too narrow for the
  /// input widen where the spec allows it (a metadata-only commit). Runs
  /// once per distinct input schema. The schema commit lands at the catalog
  /// *before* any data file carries the new columns, so Parquet files are
  /// only ever stamped with field IDs the catalog has confirmed. When a concurrent writer updates the table
  /// underneath us, the commit conflicts; we reload the table and re-derive
  /// the diff against their changes.
  auto ensure_schema(const table_slice& input, OpCtx& ctx) -> Task<void> {
    auto fingerprint = input.schema().make_fingerprint();
    if (evolved_schemas_.contains(fingerprint)) {
      co_return;
    }
    // Evolution retries run against a local handle so that the operator's
    // projection target and open data file stay untouched unless the table
    // schema actually changes.
    auto current = *table_;
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
          // The projection target changes: open data files stay valid under
          // the old schema, but must not receive new-schema batches. Close
          // them, then adopt the evolved schema from the update response.
          co_await close_all(ctx);
          if (done_) {
            co_return;
          }
          set_table(std::move(**evolved), ctx);
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
        evolved_schemas_.insert(std::move(fingerprint));
        co_return;
      }
      if (evolved.error().kind != Error::Kind::conflict
          or attempt >= commit_max_attempts) {
        fail(evolved.error(),
             fmt::format("failed to evolve schema of Iceberg table `{}`",
                         args_.table_id.inner),
             ctx);
        co_return;
      }
      diagnostic::warning("conflicting schema update (attempt {}/{}): {}",
                          attempt, commit_max_attempts, evolved.error().message)
        .primary(args_.operator_location)
        .emit(ctx.dh());
      // A concurrent writer updated the table between our load and the
      // schema commit; re-derive the diff against their changes.
      auto reloaded = co_await spawn_blocking(
        [catalog = *catalog_, ns = ns_, name = table_name_]() mutable {
          return catalog.load_table(ns, name);
        });
      if (not reloaded) {
        fail(reloaded.error(),
             fmt::format("failed to reload Iceberg table `{}`",
                         args_.table_id.inner),
             ctx);
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
  /// into struct columns line up by name, not by position.
  auto project_column(std::shared_ptr<arrow::Array> source,
                      const std::shared_ptr<arrow::Field>& target,
                      const std::string& path, int64_t rows, OpCtx& ctx)
    -> std::shared_ptr<arrow::Array> {
    auto null_column = [&] {
      return check(arrow::MakeArrayOfNull(target->type(), rows));
    };
    if (source) {
      source = normalize(std::move(source), ctx);
    }
    if (not source) {
      return null_column();
    }
    switch (target->type()->id()) {
      case arrow::Type::STRUCT: {
        auto struct_source
          = std::dynamic_pointer_cast<arrow::StructArray>(source);
        if (not struct_source) {
          warn_once(fmt::format("column `{}` has type `{}` but the table "
                                "expects `{}`; writing nulls instead",
                                path, source->type()->ToString(),
                                target->type()->ToString()),
                    ctx);
          return null_column();
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
          children.push_back(project_column(
            std::move(child), field, fmt::format("{}.{}", path, field->name()),
            rows, ctx));
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
        return check(arrow::StructArray::Make(children, target_fields,
                                              std::move(bitmap), null_count));
      }
      case arrow::Type::LIST: {
        auto list_source = std::dynamic_pointer_cast<arrow::ListArray>(source);
        if (not list_source) {
          warn_once(fmt::format("column `{}` has type `{}` but the table "
                                "expects `{}`; writing nulls instead",
                                path, source->type()->ToString(),
                                target->type()->ToString()),
                    ctx);
          return null_column();
        }
        // The values child covers the array's entire buffer range, so the
        // original offsets remain valid for the converted values.
        const auto& element = target->type()->field(0);
        auto values
          = project_column(list_source->values(), element, path + "[]",
                           list_source->values()->length(), ctx);
        return std::make_shared<arrow::ListArray>(
          arrow::list(element), list_source->length(),
          list_source->data()->buffers[1], std::move(values),
          list_source->null_bitmap(), list_source->data()->null_count,
          list_source->offset());
      }
      default: {
        if (source->type()->Equals(target->type())) {
          return source;
        }
        auto options = arrow::compute::CastOptions::Safe(target->type());
        options.allow_time_truncate = true;
        auto cast = arrow::compute::Cast(source, options);
        if (not cast.ok()) {
          warn_once(fmt::format("column `{}` has type `{}` but the table "
                                "expects `{}`; writing nulls instead",
                                path, source->type()->ToString(),
                                target->type()->ToString()),
                    ctx);
          return null_column();
        }
        return cast->make_array();
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
      arrays.push_back(
        project_column(std::move(column), field, field->name(), rows, ctx));
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

  /// Writes one partition group of a projected batch to the partition's
  /// open data file, opening it (and evicting the largest open file when at
  /// the partition cap) as needed. Rotates and commits when the file
  /// reaches `max_size`.
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
    auto it = writers_.find(group.key);
    if (it == writers_.end()) {
      if (writers_.size() >= max_open_partitions) {
        warn_once(fmt::format("exceeded {} open partitions; closing the "
                              "largest "
                              "data file early, which may produce small files",
                              max_open_partitions),
                  ctx);
        auto victim = std::ranges::max_element(writers_, std::less{},
                                               [](const auto& entry) {
                                                 return entry.second.bytes;
                                               });
        co_await close_partition(victim->first, ctx);
        if (done_) {
          co_return;
        }
      }
      auto writer = co_await spawn_blocking(
        [table = *table_, partition = group.partition]() mutable {
          return table.new_file_writer(partition);
        });
      if (not writer) {
        fail(writer.error(), "failed to open data file writer", ctx);
        co_return;
      }
      it = writers_
             .try_emplace(group.key,
                          PartitionWriter{
                            .writer = std::move(*writer),
                            .path = group.path,
                            .bytes = 0,
                            .generation = ++writer_generation_,
                            .timer_cancel = {},
                          })
             .first;
      arm_rotation_timer(group.key, it->second, ctx);
    }
    auto& partition_writer = it->second;
    const auto rows = sub->length();
    auto written = co_await spawn_blocking(
      [writer = partition_writer.writer,
       sub = std::move(sub)]() mutable -> Result<int64_t> {
        auto c_array = ArrowArray{};
        if (auto status = arrow::ExportArray(*sub, &c_array); not status.ok()) {
          return std::unexpected{Error{
            Error::Kind::permanent,
            fmt::format("failed to export batch: {}", status.ToString()),
          }};
        }
        if (auto result = writer.write(&c_array); not result) {
          return std::unexpected{result.error()};
        }
        return writer.bytes_written();
      });
    if (not written) {
      fail(written.error(), "failed to write data file", ctx);
      co_return;
    }
    events_counter_.add(rows);
    if (*written > partition_writer.bytes) {
      bytes_counter_.add(*written - partition_writer.bytes);
    }
    partition_writer.bytes = *written;
    const auto max_size
      = args_.max_size ? args_.max_size->inner : default_max_size;
    if (std::cmp_greater_equal(partition_writer.bytes, max_size)) {
      co_await close_partition(group.key, ctx);
      if (not done_ and not checkpointing_) {
        co_await commit_staged(pending_files_, ctx);
      }
    }
  }

  /// Closes the given partition's open data file and stages it for the next
  /// commit. Does not commit itself, so an eviction cascade under high
  /// partition cardinality cannot turn into a commit storm.
  auto close_partition(const std::string& key, OpCtx& ctx) -> Task<void> {
    auto node = writers_.extract(key);
    if (node.empty() or done_) {
      co_return;
    }
    auto& partition_writer = node.mapped();
    partition_writer.timer_cancel.requestCancellation();
    auto file
      = co_await spawn_blocking([writer = partition_writer.writer]() mutable {
          return writer.finish();
        });
    if (not file) {
      fail(file.error(), "failed to finalize data file", ctx);
      co_return;
    }
    auto serialized = file->serialize();
    if (not serialized) {
      fail(serialized.error(), "failed to persist data file handle", ctx);
      co_return;
    }
    pending_files_.push_back(StagedFile{
      .file = std::move(*file),
      .serialized = std::move(*serialized),
    });
  }

  /// Closes all open data files, staging them for the next commit.
  auto close_all(OpCtx& ctx) -> Task<void> {
    auto keys = std::vector<std::string>{};
    keys.reserve(writers_.size());
    for (const auto& [key, unused] : writers_) {
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
    if (epoch_snapshot_.empty()) {
      co_return;
    }
    if (table_->has_commit(CommitTag{writer_id_, commit_seq_})) {
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
    auto files = std::vector<DataFile>{};
    files.reserve(staged.size());
    for (const auto& entry : staged) {
      files.push_back(entry.file);
    }
    const auto tag = CommitTag{writer_id_, commit_seq_};
    auto backoff = duration{commit_initial_backoff};
    for (auto attempt = 1;; ++attempt) {
      auto result
        = co_await spawn_blocking([table = *table_, files, tag]() mutable {
            return table.commit_append(files, tag);
          });
      if (result) {
        staged.clear();
        commit_seq_ += 1;
        set_table(std::move(*result), ctx);
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
          fail(reloaded.error(), "failed to reload Iceberg table", ctx);
          co_return;
        }
        const auto landed = reloaded->has_commit(tag);
        if (landed) {
          staged.clear();
          commit_seq_ += 1;
        }
        set_table(std::move(*reloaded), ctx);
        if (done_ or landed) {
          co_return;
        }
        continue;
      }
      co_await sleep_for(backoff);
      backoff *= 2;
    }
  }

  /// Spawns a timer that requests rotation of the partition's open file
  /// after `timeout`. Cancelled when the file rotates for another reason.
  auto arm_rotation_timer(const std::string& key, PartitionWriter& writer,
                          OpCtx& ctx) -> void {
    const auto timeout
      = args_.timeout ? args_.timeout->inner : duration{default_timeout};
    std::ignore = ctx.spawn_task(
      [this, key, generation = writer.generation, timeout,
       token = writer.timer_cancel.getToken()]() -> Task<void> {
        auto merged = folly::cancellation_token_merge(
          co_await folly::coro::co_current_cancellation_token, token);
        co_await folly::coro::co_withCancellation(merged, sleep_for(timeout));
        control_queue_->enqueue(RotateRequested{key, generation});
      });
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
  /// Open data files, keyed by `PartitionGroup::key`.
  std::unordered_map<std::string, PartitionWriter> writers_;
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
  /// Sequence number of the next commit; each value commits at most once.
  uint64_t commit_seq_ = 0;
  /// Whether checkpoints drive commits. Without checkpoints, every rotation
  /// commits directly (there is no replay to guard against); the first
  /// checkpoint switches to one commit per checkpoint, which `post_commit`
  /// issues after the checkpoint is durable.
  bool checkpointing_ = false;
  int64_t writer_generation_ = 0;
  bool done_ = false;
  std::unordered_set<std::string> warned_;
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
    d.named("aws_iam", &ToIcebergArgs::aws_iam);
    auto endpoint_arg = d.named("s3_endpoint", &ToIcebergArgs::s3_endpoint);
    auto path_style_arg
      = d.named("s3_path_style", &ToIcebergArgs::s3_path_style);
    d.named("token", &ToIcebergArgs::token);
    auto partition_by_arg
      = d.named("partition_by", &ToIcebergArgs::partition_by, "list<any>");
    auto max_size_arg = d.named("max_size", &ToIcebergArgs::max_size);
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
