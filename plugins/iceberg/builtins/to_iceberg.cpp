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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/cast.h>
#include <folly/coro/UnboundedQueue.h>

#include <string>
#include <unordered_set>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

constexpr auto default_max_size = uint64_t{512} * 1024 * 1024;
constexpr auto default_timeout = std::chrono::minutes{15};
constexpr auto commit_max_attempts = 5;
constexpr auto commit_initial_backoff = std::chrono::milliseconds{250};

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

class ToIceberg final : public Operator<table_slice, void> {
public:
  struct RotateRequested {
    int64_t generation;
  };
  using Message = variant<RotateRequested>;

  explicit ToIceberg(ToIcebergArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
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
      if (mode_ == mode::create) {
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
    auto batch = project(std::move(input), ctx);
    if (not batch) {
      co_return;
    }
    if (not writer_) {
      auto writer = co_await spawn_blocking([table = *table_]() mutable {
        return table.new_file_writer();
      });
      if (not writer) {
        fail(writer.error(), "failed to open data file writer", ctx);
        co_return;
      }
      writer_ = std::move(*writer);
      arm_rotation_timer(ctx);
    }
    const auto rows = (*batch)->length();
    auto written = co_await spawn_blocking(
      [writer = *writer_, batch
                          = std::move(*batch)]() mutable -> Result<int64_t> {
        auto c_array = ArrowArray{};
        if (auto status = arrow::ExportArray(*batch, &c_array);
            not status.ok()) {
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
    if (*written > file_bytes_) {
      bytes_counter_.add(*written - file_bytes_);
    }
    file_bytes_ = *written;
    const auto max_size
      = args_.max_size ? args_.max_size->inner : default_max_size;
    if (std::cmp_greater_equal(file_bytes_, max_size)) {
      co_await rotate(ctx);
    }
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await control_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto msg = std::move(result).as<Message>();
    co_await co_match(msg, [&](RotateRequested& r) -> Task<void> {
      if (r.generation == writer_generation_ and writer_) {
        co_await rotate(ctx);
      }
    });
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    // Flush and commit so that a checkpoint acknowledges only data that is
    // visible in the table. Delivery stays at-least-once: a crash between
    // commit and checkpoint yields duplicates on restart, never data loss.
    co_await rotate(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await rotate(ctx);
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  /// Imports the table's Arrow schema and makes `table` the write target.
  auto set_table(Table table, OpCtx& ctx) -> void {
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
    for (const auto& field : schema.fields()) {
      if (field.name == default_sort_column and is<time_type>(field.type)) {
        options.sort_column = std::string{default_sort_column};
        break;
      }
    }
    auto dropped = std::make_shared<std::vector<std::string>>();
    auto table = co_await spawn_blocking(
      [catalog = *catalog_, ns = ns_, name = table_name_,
       ty = input.schema(), options = std::move(options), dropped,
       fall_back_to_load = mode_ == mode::create_append]() mutable
      -> Result<Table> {
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
                 t, std::static_pointer_cast<enumeration_type::array_type>(
                      array))
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
                                  "table and will be dropped; schema "
                                  "evolution arrives with a future release",
                                  path, field->name()),
                      ctx);
          }
        }
        // A fresh validity bitmap sidesteps offset bookkeeping for sliced
        // source arrays.
        auto bitmap = std::shared_ptr<arrow::Buffer>{};
        auto null_count = struct_source->null_count();
        if (null_count > 0) {
          auto valid
            = check(arrow::compute::IsValid(struct_source)).array();
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
      arrays.push_back(project_column(std::move(column), field, field->name(),
                                      rows, ctx));
    }
    for (const auto& field : batch->schema()->fields()) {
      if (not used.contains(field->name())) {
        warn_once(fmt::format("column `{}` does not exist in the table and "
                              "will be dropped; schema evolution arrives with "
                              "a future release",
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

  /// Closes the current data file and commits it as one FastAppend snapshot,
  /// retrying transient failures with exponential backoff.
  auto rotate(OpCtx& ctx) -> Task<void> {
    if (not writer_ or done_) {
      co_return;
    }
    auto writer = std::move(*writer_);
    writer_ = None{};
    file_bytes_ = 0;
    ++writer_generation_;
    rotation_cancel_.requestCancellation();
    auto file = co_await spawn_blocking([writer]() mutable {
      return writer.finish();
    });
    if (not file) {
      fail(file.error(), "failed to finalize data file", ctx);
      co_return;
    }
    auto backoff = duration{commit_initial_backoff};
    for (auto attempt = 1;; ++attempt) {
      auto result
        = co_await spawn_blocking([table = *table_, file = *file]() mutable {
            auto files = std::vector<DataFile>{std::move(file)};
            return table.commit_append(files);
          });
      if (result) {
        break;
      }
      if (result.error().kind != Error::Kind::transient
          or attempt >= commit_max_attempts) {
        fail(result.error(), "failed to commit snapshot", ctx);
        co_return;
      }
      diagnostic::warning("transient commit failure (attempt {}/{}): {}",
                          attempt, commit_max_attempts, result.error().message)
        .primary(args_.operator_location)
        .emit(ctx.dh());
      co_await sleep_for(backoff);
      backoff *= 2;
    }
  }

  /// Spawns a timer that requests rotation of the current file after
  /// `timeout`. Cancelled when the file rotates for another reason.
  auto arm_rotation_timer(OpCtx& ctx) -> void {
    rotation_cancel_ = folly::CancellationSource{};
    const auto timeout
      = args_.timeout ? args_.timeout->inner : duration{default_timeout};
    std::ignore
      = ctx.spawn_task([this, generation = writer_generation_, timeout,
                        token = rotation_cancel_.getToken()]() -> Task<void> {
          auto merged = folly::cancellation_token_merge(
            co_await folly::coro::co_current_cancellation_token, token);
          co_await folly::coro::co_withCancellation(merged, sleep_for(timeout));
          control_queue_->enqueue(RotateRequested{generation});
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
  Option<FileWriter> writer_;
  int64_t file_bytes_ = 0;
  int64_t writer_generation_ = 0;
  folly::CancellationSource rotation_cancel_;
  bool done_ = false;
  std::unordered_set<std::string> warned_;
  MetricsCounter events_counter_;
  MetricsCounter bytes_counter_;
  mutable Box<folly::coro::UnboundedQueue<Message>> control_queue_{
    std::in_place,
  };
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
