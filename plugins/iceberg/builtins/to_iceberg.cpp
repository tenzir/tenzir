//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/facade.hpp"

#include <tenzir/any.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
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
    auto [ns, name] = split_table_id(args_.table_id.inner);
    auto table = co_await spawn_blocking(
      [config = std::move(config), ns = std::move(ns),
       name = std::move(name)]() mutable -> Result<Table> {
        auto catalog = Catalog::open(std::move(config));
        if (not catalog) {
          return std::unexpected{catalog.error()};
        }
        return catalog->load_table(ns, name);
      });
    if (not table) {
      auto diag = diagnostic::error("failed to open Iceberg table `{}`: {}",
                                    args_.table_id.inner, table.error().message)
                    .primary(args_.table_id.source);
      if (table.error().kind == Error::Kind::permanent) {
        diag = std::move(diag).note(
          "`to_iceberg` only appends to existing tables; table creation "
          "arrives with a future release");
      }
      std::move(diag).emit(dh);
      done_ = true;
      co_return;
    }
    auto c_schema = ArrowSchema{};
    if (auto result = table->export_arrow_schema(&c_schema); not result) {
      diagnostic::error("failed to map Iceberg table schema: {}",
                        result.error().message)
        .primary(args_.table_id.source)
        .emit(dh);
      done_ = true;
      co_return;
    }
    auto imported = arrow::ImportSchema(&c_schema);
    if (not imported.ok()) {
      diagnostic::error("failed to import Iceberg table schema: {}",
                        imported.status().ToString())
        .primary(args_.table_id.source)
        .emit(dh);
      done_ = true;
      co_return;
    }
    table_ = std::move(*table);
    target_schema_ = imported.MoveValueUnsafe();
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_ or input.rows() == 0) {
      co_return;
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
  /// Reorders and casts a slice's columns to the table schema. Missing
  /// columns are null-filled; unknown columns are dropped with a one-time
  /// warning per input schema.
  auto project(table_slice input, OpCtx& ctx)
    -> Option<std::shared_ptr<arrow::StructArray>> {
    input = resolve_enumerations(std::move(input));
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
      if (column and not column->type()->Equals(field->type())) {
        auto options = arrow::compute::CastOptions::Safe(field->type());
        options.allow_time_truncate = true;
        auto cast = arrow::compute::Cast(column, options);
        if (cast.ok()) {
          column = cast->make_array();
        } else {
          warn_once(fmt::format("column `{}` has type `{}` but the table "
                                "expects `{}`; writing nulls instead",
                                field->name(), column->type()->ToString(),
                                field->type()->ToString()),
                    ctx);
          column = nullptr;
        }
      }
      if (not column) {
        auto nulls = arrow::MakeArrayOfNull(field->type(), rows);
        if (not nulls.ok()) {
          diagnostic::error("failed to allocate null column: {}",
                            nulls.status().ToString())
            .emit(ctx.dh());
          done_ = true;
          return None{};
        }
        column = nulls.MoveValueUnsafe();
      }
      arrays.push_back(std::move(column));
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
      if (auto mode = ctx.get(mode_arg)) {
        if (mode->inner != "append") {
          diagnostic::error("only `mode=\"append\"` is supported; table "
                            "creation arrives with a future release")
            .primary(mode->source, "got `{}`", mode->inner)
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
