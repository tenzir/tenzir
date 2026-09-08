//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/arguments.hpp"
#include "clickhouse/block_to_table_slice.hpp"
#include "clickhouse/easy_client.hpp"
#include "clickhouse/sql_pushdown.hpp"
#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tql2/filter.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <clickhouse/client.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>

#include <algorithm>
#include <unordered_set>
#include <utility>

namespace tenzir::plugins::clickhouse {

namespace {

constexpr auto clickhouse_plaintext_port = uint64_t{9000};
constexpr auto clickhouse_tls_port = uint64_t{9440};

struct FromClickhouseArgs {
  located<secret> uri = {secret::make_literal(""), location::unknown};
  Option<located<std::string>> table;
  located<secret> host = {secret::make_literal("localhost"), location::unknown};
  Option<located<uint64_t>> port = None{};
  located<secret> user = {secret::make_literal("default"), location::unknown};
  located<secret> password = {secret::make_literal(""), location::unknown};
  Option<located<std::string>> sql;
  Option<located<data>> tls;
  location operator_location;
  /// The filter chain that follows the operator, moved into it by the
  /// optimizer. Every predicate must be applied: in SQL where the translation
  /// is exact, locally otherwise.
  ir::OptimizeFilter filter;
  /// An upper bound on the events needed after `filter`, if any.
  Option<uint64_t> limit;
  /// The fields needed downstream, or `None` for all of them.
  Option<ir::OptimizeProjection> projection;
};

struct QueryPlan {
  /// The table to read from; `None` when the user provided `sql`.
  Option<std::string> table;
  /// The user-provided SQL; empty when `table` is set.
  std::string sql;
  std::string schema_name;
  ir::OptimizeFilter filter;
  Option<uint64_t> limit;
  Option<ir::OptimizeProjection> projection;
};

/// Announces the predicates that the operator evaluates itself. Sent before
/// the first slice so that `process_task` never sees data it cannot filter.
struct PlanMessage {
  ir::OptimizeFilter local_filter;
};

struct SliceMessage {
  table_slice slice;
};

struct DoneMessage {};

using Message = variant<PlanMessage, SliceMessage, DoneMessage>;
using MessageQueue = folly::coro::BoundedQueue<Message, true, true>;
constexpr auto message_queue_capacity = uint32_t{16};
constexpr auto message_queue_backoff = std::chrono::milliseconds{1};

auto has_primary_annotation(diagnostic const& diag) -> bool {
  return std::any_of(diag.annotations.begin(), diag.annotations.end(),
                     [](auto const& annotation) {
                       return annotation.primary;
                     });
}

struct RuntimeState {
  RuntimeState() : queue{message_queue_capacity} {
  }

  MessageQueue queue;
  Option<type> first_schema;
  Atomic<bool> stop_requested = false;

  auto produce_plan(ir::OptimizeFilter local_filter) -> void {
    produce(PlanMessage{std::move(local_filter)});
  }

  auto produce_data(table_slice slice) -> void {
    produce(SliceMessage{std::move(slice)});
  }

  /// Enqueues from the blocking query thread, giving up once downstream has
  /// declared that it needs no more data.
  auto produce(Message msg) -> void {
    while (not stop_requested.load(std::memory_order_acquire)) {
      if (queue.try_enqueue(msg)) {
        return;
      }
      std::this_thread::sleep_for(message_queue_backoff);
    }
  }

  auto produce_end_of_data() -> Task<void> {
    co_await queue.enqueue(DoneMessage{});
  }

  auto should_cancel() const -> bool {
    return stop_requested.load(std::memory_order::acquire);
  }

  auto request_cancellation() -> void {
    stop_requested.store(true, std::memory_order_release);
    while (queue.try_dequeue()) {
      // Drop buffered slices; downstream has already declared it needs no more.
    }
  }
};

class FromClickhouse final : public Operator<void, table_slice> {
public:
  FromClickhouse() = default;

  explicit FromClickhouse(FromClickhouseArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto ssl_opts = args_.tls ? tls_options{*args_.tls} : tls_options{};
    auto ssl = ssl_opts.resolve(ctx.actor_system().config(), ctx.dh());
    if (not ssl) {
      co_return;
    }
    tls_enabled_ = ssl->tls.inner;
    auto const default_port
      = tls_enabled_ ? clickhouse_tls_port : clickhouse_plaintext_port;
    auto client_args = easy_client::arguments{
      .host = "",
      .port = args_.port ? *args_.port
                         : located<uint64_t>{default_port, location::unknown},
      .user = "default",
      .password = "",
      .default_database = None{},
      .ssl = std::move(*ssl),
      .table = {},
      .mode = {mode::append, location::unknown},
      .primary = None{},
      .operator_location = args_.operator_location,
    };
    auto uri = std::string{};
    auto requests = std::vector<secret_request>{};
    auto has_uri = args_.uri.inner != secret::make_literal("");
    if (has_uri) {
      requests.push_back(make_secret_request("uri", args_.uri, uri, ctx.dh()));
    } else {
      requests.push_back(
        make_secret_request("host", args_.host, client_args.host, ctx.dh()));
      requests.push_back(
        make_secret_request("user", args_.user, client_args.user, ctx.dh()));
      requests.push_back(make_secret_request("password", args_.password,
                                             client_args.password, ctx.dh()));
    }
    auto ok = co_await ctx.resolve_secrets(std::move(requests));
    if (not ok) {
      co_return;
    }
    if (has_uri) {
      auto parsed = parse_connection_uri(uri, args_.uri.source, ctx.dh());
      if (not parsed) {
        co_return;
      }
      apply_connection_uri(client_args, *parsed);
      if (not parsed->has_port()) {
        client_args.port = located<uint64_t>{default_port, location::unknown};
      }
    }
    auto options = client_args.make_options();
    // The query text is only decided on the query thread: with optimizer hints
    // in table mode, it depends on the table's schema.
    auto plan = QueryPlan{
      .table = None{},
      .sql = {},
      .schema_name = "clickhouse.query",
      .filter = args_.filter,
      .limit = args_.limit,
      .projection = args_.projection,
    };
    if (args_.table) {
      plan.table = args_.table->inner;
      plan.schema_name = make_schema_name_from_table(args_.table->inner);
    } else {
      plan.sql = args_.sql->inner;
    }
    // Helper task to shutdown our query on cancellation.
    ctx.spawn_task([runtime = runtime_]() mutable -> Task<void> {
      if (not co_await catch_cancellation(wait_forever())) {
        runtime->request_cancellation();
      }
    });
    // The actual query task.
    ctx.spawn_task([this, options = std::move(options), plan = std::move(plan),
                    &dh = ctx.dh(),
                    loc = args_.operator_location]() mutable -> Task<void> {
      auto transformed_dh = transforming_diagnostic_handler{
        dh, [loc](diagnostic diag) {
          if (not has_primary_annotation(diag)) {
            diag.annotations.emplace_back(true, std::string{}, loc);
          }
          return diag;
        }};
      co_await run_query(std::move(options), std::move(plan), transformed_dh);
    });
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await runtime_->queue.dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](PlanMessage x) -> Task<void> {
        local_filter_ = std::move(x.local_filter);
        co_return;
      },
      [&](SliceMessage x) -> Task<void> {
        if (runtime_->stop_requested.load(std::memory_order_acquire)) {
          co_return;
        }
        auto slice = std::move(x.slice);
        // Predicates without an exact SQL translation run here, with the same
        // semantics as the `where` they came from.
        for (auto const& expr : local_filter_) {
          slice = filter2(slice, expr, ctx.dh(), false);
          if (slice.rows() == 0) {
            co_return;
          }
        }
        // The limit counts events after the filter chain. When it went into
        // the SQL query this is a no-op; otherwise it ends the query early.
        if (args_.limit) {
          auto remaining = *args_.limit - emitted_;
          if (slice.rows() > remaining) {
            slice = subslice(slice, 0, remaining);
          }
        }
        emitted_ += slice.rows();
        if (slice.rows() > 0) {
          co_await push(std::move(slice));
        }
        if (args_.limit and emitted_ >= *args_.limit) {
          runtime_->request_cancellation();
          done_ = true;
        }
      },
      [&](DoneMessage) -> Task<void> {
        done_ = true;
        co_return;
      });
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    runtime_->request_cancellation();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  static auto split_validated_table_name(std::string_view table)
    -> split_table_name_result {
    if (auto split = table_name_quoting.split_at_unquoted(table, '.')) {
      return {split->first, split->second};
    }
    return {None{}, table};
  }

  static auto make_schema_name_from_table(std::string_view table)
    -> std::string {
    auto split = split_validated_table_name(table);
    auto table_name = unquote_identifier_component(split.table);
    if (split.database) {
      return fmt::format("clickhouse.{}.{}",
                         unquote_identifier_component(*split.database),
                         table_name);
    }
    return fmt::format("clickhouse.{}", table_name);
  }

  /// Reads the columns of `table` that `SELECT *` returns.
  static auto fetch_schema(::clickhouse::Client& client, std::string_view table)
    -> SqlSchema {
    // Any privilege on the table grants `DESCRIBE`, so this cannot fail where
    // the `SELECT` that follows would succeed.
    auto query = ::clickhouse::Query{fmt::format("DESCRIBE TABLE {}", table)};
    auto schema = SqlSchema{};
    query.OnData([&](::clickhouse::Block const& block) {
      if (block.GetColumnCount() < 3) {
        return;
      }
      auto names = block[0]->As<::clickhouse::ColumnString>();
      auto types = block[1]->As<::clickhouse::ColumnString>();
      auto default_types = block[2]->As<::clickhouse::ColumnString>();
      if (not names or not types or not default_types) {
        return;
      }
      for (auto i = size_t{0}; i < block.GetRowCount(); ++i) {
        // Whether `SELECT *` returns a generated column depends on the
        // session's `asterisk_include_*` settings, so the schema keeps them
        // apart: predicates on them stay local and projections that name them
        // fall back to `*`, which matches the local result under any setting.
        auto default_type = default_types->At(i);
        auto generated = default_type == "ALIAS"
                         or default_type == "MATERIALIZED"
                         or default_type == "EPHEMERAL";
        schema.add_column(names->At(i), types->At(i), generated);
      }
    });
    client.Execute(query);
    return schema;
  }

  struct PreparedQuery {
    std::string text;
    ir::OptimizeFilter local_filter;
  };

  /// Decides the query text and which predicates stay local. Runs on the
  /// query thread because it may need a round-trip for the schema.
  static auto prepare_query(::clickhouse::Client& client, QueryPlan& plan)
    -> PreparedQuery {
    if (not plan.table) {
      // TODO: Weaving hints into user-provided SQL requires parsing it. Until
      // then, everything runs locally.
      return {.text = std::move(plan.sql),
              .local_filter = std::move(plan.filter)};
    }
    // A limit alone needs no schema; only filter and projection do.
    if (plan.filter.empty() and not plan.projection) {
      return {
        .text = make_select_query(*plan.table, nullptr, None{}, {}, plan.limit),
        .local_filter = {},
      };
    }
    auto schema = fetch_schema(client, *plan.table);
    auto split = split_filter_for_sql(std::move(plan.filter), schema);
    // The limit counts events after the whole filter chain, so it can only go
    // into the query if the chain did.
    auto limit = Option<uint64_t>{};
    if (split.remaining.empty()) {
      limit = plan.limit;
    }
    return {
      .text = make_select_query(*plan.table, &schema, plan.projection,
                                split.pushed, limit),
      .local_filter = std::move(split.remaining),
    };
  }

  auto run_query(::clickhouse::ClientOptions options, QueryPlan plan,
                 diagnostic_handler& dh) -> Task<void> {
    try {
      auto first_schema = Option<type>{};
      auto on_data = [&](::clickhouse::Block const& block) {
        if (runtime_->should_cancel()) {
          return false;
        }
        auto slice = block_to_table_slice(block, plan.schema_name, dh);
        if (not slice) {
          return not runtime_->should_cancel();
        }
        if (not first_schema) {
          first_schema = slice->schema();
        } else if (slice->schema() != *first_schema) {
          diagnostic::warning(
            "ClickHouse query schema changed during execution")
            .hint("continuing to emit data with the new schema")
            .emit(dh);
        }
        runtime_->produce_data(std::move(*slice));
        return not runtime_->should_cancel();
      };
      co_await spawn_blocking([&]() {
        auto client = ::clickhouse::Client{options};
        auto prepared = prepare_query(client, plan);
        TENZIR_DEBUG("from_clickhouse runs `{}`", prepared.text);
        runtime_->produce_plan(std::move(prepared.local_filter));
        auto query = ::clickhouse::Query{std::move(prepared.text)};
        query.SetSetting("max_block_size",
                         {std::to_string(defaults::import::table_slice_size),
                          ::clickhouse::QuerySettingsField::IMPORTANT});
        // Without this, MergeTree's byte-based cap overrides max_block_size
        // on wide tables.
        query.SetSetting("preferred_block_size_bytes",
                         {"0", ::clickhouse::QuerySettingsField::IMPORTANT});
        // `block_to_table_slice` cannot decode ClickHouse's native `JSON`
        // column type; this makes the server send such columns as plain
        // strings instead, matching how `to_clickhouse` writes JSON columns.
        query.SetSetting("output_format_native_write_json_as_string",
                         {"1", ::clickhouse::QuerySettingsField::IMPORTANT});
        query.OnDataCancelable(on_data);
        client.Select(query);
      });
    } catch (const panic_exception&) {
      throw;
    } catch (const ::clickhouse::ServerError& e) {
      if (not runtime_->stop_requested.load(std::memory_order_acquire)) {
        add_tls_client_diagnostic_hints(
          diagnostic::error("ClickHouse error {}: {}", e.GetCode(), e.what()),
          tls_enabled_, "ClickHouse", clickhouse_plaintext_port,
          clickhouse_tls_port)
          .emit(dh);
      }
    } catch (const std::exception& e) {
      if (not runtime_->stop_requested.load(std::memory_order_acquire)) {
        add_tls_client_diagnostic_hints(
          diagnostic::error("ClickHouse error: {}", e.what()), tls_enabled_,
          "ClickHouse", clickhouse_plaintext_port, clickhouse_tls_port)
          .emit(dh);
      }
    }
    co_await runtime_->produce_end_of_data();
  }

  FromClickhouseArgs args_;
  mutable Arc<RuntimeState> runtime_ = Arc<RuntimeState>{std::in_place};
  bool tls_enabled_ = false;
  bool done_ = false;
  ir::OptimizeFilter local_filter_;
  uint64_t emitted_ = 0;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_clickhouse";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromClickhouseArgs, FromClickhouse>{};
    auto uri_arg = d.named_optional("uri", &FromClickhouseArgs::uri);
    auto table_arg = d.named("table", &FromClickhouseArgs::table);
    auto host_arg = d.named_optional("host", &FromClickhouseArgs::host);
    auto port_arg = d.named("port", &FromClickhouseArgs::port);
    auto user_arg = d.named_optional("user", &FromClickhouseArgs::user);
    auto password_arg
      = d.named_optional("password", &FromClickhouseArgs::password);
    auto sql_arg = d.named("sql", &FromClickhouseArgs::sql);
    auto tls_validate
      = tls_options{}.add_to_describer(d, &FromClickhouseArgs::tls);
    d.operator_location(&FromClickhouseArgs::operator_location);
    d.validate([=, tls_validate
                   = std::move(tls_validate)](DescribeCtx& ctx) -> Empty {
      tls_validate(ctx);
      auto has_table = ctx.get(table_arg).has_value();
      auto has_sql = ctx.get(sql_arg).has_value();
      auto has_uri = ctx.get(uri_arg).has_value();
      auto has_host = ctx.get(host_arg).has_value();
      auto has_port = ctx.get(port_arg).has_value();
      auto has_user = ctx.get(user_arg).has_value();
      auto has_password = ctx.get(password_arg).has_value();
      if (has_uri and (has_host or has_port or has_user or has_password)) {
        diagnostic::error(
          "`uri` and explicit connection arguments are mutually exclusive")
          .primary(ctx.get_location(uri_arg).value_or(location::unknown))
          .emit(ctx);
        return {};
      }
      if (not has_table and not has_sql) {
        diagnostic::error("no query specified")
          .hint("specify `table` or `sql`")
          .emit(ctx);
        return {};
      }
      if (has_sql and has_table) {
        diagnostic::error("`sql` and `table` are mutually exclusive").emit(ctx);
        return {};
      }
      if (auto port = ctx.get(port_arg)) {
        if (port->inner == 0 or port->inner > 65535) {
          diagnostic::error("`port` must be between 1 and 65535")
            .primary(port->source, "got `{}`", port->inner)
            .emit(ctx);
        }
      }
      if (auto table = ctx.get(table_arg)) {
        if (not validate_table_name<true>(table->inner, table->source, ctx)) {
          return {};
        }
      }
      return {};
    });
    d.optimize_limit(&FromClickhouseArgs::limit);
    d.optimize_projection(&FromClickhouseArgs::projection);
    return d.optimize_filter(&FromClickhouseArgs::filter);
  }
};

} // namespace

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::Plugin)
