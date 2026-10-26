//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/arguments.hpp"
#include "clickhouse/block_to_events.hpp"
#include "clickhouse/block_to_table_slice.hpp"
#include "clickhouse/catch_all_reader.hpp"
#include "clickhouse/connection.hpp"
#include "clickhouse/easy_client.hpp"
#include "clickhouse/query_diagnostics.hpp"
#include "clickhouse/sql_pushdown.hpp"
#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/http.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova_flag.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/pushdown/read.hpp"
#include "tenzir/pushdown/translate.hpp"
#include "tenzir/tql2/filter.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/url/parse.hpp>
#include <clickhouse/client.h>
#include <fmt/ranges.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>

#include <algorithm>
#include <cctype>
#include <unordered_set>
#include <utility>

namespace tenzir::plugins::clickhouse {

namespace {

constexpr auto clickhouse_plaintext_port = uint64_t{9000};
constexpr auto clickhouse_tls_port = uint64_t{9440};

/// The user name that makes Hydrolix treat the password as an API token.
constexpr auto hydrolix_token_user = std::string_view{"__api_token__"};

/// The connection arguments of `from_hydrolix`, which replace the explicit
/// ones of `from_clickhouse`.
struct HydrolixConnectionArgs {
  StartupArgument<nova::Secret> url;
  StartupArgument<nova::Secret> token;
};

struct FromClickhouseArgs {
  Option<StartupArgument<nova::Secret>> uri;
  Option<located<std::string>> table;
  StartupArgument<nova::Secret> host
    = StartupArgument<nova::Secret>::constant("localhost");
  Option<located<uint64_t>> port = None{};
  StartupArgument<nova::Secret> user
    = StartupArgument<nova::Secret>::constant("default");
  StartupArgument<nova::Secret> password
    = StartupArgument<nova::Secret>::constant("");
  Option<located<std::string>> sql;
  Option<located<data>> tls;
  location operator_location;
  /// Apply every accepted predicate: in SQL where exact, locally otherwise.
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection, opt::Aggregation>
    optimization;
  /// Set when the operator runs as `from_hydrolix`. Not an argument of
  /// `from_clickhouse`, so only the conversion from `FromHydrolixArgs` sets it.
  Option<HydrolixConnectionArgs> hydrolix = None{};
};

struct FromHydrolixArgs {
  StartupArgument<nova::Secret> url;
  StartupArgument<nova::Secret> token;
  Option<located<std::string>> table;
  Option<located<std::string>> sql;
  Option<located<uint64_t>> port = None{};
  Option<located<data>> tls;
  location operator_location;
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection, opt::Aggregation>
    optimization;
};

/// Runs `from_hydrolix` as `from_clickhouse` against the cluster's native
/// ClickHouse interface.
auto to_clickhouse_args(FromHydrolixArgs args) -> FromClickhouseArgs {
  auto result = FromClickhouseArgs{};
  result.table = std::move(args.table);
  result.port = args.port;
  result.sql = std::move(args.sql);
  result.tls = std::move(args.tls);
  result.operator_location = args.operator_location;
  result.optimization = std::move(args.optimization);
  result.hydrolix = HydrolixConnectionArgs{
    .url = std::move(args.url),
    .token = std::move(args.token),
  };
  return result;
}

/// The names that tell users which backend an operator talks to.
struct BackendNames {
  std::string_view operator_name;
  std::string_view service;
  std::string_view schema_prefix;
};

auto backend_names(bool hydrolix) -> BackendNames {
  if (hydrolix) {
    return {"from_hydrolix", "Hydrolix", "hydrolix"};
  }
  return {"from_clickhouse", "ClickHouse", "clickhouse"};
}

/// Asks the server to compress the blocks it sends, which shrinks results that
/// cross a network at little CPU cost.
auto with_read_compression(::clickhouse::ClientOptions options)
  -> ::clickhouse::ClientOptions {
  options.SetCompressionMethod(::clickhouse::CompressionMethod::LZ4);
  return options;
}

auto sql_target(FromClickhouseArgs const& args) -> SqlTarget {
  return args.hydrolix.has_value() ? SqlTarget::hydrolix
                                   : SqlTarget::clickhouse;
}

/// Fails a checkpoint. A query has no cursor to resume from, so a restored
/// operator would run it again and repeat the events that earlier checkpoints
/// committed.
[[noreturn]] auto reject_checkpoint(FromClickhouseArgs const& args) -> void {
  diagnostic::error("{} does not support checkpoints yet",
                    backend_names(args.hydrolix.has_value()).operator_name)
    .primary(args.operator_location)
    .throw_();
}

/// Creates the counter of the events that the operator reads.
auto make_events_counter(bool hydrolix, OpCtx& ctx) -> MetricsCounter {
  auto label = hydrolix ? MetricsLabel{"operator", "from_hydrolix"}
                        : MetricsLabel{"operator", "from_clickhouse"};
  return ctx.make_counter(label, MetricsDirection::read,
                          MetricsVisibility::external_, MetricsUnit::events);
}

/// The native ClickHouse endpoint of a Hydrolix cluster.
struct HydrolixEndpoint {
  std::string host;
  uint16_t port = 0;
  std::string token;
  TlsConfig tls;
};

/// Derives the native endpoint from the cluster URL of `from_hydrolix`. The
/// URL names the cluster as in `to_hydrolix`, so only its scheme and host
/// matter: the scheme decides TLS, which in turn decides the default port.
auto resolve_hydrolix_endpoint(HydrolixConnectionArgs const& hydrolix,
                               Option<located<uint64_t>> const& port,
                               Option<located<data>> const& tls, OpCtx& ctx)
  -> Task<Option<HydrolixEndpoint>> {
  auto& dh = ctx.dh();
  auto evaluated_url = co_await hydrolix.url.evaluate_string(ctx, "url");
  if (not evaluated_url) {
    co_return None{};
  }
  auto url = std::move(*evaluated_url);
  auto evaluated_token = co_await hydrolix.token.evaluate_string(ctx, "token");
  if (not evaluated_token) {
    co_return None{};
  }
  auto token = std::move(*evaluated_token);
  if (token.empty()) {
    diagnostic::error("`token` must not be empty")
      .primary(hydrolix.token)
      .emit(dh);
    co_return None{};
  }
  // An explicit scheme states whether the connection must use TLS. It takes
  // the place of the `tls` argument, so that the node configuration cannot
  // turn TLS off for an `https://` URL and send the token in plain text.
  auto scheme_tls = Option<bool>{};
  auto const end = url.find("://");
  auto const is_scheme_char = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 or c == '+'
           or c == '-' or c == '.';
  };
  // A `://` after the authority, for example in the query, is no scheme.
  if (end != std::string::npos and end > 0
      and std::ranges::all_of(std::string_view{url}.substr(0, end),
                              is_scheme_char)) {
    auto scheme = detail::ascii_tolower(std::string_view{url}.substr(0, end));
    if (scheme != "http" and scheme != "https") {
      diagnostic::error("`url` must use the `http` or `https` scheme")
        .primary(hydrolix.url, "got `{}`", scheme)
        .emit(dh);
      co_return None{};
    }
    url.replace(0, end, scheme);
    scheme_tls = scheme == "https";
  }
  auto effective_tls = tls;
  if (not effective_tls and scheme_tls) {
    effective_tls
      = located<data>{data{*scheme_tls}, hydrolix.url.get_location()};
  }
  auto resolved = tls_options::from_optional(effective_tls)
                    .resolve(url, hydrolix.url.get_location(),
                             ctx.actor_system().config(), dh);
  if (not resolved) {
    co_return None{};
  }
  http::add_default_url_scheme(url, resolved->tls.inner);
  auto parsed = boost::urls::parse_uri(url);
  if (not parsed or parsed->host_address().empty()) {
    diagnostic::error("failed to parse `url`")
      .primary(hydrolix.url)
      .hint("pass the URL of the cluster, e.g., `https://acme.hydrolix.live`")
      .emit(dh);
    co_return None{};
  }
  auto const default_port
    = resolved->tls.inner ? clickhouse_tls_port : clickhouse_plaintext_port;
  co_return HydrolixEndpoint{
    // Without the brackets of an IPv6 literal.
    .host = std::string{parsed->host_address()},
    .port = detail::narrow_cast<uint16_t>(port ? port->inner : default_port),
    .token = std::move(token),
    .tls = std::move(*resolved),
  };
}

/// Explains the errors of a Hydrolix cluster that stem from its access
/// control and query guards.
auto add_hydrolix_hints(diagnostic_builder diag, int code,
                        std::string_view message) -> diagnostic_builder {
  // See ClickHouse's `ErrorCodes.cpp`.
  constexpr auto unknown_table = 60;
  constexpr auto unknown_database = 81;
  constexpr auto access_denied = 497;
  constexpr auto authentication_failed = 516;
  if (code == authentication_failed) {
    return std::move(diag).hint(
      "check that `token` is a valid Hydrolix token that has not expired");
  }
  if (code == access_denied and message.contains("grant SHOW COLUMNS")) {
    return std::move(diag).hint(
      "check that the account of `token` has the `show_columns_sql` "
      "permission on the table for schema access (also granted by "
      "`select_sql`)");
  }
  if (code == unknown_table or code == unknown_database
      or code == access_denied) {
    // Hydrolix hides the tables that an account cannot query, and a table
    // exists for queries only once it has a transform.
    return std::move(diag)
      .hint("check that the table exists and has a transform")
      .hint("check that the account of `token` has the `select_sql` "
            "permission on the table");
  }
  if (message.contains("hdx_query_timerange_required")
      or message.contains("Maximum time range exceeded")) {
    // A `where` pushes only constant bounds into the query, so `now()` needs
    // a `let` that evaluates it once.
    return std::move(diag).hint(
      "restrict the query to a time range on the table's primary timestamp, "
      "e.g., with `let $since = now() - 1h` before `from_hydrolix` and "
      "`where time > $since` after it");
  }
  return diag;
}

/// Replaces every occurrence of `secret` in `text`.
auto redact(std::string text, std::string_view secret) -> std::string {
  if (secret.empty()) {
    return text;
  }
  for (auto pos = text.find(secret); pos != std::string::npos;
       pos = text.find(secret, pos)) {
    text.replace(pos, secret.size(), "***");
    pos += 3;
  }
  return text;
}

} // namespace

/// Reports a failed query with the hints that fit its backend. For
/// `from_hydrolix`, `token` is the token that the server may echo, for
/// example as part of a failed query, and that the report must not reveal.
auto query_error(bool hydrolix, std::string_view token, bool tls_enabled,
                 Option<int> code, std::string_view raw_message)
  -> diagnostic_builder {
  auto names = backend_names(hydrolix);
  auto message = hydrolix ? redact(std::string{raw_message}, token)
                          : std::string{raw_message};
  if (hydrolix and code and *code == 516) {
    // Native authentication errors may append ClickHouse password-reset
    // instructions, which do not apply to Hydrolix API tokens.
    auto const boilerplate = message.find("\n\nIf you use ClickHouse Cloud");
    if (boilerplate != std::string::npos) {
      message.resize(boilerplate);
    }
  }
  auto diag
    = code ? diagnostic::error("{} error {}: {}", names.service, *code, message)
           : diagnostic::error("{} error: {}", names.service, message);
  if (hydrolix and code) {
    // The server answered, so the connection and its TLS setting work.
    return add_hydrolix_hints(std::move(diag), *code, message);
  }
  return add_tls_client_diagnostic_hints(std::move(diag), tls_enabled,
                                         names.service,
                                         clickhouse_plaintext_port,
                                         clickhouse_tls_port);
}

namespace {

/// The key of the subpipeline that aggregates locally.
constexpr auto aggregation_sub_key = int64_t{0};

struct QueryPlan {
  /// The table to read from; `None` when the user provided `sql`.
  Option<std::string> table;
  /// The user-provided SQL; empty when `table` is set.
  std::string sql;
  std::string schema_name;
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection, opt::Aggregation>
    optimization;
  /// The server whose dialect the pushed predicates use.
  SqlTarget target;
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

auto split_validated_table_name(std::string_view table)
  -> split_table_name_result {
  if (auto split = table_name_quoting.split_at_unquoted(table, '.')) {
    return {split->first, split->second};
  }
  return {None{}, table};
}

auto make_schema_name_from_table(std::string_view prefix,
                                 std::string_view table) -> std::string {
  auto split = split_validated_table_name(table);
  auto table_name = unquote_identifier_component(split.table);
  if (split.database) {
    return fmt::format("{}.{}.{}", prefix,
                       unquote_identifier_component(*split.database),
                       table_name);
  }
  return fmt::format("{}.{}", prefix, table_name);
}

/// Whether `SELECT *` may leave out a column with the default kind `kind`.
auto is_generated_kind(std::string_view kind) -> bool {
  return kind == "ALIAS" or kind == "MATERIALIZED" or kind == "EPHEMERAL";
}

/// Reads the columns of `table` that `SELECT *` returns.
auto fetch_schema(::clickhouse::Client& client, std::string_view table)
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
      schema.add_column(names->At(i), types->At(i),
                        is_generated_kind(default_types->At(i)));
    }
  });
  client.Execute(query);
  return schema;
}

struct PreparedQuery {
  std::string text;
  ir::OptimizeFilter local_filter;
  /// The layout of a catch-all table, whose events need reconstruction.
  Option<CatchAllLayout> catch_all = None{};
  /// Whether the query computes the plan's aggregation.
  bool aggregated = false;
  /// Whether the SELECT applies any filter based on the described schema.
  bool has_pushed_filter = false;
};

/// Pushes as much of `plan`'s hints into a `SELECT` from its table as
/// `schema` allows.
auto make_table_query(QueryPlan& plan, SqlSchema const& schema,
                      Option<ir::OptimizeProjection> const& projection,
                      registry const& reg) -> PreparedQuery {
  auto split
    = pushdown::split_filter(std::move(plan.optimization.filter),
                             schema.model(),
                             ClickHouseRenderer{plan.target, &schema}, reg);
  // The limit counts events after the whole filter chain, so it can only go
  // into the query if the chain did.
  auto limit = Option<uint64_t>{};
  if (split.remaining.empty()) {
    limit = plan.optimization.limit;
  }
  return {
    .text
    = make_select_query(*plan.table, &schema, projection, split.pushed, limit),
    .local_filter = std::move(split.remaining),
    .has_pushed_filter = not split.pushed.empty(),
  };
}

/// Decides the query text and which predicates stay local. Runs on the
/// query thread because it may need a round-trip for the schema.
auto prepare_query(::clickhouse::Client& client, QueryPlan& plan,
                   registry const& reg) -> PreparedQuery {
  if (not plan.table) {
    // TODO: Weaving hints into user-provided SQL requires parsing it. Until
    // then, everything runs locally.
    return {.text = std::move(plan.sql),
            .local_filter = std::move(plan.optimization.filter)};
  }
  // A limit alone needs no schema; only filter and projection do.
  if (plan.optimization.filter.empty() and not plan.optimization.projection) {
    return {
      .text = make_select_query(*plan.table, nullptr, None{}, {},
                                plan.optimization.limit),
      .local_filter = {},
    };
  }
  auto schema = fetch_schema(client, *plan.table);
  return make_table_query(plan, schema, plan.optimization.projection, reg);
}

/// Like `prepare_query`, but always describes a table to find out whether it
/// has a catch-all column, whose events need reconstruction.
auto prepare_events_query(::clickhouse::Client& client, QueryPlan& plan,
                          diagnostic_handler& dh, registry const& reg)
  -> failure_or<PreparedQuery> {
  if (not plan.table) {
    return prepare_query(client, plan, reg);
  }
  TRY(auto description, describe_table(client, *plan.table, dh));
  TRY(auto catch_all, make_catch_all_layout(description, dh));
  // Reconstruction moves the fields of the catch-all column to the top level
  // and places dotted columns at nested paths, so predicates address a field
  // through the column that holds it. Fields below `JSON` columns serve only
  // as prefilters, which Hydrolix does not get, since its engine may treat
  // `JSON` differently.
  auto schema = SqlSchema{};
  if (catch_all) {
    schema
      = make_catch_all_schema(description, *catch_all, plan.optimization.filter,
                              plan.target == SqlTarget::clickhouse);
    for (auto const& shadowed : schema.shadowed_fields()) {
      diagnostic::warning("column `{}` shadows field `{}` of `JSON` column",
                          shadowed.column, fmt::join(shadowed.path, "."))
        .note("predicates on the field are evaluated after reading the rows")
        .hint("rename the column or the `JSON` column to avoid the shared "
              "prefix")
        .emit(dh);
    }
  } else {
    for (auto const& column : description) {
      schema.add_column(column.name, column.type,
                        is_generated_kind(column.default_kind));
    }
  }
  // ClickHouse computes a summary only if it also evaluates every predicate,
  // which apply to the rows before the summary. A field of a catch-all table
  // may live partly in the catch-all column, so its summary always runs
  // locally, on the reconstructed events.
  auto const& aggregation = plan.optimization.aggregation;
  if (aggregation and not catch_all) {
    auto split
      = pushdown::split_filter(plan.optimization.filter, schema.model(),
                               ClickHouseRenderer{plan.target, &schema}, reg);
    if (split.remaining.empty()) {
      if (auto query = make_aggregate_query(*aggregation, schema, *plan.table,
                                            split.pushed, plan.target)) {
        return PreparedQuery{
          .text = std::move(*query),
          .local_filter = {},
          .catch_all = None{},
          .aggregated = true,
        };
      }
    }
  }
  // A projection could leave out the part of a field that is in the
  // catch-all, so the query of a catch-all table selects every column.
  auto projection = catch_all ? Option<ir::OptimizeProjection>{}
                              : plan.optimization.projection;
  auto prepared = make_table_query(plan, schema, projection, reg);
  prepared.catch_all = std::move(catch_all);
  return prepared;
}

class FromClickhouse final : public Operator<void, table_slice> {
public:
  FromClickhouse() = default;

  explicit FromClickhouse(FromClickhouseArgs args) : args_{std::move(args)} {
  }

  explicit FromClickhouse(FromHydrolixArgs args)
    : args_{to_clickhouse_args(std::move(args))} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    events_counter_ = make_events_counter(args_.hydrolix.has_value(), ctx);
    auto options = co_await make_client_options(ctx);
    if (not options) {
      // No query task runs that could end the operator, so it ends here, as
      // `FromClickhouseEvents` does.
      done_ = true;
      co_return;
    }
    auto const names = backend_names(args_.hydrolix.has_value());
    // The query text is only decided on the query thread: with optimizer hints
    // in table mode, it depends on the table's schema.
    auto plan = QueryPlan{
      .table = None{},
      .sql = {},
      .schema_name = fmt::format("{}.query", names.schema_prefix),
      .optimization = args_.optimization,
      .target = sql_target(args_),
    };
    if (args_.table) {
      plan.table = args_.table->inner;
      plan.schema_name
        = make_schema_name_from_table(names.schema_prefix, args_.table->inner);
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
    ctx.spawn_task([this, options = std::move(*options), plan = std::move(plan),
                    &dh = ctx.dh(), &reg = ctx.reg(),
                    loc = args_.operator_location]() mutable -> Task<void> {
      auto transformed_dh = transforming_diagnostic_handler{
        dh, [loc](diagnostic diag) {
          if (not has_primary_annotation(diag)) {
            diag.annotations.emplace_back(true, std::string{}, loc);
          }
          return diag;
        }};
      co_await run_query(std::move(options), std::move(plan), transformed_dh,
                         reg);
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
          slice = filter2(slice, expr, ctx, false);
          if (slice.rows() == 0) {
            co_return;
          }
        }
        // The limit counts events after the filter chain. When it went into
        // the SQL query this is a no-op; otherwise it ends the query early.
        if (args_.optimization.limit) {
          auto remaining = *args_.optimization.limit - emitted_;
          if (slice.rows() > remaining) {
            slice = subslice(slice, 0, remaining);
          }
        }
        emitted_ += slice.rows();
        if (slice.rows() > 0) {
          events_counter_.add(slice.rows());
          co_await push(std::move(slice));
        }
        if (args_.optimization.limit
            and emitted_ >= *args_.optimization.limit) {
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

  auto snapshot(Serde&) -> void override {
    reject_checkpoint(args_);
  }

private:
  /// Resolves the connection arguments, which may be secrets.
  auto make_client_options(OpCtx& ctx)
    -> Task<Option<::clickhouse::ClientOptions>> {
    if (args_.hydrolix) {
      auto endpoint = co_await resolve_hydrolix_endpoint(
        *args_.hydrolix, args_.port, args_.tls, ctx);
      if (not endpoint) {
        co_return None{};
      }
      tls_enabled_ = endpoint->tls.tls.inner;
      auto client_args = easy_client::arguments{
        .host = std::move(endpoint->host),
        .port = located<uint64_t>{endpoint->port, location::unknown},
        .user = std::string{hydrolix_token_user},
        .password = std::move(endpoint->token),
        .default_database = None{},
        .ssl = std::move(endpoint->tls),
        .table = {},
        .mode = {mode::append, location::unknown},
        .primary = None{},
        .operator_location = args_.operator_location,
      };
      co_return with_read_compression(client_args.make_options());
    }
    auto ssl_opts = args_.tls ? tls_options{*args_.tls} : tls_options{};
    auto ssl = ssl_opts.resolve(ctx.actor_system().config(), ctx.dh());
    if (not ssl) {
      co_return None{};
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
    if (args_.uri) {
      auto uri = co_await args_.uri->evaluate_string(ctx, "uri");
      if (not uri) {
        co_return None{};
      }
      auto parsed
        = parse_connection_uri(*uri, args_.uri->get_location(), ctx.dh());
      if (not parsed) {
        co_return None{};
      }
      apply_connection_uri(client_args, *parsed);
      if (not parsed->has_port()) {
        client_args.port = located<uint64_t>{default_port, location::unknown};
      }
    } else {
      auto host = co_await args_.host.evaluate_string(ctx, "host");
      if (not host) {
        co_return None{};
      }
      client_args.host = std::move(*host);
      auto user = co_await args_.user.evaluate_string(ctx, "user");
      if (not user) {
        co_return None{};
      }
      client_args.user = std::move(*user);
      auto password = co_await args_.password.evaluate_string(ctx, "password");
      if (not password) {
        co_return None{};
      }
      client_args.password = std::move(*password);
    }
    co_return with_read_compression(client_args.make_options());
  }

  auto run_query(::clickhouse::ClientOptions options, QueryPlan plan,
                 diagnostic_handler& dh, registry const& reg) -> Task<void> {
    try {
      auto first_schema = Option<type>{};
      auto const service = backend_names(args_.hydrolix.has_value()).service;
      auto on_data = [&](::clickhouse::Block const& block) {
        if (runtime_->should_cancel()) {
          return false;
        }
        auto slice = block_to_table_slice(block, plan.schema_name, service, dh);
        if (not slice) {
          return not runtime_->should_cancel();
        }
        if (not first_schema) {
          first_schema = slice->schema();
        } else if (slice->schema() != *first_schema) {
          diagnostic::warning("{} query schema changed during execution",
                              backend_names(args_.hydrolix.has_value()).service)
            .hint("continuing to emit data with the new schema")
            .emit(dh);
        }
        runtime_->produce_data(std::move(*slice));
        return not runtime_->should_cancel();
      };
      co_await spawn_blocking([&]() {
        auto client = ::clickhouse::Client{options};
        auto prepared = prepare_query(client, plan, reg);
        TENZIR_DEBUG("{} runs `{}`",
                     backend_names(args_.hydrolix.has_value()).operator_name,
                     prepared.text);
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
        query_error(args_.hydrolix.has_value(), options.password, tls_enabled_,
                    e.GetCode(), e.what())
          .emit(dh);
      }
    } catch (const std::exception& e) {
      if (not runtime_->stop_requested.load(std::memory_order_acquire)) {
        query_error(args_.hydrolix.has_value(), options.password, tls_enabled_,
                    None{}, e.what())
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
  MetricsCounter events_counter_;
  uint64_t emitted_ = 0;
};

class FromClickhouseEvents final : public Operator<void, nova::Events> {
public:
  FromClickhouseEvents() = default;

  explicit FromClickhouseEvents(FromClickhouseArgs args)
    : args_{std::move(args)} {
  }

  explicit FromClickhouseEvents(FromHydrolixArgs args)
    : args_{to_clickhouse_args(std::move(args))} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    remaining_ = args_.optimization.limit;
    events_counter_ = make_events_counter(args_.hydrolix.has_value(), ctx);
    auto connection = co_await make_connection_args(ctx);
    if (not connection) {
      done_ = true;
      co_return;
    }
    auto const tls_enabled = connection->tls and connection->tls->tls.inner;
    auto const hydrolix = args_.hydrolix.has_value();
    auto const names = backend_names(hydrolix);
    // The query text is only decided on the query thread: with optimizer hints
    // in table mode, it depends on the table's schema.
    auto plan = QueryPlan{
      .table = None{},
      .sql = {},
      .schema_name = fmt::format("{}.query", names.schema_prefix),
      .optimization = args_.optimization,
      .target = sql_target(args_),
    };
    if (args_.table) {
      plan.table = args_.table->inner;
      plan.schema_name
        = make_schema_name_from_table(names.schema_prefix, args_.table->inner);
    } else {
      plan.sql = args_.sql->inner;
    }
    // Helper task to shut down our query on cancellation.
    ctx.spawn_task([shared = shared_]() mutable -> Task<void> {
      if (not co_await catch_cancellation(wait_forever())) {
        shared->request_cancellation();
      }
    });
    // The actual query task.
    ctx.spawn_task([shared = shared_, connection = std::move(*connection),
                    plan = std::move(plan), tls_enabled, hydrolix, &dh,
                    &reg = ctx.reg(),
                    loc = args_.operator_location]() mutable -> Task<void> {
      auto transformed_dh = transforming_diagnostic_handler{
        dh, [loc](diagnostic diag) {
          if (not has_primary_annotation(diag)) {
            diag.annotations.emplace_back(true, std::string{}, loc);
          }
          return diag;
        }};
      co_await run_query(std::move(shared), std::move(connection),
                         std::move(plan), tls_enabled, hydrolix, transformed_dh,
                         reg);
    });
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await shared_->queue.dequeue();
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](PlanMessage x) -> Task<void> {
        for (auto& expr : x.local_filter) {
          auto filter = co_await nova::Evaluator::make(std::move(expr), ctx);
          if (not filter) {
            shared_->request_cancellation();
            done_ = true;
            co_return;
          }
          filters_.push_back(std::move(*filter));
        }
      },
      [&](AggregateLocallyMessage) -> Task<void> {
        TENZIR_ASSERT(args_.optimization.aggregation);
        auto const& aggregation = *args_.optimization.aggregation;
        if (not pushdown::can_aggregate_locally(aggregation, ctx)) {
          shared_->request_cancellation();
          done_ = true;
          co_return;
        }
        auto sub = co_await ctx.plan_and_spawn_sub<nova::Events>(
          aggregation_sub_key, aggregation.fallback);
        if (not sub) {
          shared_->request_cancellation();
          done_ = true;
          co_return;
        }
        aggregating_locally_ = true;
      },
      [&](EventsMessage x) -> Task<void> {
        if (done_ or shared_->should_cancel()) {
          co_return;
        }
        // Predicates without an exact SQL translation run here, with the same
        // semantics as the `where` they came from. The limit counts events
        // after them; when it went into the SQL query this is a no-op.
        auto events = pushdown::apply_read(std::move(x.events), filters_,
                                           remaining_, ctx.dh());
        if (events.active_count() > 0) {
          // The rows enter the pipeline here, even if a local aggregation
          // consumes them.
          events_counter_.add(events.active_count());
        }
        if (events.active_count() > 0 and aggregating_locally_) {
          if (auto sub = ctx.get_sub(aggregation_sub_key)) {
            // The subpipeline closes its input only when it fails, which it
            // reports itself.
            std::ignore = co_await as<SubHandle<nova::Events>>(*sub).push(
              std::move(events));
          }
        } else if (events.active_count() > 0) {
          co_await push(std::move(events));
        }
        if (remaining_ == uint64_t{0}) {
          shared_->request_cancellation();
          done_ = true;
        }
      },
      [&](DoneMessage) -> Task<void> {
        done_ = true;
        if (aggregating_locally_) {
          // The operator ends once the subpipeline emitted the aggregation.
          if (auto sub = ctx.get_sub(aggregation_sub_key)) {
            co_await as<SubHandle<nova::Events>>(*sub).close();
          }
        }
      });
  }

  auto finish_sub(SubKeyView, Push<nova::Events>&, OpCtx&)
    -> Task<void> override {
    aggregating_locally_ = false;
    co_return;
  }

  auto finish_sub(SubKeyView, failure, Push<nova::Events>&, OpCtx&)
    -> Task<void> override {
    // The subpipeline reported its error, so the read cannot complete.
    aggregating_locally_ = false;
    shared_->request_cancellation();
    done_ = true;
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    shared_->request_cancellation();
    co_return;
  }

  auto state() -> OperatorState override {
    if (aggregating_locally_) {
      return OperatorState::normal;
    }
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde&) -> void override {
    reject_checkpoint(args_);
  }

private:
  /// Resolves the connection arguments, which may be secrets.
  auto make_connection_args(OpCtx& ctx) -> Task<Option<ConnectionArgs>> {
    auto& dh = ctx.dh();
    auto connection = ConnectionArgs{};
    if (args_.hydrolix) {
      auto endpoint = co_await resolve_hydrolix_endpoint(
        *args_.hydrolix, args_.port, args_.tls, ctx);
      if (not endpoint) {
        co_return None{};
      }
      connection.host = std::move(endpoint->host);
      connection.port = endpoint->port;
      connection.user = std::string{hydrolix_token_user};
      connection.password = std::move(endpoint->token);
      connection.tls = std::move(endpoint->tls);
      co_return connection;
    }
    auto tls = tls_options::from_optional(args_.tls).resolve(
      ctx.actor_system().config(), dh);
    if (not tls) {
      co_return None{};
    }
    auto const default_port
      = tls->tls.inner ? clickhouse_tls_port : clickhouse_plaintext_port;
    connection.tls = std::move(*tls);
    connection.port = detail::narrow_cast<uint16_t>(
      args_.port ? args_.port->inner : default_port);
    if (args_.uri) {
      auto uri = co_await args_.uri->evaluate_string(ctx, "uri");
      if (not uri) {
        co_return None{};
      }
      auto parsed = parse_connection_uri(*uri, args_.uri->get_location(), dh);
      if (not parsed) {
        co_return None{};
      }
      connection.port = detail::narrow_cast<uint16_t>(default_port);
      connection.apply_uri(*parsed);
    } else {
      auto host = co_await args_.host.evaluate_string(ctx, "host");
      if (not host) {
        co_return None{};
      }
      connection.host = std::move(*host);
      auto user = co_await args_.user.evaluate_string(ctx, "user");
      if (not user) {
        co_return None{};
      }
      connection.user = std::move(*user);
      auto password = co_await args_.password.evaluate_string(ctx, "password");
      if (not password) {
        co_return None{};
      }
      connection.password = std::move(*password);
    }
    co_return connection;
  }

  struct EventsMessage {
    nova::Events events;
  };

  /// Sent before the events of a table read whose aggregation ClickHouse
  /// cannot compute, which the operator then passes to the aggregation's
  /// fallback.
  struct AggregateLocallyMessage {};

  using Message
    = variant<PlanMessage, AggregateLocallyMessage, EventsMessage, DoneMessage>;

  /// The state shared with the query thread.
  struct Shared {
    Shared() : queue{message_queue_capacity} {
    }

    folly::coro::BoundedQueue<Message, true, true> queue;
    Atomic<bool> stop_requested = false;

    /// Enqueues from the blocking query thread, giving up once downstream has
    /// declared that it needs no more data.
    auto produce(Message message) -> void {
      while (not should_cancel()) {
        if (queue.try_enqueue(message)) {
          return;
        }
        std::this_thread::sleep_for(message_queue_backoff);
      }
    }

    auto should_cancel() const -> bool {
      return stop_requested.load(std::memory_order::acquire);
    }

    auto request_cancellation() -> void {
      stop_requested.store(true, std::memory_order_release);
      while (queue.try_dequeue()) {
        // Drop buffered events; downstream has already declared it needs no
        // more.
      }
    }
  };

  static auto run_query(Arc<Shared> shared, ConnectionArgs connection,
                        QueryPlan plan, bool tls_enabled, bool hydrolix,
                        diagnostic_handler& dh, registry const& reg)
    -> Task<void> {
    // Whether the query computes the aggregation, decided before it runs.
    auto aggregated = false;
    try {
      auto catch_all = Option<CatchAllLayout>{};
      auto validate_layout = false;
      auto invalid_layout = false;
      auto const service = backend_names(hydrolix).service;
      auto on_data = [&](::clickhouse::Block const& block) {
        if (invalid_layout or shared->should_cancel()) {
          return false;
        }
        // The header arrives even if the pushed filter removed every row.
        // Validate it before decoding discards zero-row blocks, and stay
        // failed if cancellation still delivers further blocks.
        if (validate_layout
            and not validate_catch_all_block(block, *catch_all, service, dh)) {
          invalid_layout = true;
          return false;
        }
        auto events = block_to_events(block, plan.schema_name, service, dh);
        if (not events) {
          return not shared->should_cancel();
        }
        if (aggregated) {
          events = pushdown::nest_aggregation(
            std::move(*events), *plan.optimization.aggregation, dh);
        } else if (catch_all) {
          auto columns = std::vector<std::string_view>{};
          columns.reserve(block.GetColumnCount());
          for (auto i = size_t{0}; i < block.GetColumnCount(); ++i) {
            columns.push_back(block.GetColumnName(i));
          }
          events = reconstruct_catch_all(std::move(*events), columns,
                                         *catch_all, service, dh);
        }
        shared->produce(EventsMessage{std::move(*events)});
        return not shared->should_cancel();
      };
      co_await spawn_blocking([&]() {
        // Tables without a database, in `table` and in `sql`, refer to the
        // database of the URI.
        auto options = with_read_compression(connection.to_client_options());
        if (connection.default_database) {
          options.SetDefaultDatabase(*connection.default_database);
        }
        auto client = ::clickhouse::Client{options};
        auto prepared = prepare_events_query(client, plan, dh, reg);
        if (not prepared) {
          return;
        }
        TENZIR_DEBUG("{} runs `{}`", backend_names(hydrolix).operator_name,
                     prepared->text);
        aggregated = prepared->aggregated;
        if (plan.optimization.aggregation and not aggregated) {
          shared->produce(AggregateLocallyMessage{});
        }
        shared->produce(PlanMessage{std::move(prepared->local_filter)});
        catch_all = std::move(prepared->catch_all);
        validate_layout = catch_all and prepared->has_pushed_filter;
        auto query = ::clickhouse::Query{std::move(prepared->text)};
        if (aggregated) {
          // An aggregation without group keys must count `0` for empty
          // input, whatever the session's settings say.
          query.SetSetting("aggregate_functions_null_for_empty",
                           {"0", ::clickhouse::QuerySettingsField::IMPORTANT});
        }
        if (catch_all) {
          // Keeps literal dotted keys apart from nested paths, as the
          // inserts of `to_clickhouse` do.
          query.SetSetting("json_type_escape_dots_in_keys",
                           {"1", ::clickhouse::QuerySettingsField::IMPORTANT});
          // Prefilters on `JSON` fields parse their text as numbers, which
          // must yield what TQL parses from JSON, down to the last bit.
          if (not hydrolix) {
            query.SetSetting("precise_float_parsing",
                             {"1",
                              ::clickhouse::QuerySettingsField::IMPORTANT});
          }
        }
        query.SetSetting("max_block_size",
                         {std::to_string(defaults::import::table_slice_size),
                          ::clickhouse::QuerySettingsField::IMPORTANT});
        // Without this, MergeTree's byte-based cap overrides max_block_size
        // on wide tables.
        query.SetSetting("preferred_block_size_bytes",
                         {"0", ::clickhouse::QuerySettingsField::IMPORTANT});
        // `block_to_events` cannot decode ClickHouse's native `JSON` column
        // type; this makes the server send such columns as plain strings
        // instead, matching how `to_clickhouse` writes JSON columns.
        query.SetSetting("output_format_native_write_json_as_string",
                         {"1", ::clickhouse::QuerySettingsField::IMPORTANT});
        query.OnDataCancelable(on_data);
        client.Select(query);
      });
    } catch (const panic_exception&) {
      throw;
    } catch (const ::clickhouse::ServerError& e) {
      if (not shared->should_cancel()) {
        query_error(hydrolix, connection.password, tls_enabled, e.GetCode(),
                    e.what())
          .emit(dh);
      }
    } catch (const std::exception& e) {
      if (not shared->should_cancel()) {
        query_error(hydrolix, connection.password, tls_enabled, None{},
                    e.what())
          .emit(dh);
      }
    }
    co_await shared->queue.enqueue(DoneMessage{});
  }

  FromClickhouseArgs args_;
  mutable Arc<Shared> shared_ = Arc<Shared>{std::in_place};
  bool done_ = false;
  std::vector<nova::Evaluator> filters_;
  Option<uint64_t> remaining_;
  /// Whether the read passes its events to the subpipeline that aggregates
  /// them, until that subpipeline ends.
  bool aggregating_locally_ = false;
  MetricsCounter events_counter_;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_clickhouse";
  }

  auto describe() const -> Description override {
    auto d
      = Describer<FromClickhouseArgs, FromClickhouse, FromClickhouseEvents>{};
    auto uri_arg = d.named("uri", &FromClickhouseArgs::uri);
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
    d.optimization(&FromClickhouseArgs::optimization);
    // A table read can compute an aggregation in its query, while a query of
    // the user is opaque. Only the Nova operator implements the aggregation.
    d.aggregation([](FromClickhouseArgs const& args,
                     ir::OptimizeAggregation const& aggregation) -> bool {
      return nova_enabled() and args.table and not args.sql
             and supports_aggregate_functions(aggregation);
    });
    return d.without_optimize();
  }
};

/// Reads from Hydrolix through the native ClickHouse interface of its query
/// engine, which accepts a Hydrolix token as the password of a special user.
class FromHydrolixPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_hydrolix";
  }

  auto describe() const -> Description override {
    auto d
      = Describer<FromHydrolixArgs, FromClickhouse, FromClickhouseEvents>{};
    d.positional("url", &FromHydrolixArgs::url);
    d.named("token", &FromHydrolixArgs::token);
    auto table_arg = d.named("table", &FromHydrolixArgs::table);
    auto sql_arg = d.named("sql", &FromHydrolixArgs::sql);
    auto port_arg = d.named("port", &FromHydrolixArgs::port);
    auto tls_validate
      = tls_options{}.add_to_describer(d, &FromHydrolixArgs::tls);
    d.operator_location(&FromHydrolixArgs::operator_location);
    d.validate(
      [=, tls_validate = std::move(tls_validate)](DescribeCtx& ctx) -> Empty {
        tls_validate(ctx);
        auto table = ctx.get(table_arg);
        auto sql = ctx.get(sql_arg);
        if (not table and not sql) {
          diagnostic::error("no query specified")
            .hint("specify `table` or `sql`")
            .emit(ctx);
          return {};
        }
        if (table and sql) {
          diagnostic::error("`sql` and `table` are mutually exclusive")
            .primary(sql->source)
            .secondary(table->source)
            .emit(ctx);
          return {};
        }
        if (auto port = ctx.get(port_arg)) {
          if (port->inner == 0 or port->inner > 65535) {
            diagnostic::error("`port` must be between 1 and 65535")
              .primary(port->source, "got `{}`", port->inner)
              .emit(ctx);
          }
        }
        if (table) {
          std::ignore
            = validate_table_name<true>(table->inner, table->source, ctx);
        }
        return {};
      });
    d.optimization(&FromHydrolixArgs::optimization);
    d.aggregation([](FromHydrolixArgs const& args,
                     ir::OptimizeAggregation const& aggregation) -> bool {
      return nova_enabled() and args.table and not args.sql
             and supports_aggregate_functions(aggregation);
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::Plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::FromHydrolixPlugin)
