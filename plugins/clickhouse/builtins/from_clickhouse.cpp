//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/arguments.hpp"
#include "clickhouse/block_to_events.hpp"
#include "clickhouse/catch_all_reader.hpp"
#include "clickhouse/connection.hpp"
#include "clickhouse/query_diagnostics.hpp"
#include "clickhouse/query_plan.hpp"
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
#include "tenzir/nova/events.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/pushdown/plan.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/url/parse.hpp>
#include <clickhouse/client.h>
#include <fmt/ranges.h>
#include <folly/OperationCancelled.h>
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

/// What a read connects to and what it queries, which `from_clickhouse` and
/// `from_hydrolix` share.
struct ReadSource {
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
  /// Set when the operator runs as `from_hydrolix`, whose connection
  /// arguments replace the explicit ones.
  Option<HydrolixConnectionArgs> hydrolix = None{};
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
  Option<located<bool>> pushdown;
  /// The stages that the operator takes over, which it computes in its query
  /// with ClickHouse's semantics.
  OptimizationArgs<opt::Projection, opt::Plan> optimization;

  auto source() const -> ReadSource {
    return {
      .uri = uri,
      .table = table,
      .host = host,
      .port = port,
      .user = user,
      .password = password,
      .sql = sql,
      .tls = tls,
      .operator_location = operator_location,
      .hydrolix = None{},
    };
  }
};

struct FromHydrolixArgs {
  StartupArgument<nova::Secret> url;
  StartupArgument<nova::Secret> token;
  Option<located<std::string>> table;
  Option<located<std::string>> sql;
  Option<located<uint64_t>> port = None{};
  Option<located<data>> tls;
  location operator_location;
  Option<located<bool>> pushdown;
  /// The stages that the operator takes over, which it computes in its query
  /// with the semantics of Hydrolix's ClickHouse engine.
  OptimizationArgs<opt::Projection, opt::Plan> optimization;

  /// Runs `from_hydrolix` like `from_clickhouse` against the cluster's native
  /// ClickHouse interface.
  auto source() const -> ReadSource {
    auto result = ReadSource{};
    result.table = table;
    result.port = port;
    result.sql = sql;
    result.tls = tls;
    result.operator_location = operator_location;
    result.hydrolix = HydrolixConnectionArgs{
      .url = url,
      .token = token,
    };
    return result;
  }
};

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

/// Fails a checkpoint. A query has no cursor to resume from, so a restored
/// operator would run it again and repeat the events that earlier checkpoints
/// committed.
[[noreturn]] auto reject_checkpoint(ReadSource const& source) -> void {
  diagnostic::error("{} does not support checkpoints yet",
                    backend_names(source.hydrolix.has_value()).operator_name)
    .primary(source.operator_location)
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

/// Whether Hydrolix rejected a query for the time range that it reads, which
/// is missing or too long.
auto is_time_range_error(std::string_view message) -> bool {
  return message.contains("hdx_query_timerange_required")
         or message.contains("Maximum time range exceeded");
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
  if (is_time_range_error(message)) {
    // A `where` reaches the query only with constant bounds, so `now()` needs
    // a `let` that evaluates it once. The optimizer moves a `where` ahead of a
    // `select`, `drop`, or `sort`, but not ahead of a `head` or a `summarize`,
    // whose result it reads, so it would not constrain the table.
    return std::move(diag)
      .hint("restrict the query to a time range on the table's primary "
            "timestamp, e.g., with `let $since = now() - 1h` before "
            "`from_hydrolix` and `where time > $since` directly after it")
      .hint("place the `where` before any `head` or `summarize`, whose result "
            "it would read instead of the table");
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

struct DoneMessage {};

constexpr auto message_queue_capacity = uint32_t{16};
constexpr auto message_queue_backoff = std::chrono::milliseconds{1};

auto has_primary_annotation(diagnostic const& diag) -> bool {
  return std::any_of(diag.annotations.begin(), diag.annotations.end(),
                     [](auto const& annotation) {
                       return annotation.primary;
                     });
}

/// The state that an operator shares with its query thread, which enqueues
/// `Message`s.
template <class Message>
struct QueryChannel {
  QueryChannel() : queue{message_queue_capacity} {
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

/// Sets the query settings that every read needs.
auto apply_read_settings(::clickhouse::Query& query) -> void {
  query.SetSetting("max_block_size",
                   {std::to_string(defaults::import::table_slice_size),
                    ::clickhouse::QuerySettingsField::IMPORTANT});
  // Without this, MergeTree's byte-based cap overrides max_block_size on wide
  // tables.
  query.SetSetting("preferred_block_size_bytes",
                   {"0", ::clickhouse::QuerySettingsField::IMPORTANT});
  // The decoders cannot decode ClickHouse's native `JSON` column type; this
  // makes the server send such columns as plain strings instead, matching how
  // `to_clickhouse` writes JSON columns.
  query.SetSetting("output_format_native_write_json_as_string",
                   {"1", ::clickhouse::QuerySettingsField::IMPORTANT});
}

/// Runs `query` with `client` until it ends or `cancelled()` holds.
///
/// A sort or a summary may send no data block until it has read all its
/// input, but the server reports progress in the meantime. Cancellation
/// unwinds `Select` from the progress callback, so that the caller's private
/// client closes its connection without draining the result, which also stops
/// the query on the server.
template <class Cancelled>
auto select_cancelable(::clickhouse::Client& client, ::clickhouse::Query& query,
                       Cancelled cancelled) -> void {
  query.OnProgress([cancelled](::clickhouse::Progress const&) {
    if (cancelled()) {
      throw folly::OperationCancelled{};
    }
  });
  if (cancelled()) {
    return;
  }
  client.Select(query);
}

/// Resolves the connection arguments of `source`, which may be secrets.
auto resolve_connection(ReadSource const& source, OpCtx& ctx)
  -> Task<Option<ConnectionArgs>> {
  auto& dh = ctx.dh();
  auto connection = ConnectionArgs{};
  if (source.hydrolix) {
    auto endpoint = co_await resolve_hydrolix_endpoint(
      *source.hydrolix, source.port, source.tls, ctx);
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
  auto tls = tls_options::from_optional(source.tls)
               .resolve(ctx.actor_system().config(), dh);
  if (not tls) {
    co_return None{};
  }
  auto const default_port
    = tls->tls.inner ? clickhouse_tls_port : clickhouse_plaintext_port;
  connection.tls = std::move(*tls);
  connection.port = detail::narrow_cast<uint16_t>(
    source.port ? source.port->inner : default_port);
  if (source.uri) {
    auto uri = co_await source.uri->evaluate_string(ctx, "uri");
    if (not uri) {
      co_return None{};
    }
    auto parsed = parse_connection_uri(*uri, source.uri->get_location(), dh);
    if (not parsed) {
      co_return None{};
    }
    connection.port = detail::narrow_cast<uint16_t>(default_port);
    connection.apply_uri(*parsed);
  } else {
    auto host = co_await source.host.evaluate_string(ctx, "host");
    if (not host) {
      co_return None{};
    }
    connection.host = std::move(*host);
    auto user = co_await source.user.evaluate_string(ctx, "user");
    if (not user) {
      co_return None{};
    }
    connection.user = std::move(*user);
    auto password = co_await source.password.evaluate_string(ctx, "password");
    if (not password) {
      co_return None{};
    }
    connection.password = std::move(*password);
  }
  co_return connection;
}

/// Reads a ClickHouse or Hydrolix table, or the result of a query of the user,
/// and in `table` mode computes the stages that it took over in its query,
/// with the semantics of the server's ClickHouse engine.
class FromClickhouseEvents final : public Operator<void, nova::Events> {
public:
  FromClickhouseEvents() = default;

  explicit FromClickhouseEvents(FromClickhouseArgs args)
    : source_{args.source()},
      plan_{std::move(args.optimization.plan).value_or(ir::QueryPlan{})},
      projection_{std::move(args.optimization.projection)} {
  }

  explicit FromClickhouseEvents(FromHydrolixArgs args)
    : source_{args.source()},
      plan_{std::move(args.optimization.plan).value_or(ir::QueryPlan{})},
      projection_{std::move(args.optimization.projection)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto const hydrolix = source_.hydrolix.has_value();
    auto const names = backend_names(hydrolix);
    events_counter_ = make_events_counter(hydrolix, ctx);
    auto connection = co_await resolve_connection(source_, ctx);
    if (not connection) {
      done_ = true;
      co_return;
    }
    auto read = Read{
      .table = None{},
      .sql = {},
      .schema_name = fmt::format("{}.query", names.schema_prefix),
      .plan = plan_,
      .projection = projection_,
      .hydrolix = hydrolix,
    };
    if (source_.table) {
      read.table = source_.table->inner;
      read.schema_name = make_schema_name_from_table(names.schema_prefix,
                                                     source_.table->inner);
    } else {
      read.sql = source_.sql->inner;
    }
    // Helper task to shut down our query on cancellation.
    ctx.spawn_task([shared = shared_]() mutable -> Task<void> {
      if (not co_await catch_cancellation(wait_forever())) {
        shared->request_cancellation();
      }
    });
    // The actual query task.
    ctx.spawn_task([shared = shared_, connection = std::move(*connection),
                    read = std::move(read), &dh,
                    loc = source_.operator_location]() mutable -> Task<void> {
      auto transformed_dh = transforming_diagnostic_handler{
        dh, [loc](diagnostic diag) {
          if (not has_primary_annotation(diag)) {
            diag.annotations.emplace_back(true, std::string{}, loc);
          }
          return diag;
        }};
      co_await run_query(std::move(shared), std::move(connection),
                         std::move(read), transformed_dh);
    });
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await shared_->queue.dequeue();
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](EventsMessage x) -> Task<void> {
        if (done_ or shared_->should_cancel()) {
          co_return;
        }
        auto const rows = x.events.active_count();
        if (rows > 0) {
          events_counter_.add(rows);
          co_await push(std::move(x.events));
        }
      },
      [&](DoneMessage) -> Task<void> {
        done_ = true;
        co_return;
      });
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    shared_->request_cancellation();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde&) -> void override {
    reject_checkpoint(source_);
  }

private:
  /// What the query task reads.
  struct Read {
    /// The table to read from; `None` when the user provided `sql`.
    Option<std::string> table;
    /// The user-provided SQL; empty when `table` is set.
    std::string sql;
    std::string schema_name;
    /// The stages that the query computes.
    ir::QueryPlan plan;
    /// The fields that the stages and the operators after them need.
    Option<ir::OptimizeProjection> projection;
    /// Whether the read runs as `from_hydrolix`.
    bool hydrolix = false;
  };

  struct EventsMessage {
    nova::Events events;
  };

  using Message = variant<EventsMessage, DoneMessage>;

  /// The state shared with the query thread.
  using Shared = QueryChannel<Message>;

  /// The query of a read and how its result becomes events.
  struct PreparedRead {
    std::string sql;
    pushdown::SqlLayout layout
      = {.fields = None{}, .hidden = {}, .schema_name = None{}};
    /// The layout of a catch-all table, whose rows need reconstruction.
    Option<CatchAllLayout> catch_all = None{};
    BoundPlan bound;
  };

  /// Plans the query of `read`, which in `table` mode depends on the columns
  /// of the table. Fails with a diagnostic if a field that the stages read
  /// does not bind to the table.
  static auto prepare(::clickhouse::Client& client, Read const& read,
                      diagnostic_handler& dh) -> failure_or<PreparedRead> {
    if (not read.table) {
      // A query of the user is opaque.
      return PreparedRead{
        .sql = read.sql,
        .layout = {.fields = None{}, .hidden = {}, .schema_name = None{}},
        .catch_all = None{},
        .bound = {},
      };
    }
    auto const& table = *read.table;
    auto const names = backend_names(read.hydrolix);
    TRY(auto description, describe_table(client, table, dh));
    TRY(auto catch_all, make_catch_all_layout(description, dh));
    auto columns = std::vector<std::string>{};
    auto schema = SqlSchema{};
    for (auto const& column : description) {
      columns.push_back(column.name);
      schema.add_column(column.name, column.type,
                        is_generated_kind(column.default_kind));
    }
    auto bound
      = bind_plan(TableBinding{std::move(description), catch_all}, read.plan);
    if (bound.is_err()) {
      auto error = std::move(bound).unwrap_err();
      auto diag = [&] {
        switch (error.kind) {
          case BindError::Kind::field:
            return diagnostic::error("cannot bind `{}` in {} table `{}`",
                                     error.subject, names.service, table);
          case BindError::Kind::stage:
            return diagnostic::error("cannot compute `{}` in {} table `{}`",
                                     error.subject, names.service, table);
          case BindError::Kind::sort:
            return diagnostic::error("cannot sort by the JSON field `{}` "
                                     "before further stages in {} table `{}`",
                                     error.subject, names.service, table);
        }
        TENZIR_UNREACHABLE();
      }();
      std::move(diag)
        .note("{}", error.reason)
        .hint("use `pushdown=false` to run the operators after `{}` in "
              "Tenzir instead",
              names.operator_name)
        .emit(dh);
      return failure::promise();
    }
    // A catch-all table needs every column for the reconstruction, and a
    // drop keeps every column that it does not name.
    auto const drops = std::ranges::any_of(read.plan, [](auto const& stage) {
      return is<ir::QueryStage::Drop>(stage.kind);
    });
    auto const selection = catch_all or drops
                             ? std::string{"*"}
                             : select_columns(&schema, read.projection);
    auto rendered
      = make_table_read(table, selection, read.plan, columns, bound.unwrap());
    if (rendered.is_err()) {
      diagnostic::error("{}", rendered.unwrap_err()).emit(dh);
      return failure::promise();
    }
    return PreparedRead{
      .sql = std::move(rendered.unwrap().sql),
      .layout = std::move(rendered.unwrap().layout),
      .catch_all = std::move(catch_all),
      .bound = std::move(bound).unwrap(),
    };
  }

  static auto run_query(Arc<Shared> shared, ConnectionArgs connection,
                        Read read, diagnostic_handler& dh) -> Task<void> {
    auto const tls_enabled = connection.tls and connection.tls->tls.inner;
    auto const names = backend_names(read.hydrolix);
    // Whether the failing query computes stages, rather than describing the
    // table.
    auto computes = false;
    try {
      auto prepared = PreparedRead{};
      auto invalid_layout = false;
      auto on_data = [&](::clickhouse::Block const& block) {
        if (invalid_layout or shared->should_cancel()) {
          return false;
        }
        auto const& catch_all = prepared.catch_all;
        // A column that the table gained after it was described could hold a
        // field that a stage read from the catch-all column instead. The
        // header arrives even if a filter removed every row, so validate it
        // before decoding discards zero-row blocks.
        if (catch_all and not read.plan.empty()
            and not validate_catch_all_block(block, *catch_all, names.service,
                                             dh, prepared.layout.hidden)) {
          invalid_layout = true;
          return false;
        }
        auto events
          = block_to_events(block, read.schema_name, names.service, dh);
        if (not events) {
          return not shared->should_cancel();
        }
        // Leaves out the columns that the query needs for itself, before the
        // reconstruction could mistake them for fields.
        events
          = pushdown::apply_layout(std::move(*events), prepared.layout, dh);
        if (catch_all) {
          auto columns = std::vector<std::string_view>{};
          for (auto i = size_t{0}; i < block.GetColumnCount(); ++i) {
            auto const& name = block.GetColumnName(i);
            if (not std::ranges::contains(prepared.layout.hidden, name)) {
              columns.push_back(name);
            }
          }
          events = reconstruct_catch_all(std::move(*events), columns,
                                         *catch_all, names.service, dh);
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
        auto planned = prepare(client, read, dh);
        if (not planned) {
          return;
        }
        prepared = std::move(*planned);
        if (read.table) {
          TENZIR_DEBUG("{} runs `{}`", names.operator_name,
                       pushdown::redact_query(prepared.sql,
                                              clickhouse_lexicon));
        }
        auto query = ::clickhouse::Query{prepared.sql};
        if (read.table) {
          // A query of the user keeps the session's settings.
          for (auto const& setting : table_read_settings(
                 prepared.bound, prepared.catch_all.has_value(),
                 read.hydrolix)) {
            query.SetSetting(std::string{setting.name},
                             {std::string{setting.value},
                              ::clickhouse::QuerySettingsField::IMPORTANT});
          }
        }
        apply_read_settings(query);
        query.OnDataCancelable(on_data);
        computes = not read.plan.empty();
        select_cancelable(client, query, [&] {
          return shared->should_cancel();
        });
      });
    } catch (const panic_exception&) {
      throw;
    } catch (const ::clickhouse::ServerError& e) {
      if (not shared->should_cancel()) {
        auto diag = query_error(read.hydrolix, connection.password, tls_enabled,
                                e.GetCode(), e.what());
        // Running the operators in Tenzir would not give the query of a
        // Hydrolix cluster the time range that it lacks.
        if (computes and not(read.hydrolix and is_time_range_error(e.what()))) {
          // The server may reject a stage that it computes with its own
          // semantics, such as a comparison of values of different types.
          diag = std::move(diag)
                   .note("the query computes the operators after `{}` with "
                         "{}'s semantics",
                         names.operator_name, names.service)
                   .hint("use `pushdown=false` to run them in Tenzir "
                         "instead");
        }
        std::move(diag).emit(dh);
      }
    } catch (const std::exception& e) {
      if (not shared->should_cancel()) {
        query_error(read.hydrolix, connection.password, tls_enabled, None{},
                    e.what())
          .emit(dh);
      }
    }
    co_await shared->queue.enqueue(DoneMessage{});
  }

  ReadSource source_;
  /// The stages that the operator took over.
  ir::QueryPlan plan_;
  Option<ir::OptimizeProjection> projection_;
  mutable Arc<Shared> shared_ = Arc<Shared>{std::in_place};
  bool done_ = false;
  MetricsCounter events_counter_;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_clickhouse";
  }

  auto describe() const -> Description override {
    // Only the Nova operator reads from ClickHouse.
    auto d = Describer<FromClickhouseArgs, FromClickhouseEvents>{};
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
    pushdown::add_pushdown_argument(d, &FromClickhouseArgs::pushdown);
    d.optimization(&FromClickhouseArgs::optimization);
    // A table read takes over the stages that ClickHouse spells. A query of
    // the user is opaque.
    d.query_plan([](FromClickhouseArgs const& args,
                    ir::QueryChain const& chain) -> ir::Absorption {
      if (not pushdown::pushdown_enabled(args.pushdown)) {
        return {.prefix = 0, .declined = "`pushdown=false`"};
      }
      if (not args.table or args.sql) {
        return {.prefix = 0, .declined = "the query of the user is opaque"};
      }
      return absorb_stages(chain, "ClickHouse");
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
    // Only the Nova operator reads from Hydrolix.
    auto d = Describer<FromHydrolixArgs, FromClickhouseEvents>{};
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
    pushdown::add_pushdown_argument(d, &FromHydrolixArgs::pushdown);
    d.optimization(&FromHydrolixArgs::optimization);
    // A table read takes over the stages that Hydrolix's ClickHouse engine
    // spells, as `from_clickhouse` does. A query of the user is opaque.
    d.query_plan([](FromHydrolixArgs const& args,
                    ir::QueryChain const& chain) -> ir::Absorption {
      if (not pushdown::pushdown_enabled(args.pushdown)) {
        return {.prefix = 0, .declined = "`pushdown=false`"};
      }
      if (not args.table or args.sql) {
        return {.prefix = 0, .declined = "the query of the user is opaque"};
      }
      return absorb_stages(chain, "Hydrolix");
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::Plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::FromHydrolixPlugin)
