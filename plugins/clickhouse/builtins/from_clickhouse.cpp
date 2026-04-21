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
#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <clickhouse/client.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>

#include <algorithm>
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
};

struct QueryPlan {
  std::string query;
  std::string schema_name;
};

struct SliceMessage {
  table_slice slice;
};

struct DoneMessage {};

using Message = variant<SliceMessage, DoneMessage>;
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
  Atomic<bool> stop_requested = false;
};

class FromClickhouse final : public Operator<void, table_slice> {
public:
  FromClickhouse() = default;

  explicit FromClickhouse(FromClickhouseArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto plan = make_query_plan(args_);
    auto ssl = args_.tls ? tls_options{*args_.tls} : tls_options{};
    tls_enabled_ = ssl.get_tls(&ctx.actor_system().config()).inner;
    auto const default_port
      = tls_enabled_ ? clickhouse_tls_port : clickhouse_plaintext_port;
    auto client_args = easy_client::arguments{
      .host = "",
      .port = args_.port ? *args_.port
                         : located<uint64_t>{default_port, location::unknown},
      .user = "default",
      .password = "",
      .default_database = None{},
      .ssl = std::move(ssl),
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
    auto options = client_args.make_options(ctx.actor_system().config());
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

  auto process_task(Any result, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](SliceMessage x) -> Task<void> {
        co_await push(std::move(x.slice));
        co_return;
      },
      [&](DoneMessage) -> Task<void> {
        done_ = true;
        co_return;
      });
    co_return;
  }

  auto stop(OpCtx&) -> Task<void> override {
    runtime_->stop_requested.store(true, std::memory_order_release);
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
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

  static auto make_query_plan(FromClickhouseArgs const& args) -> QueryPlan {
    TENZIR_ASSERT(args.sql or args.table);
    if (args.sql) {
      return QueryPlan{
        .query = args.sql->inner,
        .schema_name = "clickhouse.query",
      };
    }
    TENZIR_ASSERT(args.table);
    auto qualified = std::string{args.table->inner};
    return QueryPlan{
      .query = fmt::format("SELECT * FROM {}", qualified),
      .schema_name = make_schema_name_from_table(args.table->inner),
    };
  }

  static auto enqueue_message(RuntimeState& runtime, Message message)
    -> Task<bool> {
    while (not runtime.stop_requested.load(std::memory_order_acquire)) {
      if (runtime.queue.try_enqueue(std::move(message))) {
        co_return true;
      }
      co_await sleep_for(message_queue_backoff);
    }
    co_return false;
  }

  auto run_query(::clickhouse::ClientOptions options, QueryPlan plan,
                 diagnostic_handler& dh) -> Task<void> {
    auto emitted_terminal_diagnostic = false;
    try {
      auto client = ::clickhouse::Client{std::move(options)};
      auto first_schema = Option<type>{};
      auto query = ::clickhouse::Query{plan.query};
      query.OnDataCancelable([&](::clickhouse::Block const& block) {
        if (runtime_->stop_requested.load(std::memory_order_acquire)) {
          return false;
        }
        if (block.GetColumnCount() == 0) {
          return not runtime_->stop_requested.load(std::memory_order_acquire);
        }
        if (block.GetRowCount() == 0) {
          return not runtime_->stop_requested.load(std::memory_order_acquire);
        }
        auto slice = block_to_table_slice(block, plan.schema_name, dh);
        if (not slice) {
          return not runtime_->stop_requested.load(std::memory_order_acquire);
        }
        if (not first_schema) {
          first_schema = slice->schema();
        } else if (slice->schema() != *first_schema) {
          diagnostic::warning(
            "ClickHouse query schema changed during execution")
            .hint("continuing to emit data with the new schema")
            .emit(dh);
        }
        if (not folly::coro::blockingWait(enqueue_message(
              *runtime_, Message{SliceMessage{.slice = std::move(*slice)}}))) {
          return false;
        }
        return not runtime_->stop_requested.load(std::memory_order_acquire);
      });
      client.Select(query);
      if (runtime_->stop_requested.load(std::memory_order_acquire)) {
        co_return;
      }
      co_await enqueue_message(*runtime_, Message{DoneMessage{}});
    } catch (const panic_exception&) {
      throw;
    } catch (const ::clickhouse::ServerError& e) {
      add_tls_client_diagnostic_hints(
        diagnostic::error("ClickHouse error {}: {}", e.GetCode(), e.what()),
        tls_enabled_, "ClickHouse", clickhouse_plaintext_port,
        clickhouse_tls_port)
        .emit(dh);
      emitted_terminal_diagnostic = true;
    } catch (const std::exception& e) {
      add_tls_client_diagnostic_hints(
        diagnostic::error("ClickHouse error: {}", e.what()), tls_enabled_,
        "ClickHouse", clickhouse_plaintext_port, clickhouse_tls_port)
        .emit(dh);
      emitted_terminal_diagnostic = true;
    }
    if (emitted_terminal_diagnostic) {
      co_return;
    }
    co_return;
  }

  FromClickhouseArgs args_;
  mutable Arc<RuntimeState> runtime_ = Arc<RuntimeState>{std::in_place};
  bool tls_enabled_ = false;
  bool done_ = false;
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::Plugin)
