//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/easy_client.hpp"
#include "tenzir/async.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <fmt/format.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>

#include <memory>

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

namespace {

constexpr auto clickhouse_plaintext_port = uint64_t{9000};
constexpr auto clickhouse_tls_port = uint64_t{9440};
/// Must stay below ClickHouse's server-side `receive_timeout`, which defaults
/// to 300 seconds in `src/Core/Defines.h` as
/// `DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC` and is applied to native TCP connections
/// in `src/Server/TCPHandler.cpp` via `socket().setReceiveTimeout(...)`.
constexpr auto clickhouse_ping_interval = std::chrono::minutes{3};
static_assert(clickhouse_ping_interval < std::chrono::minutes{5},
              "clickhouse_ping_interval must stay below 5 minutes");

class clickhouse_sink_operator final
  : public crtp_operator<clickhouse_sink_operator> {
public:
  clickhouse_sink_operator() = default;

  clickhouse_sink_operator(operator_arguments args) : args_{std::move(args)} {
  }

  friend auto inspect(auto& f, clickhouse_sink_operator& x) -> bool {
    return f.apply(x.args_);
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return {{}, event_order::unordered, copy()};
  }

  auto name() const -> std::string override {
    return "to_clickhouse";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto detached() const -> bool override {
    return true;
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> try {
    auto& dh = ctrl.diagnostics();
    auto const tls_enabled = args_.ssl.get_tls(&ctrl).inner;
    auto const default_port
      = tls_enabled ? clickhouse_tls_port : clickhouse_plaintext_port;
    auto args = easy_client::arguments{
      .host = "",
      .port = args_.port ? *args_.port
                         : located<uint64_t>{default_port, location::unknown},
      .user = "default",
      .password = "",
      .default_database = None{},
      .ssl = args_.ssl,
      .table = args_.table,
      .mode = args_.mode,
      .primary = args_.primary,
      .operator_location = args_.operator_location,
    };
    auto uri = std::string{};
    auto requests = std::vector<secret_request>{};
    auto has_uri = args_.uri && args_.uri->inner != secret::make_literal("");
    if (has_uri) {
      requests.push_back(make_secret_request("uri", *args_.uri, uri, dh));
    } else {
      requests.push_back(
        make_secret_request("host", args_.host, args.host, dh));
      requests.push_back(
        make_secret_request("user", args_.user, args.user, dh));
      requests.push_back(
        make_secret_request("password", args_.password, args.password, dh));
    }
    /// GCC 14.2 erroneously warns that the first temporary here may used as a
    /// dangling pointer at the end/suspension of the coroutine. Giving `x` a
    /// name somehow circumvents this warning.
    auto x = ctrl.resolve_secrets_must_yield(std::move(requests));
    co_yield std::move(x);
    if (has_uri) {
      auto parsed = parse_connection_uri(uri, args_.uri->source, dh);
      if (not parsed) {
        co_return;
      }
      apply_connection_uri(args, *parsed);
      if (not parsed->has_port()) {
        args.port = located<uint64_t>{default_port, location::unknown};
      }
    }
    auto client = easy_client::make(args, ctrl.self().system().config(),
                                    ctrl.diagnostics());
    if (not client) {
      co_return;
    }
    auto disp = detail::weak_run_delayed_loop(
      &ctrl.self(), clickhouse_ping_interval,
      [&client]() {
        client->ping();
      },
      false);
    const auto guard
      = detail::scope_guard([disp = std::move(disp)]() mutable noexcept {
          disp.dispose();
        });
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (slice.columns() == 0) {
        diagnostic::warning("empty event will be dropped")
          .primary(args.operator_location)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      slice = resolve_enumerations(slice);
      if (not client->insert_dynamic(slice)) {
        co_return;
      }
      co_yield {};
    }
  } catch (const panic_exception& e) {
    throw;
  } catch (const std::exception& e) {
    auto diag = diagnostic::error("ClickHouse error: {}", e.what())
                  .primary(args_.operator_location);
    add_tls_client_diagnostic_hints(std::move(diag),
                                    args_.ssl.get_tls(&ctrl).inner,
                                    "ClickHouse", clickhouse_plaintext_port,
                                    clickhouse_tls_port)
      .emit(ctrl.diagnostics());
    co_return;
  }

private:
  operator_arguments args_;
};

struct ToClickhouseArgs {
  located<secret> uri = {secret::make_literal(""), location::unknown};
  located<secret> host = {secret::make_literal("localhost"), location::unknown};
  Option<located<uint64_t>> port = None{};
  located<secret> user = {secret::make_literal("default"), location::unknown};
  located<secret> password = {secret::make_literal(""), location::unknown};
  ast::expression table = {};
  located<std::string> mode = {
    std::string{to_string(clickhouse::mode::create_append)}, location::unknown};
  Option<ast::field_path> primary = None{};
  Option<located<data>> tls;
  location operator_location;
  uint64_t jobs = 1;
};

class ToClickhouse final : public Operator<table_slice, void> {
public:
  using InputQueue = folly::coro::BoundedQueue<Option<uuid>>;

  struct runtime_state {
    explicit runtime_state(uint32_t queue_capacity)
      : input_queue{queue_capacity} {
    }

    InputQueue input_queue;
    std::unordered_map<uuid, table_slice> pending_inserts;
    std::mutex pending_inserts_mutex;
    Atomic<bool> done{false};
    std::vector<AsyncHandle<void>> worker_handles;
  };

  explicit ToClickhouse(ToClickhouseArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    TENZIR_ASSERT(not state_);
    const auto queue_capacity = detail::narrow_cast<uint32_t>(args_.jobs * 2);
    state_ = std::make_unique<runtime_state>(queue_capacity);
    auto& dh = ctx.dh();
    auto mode_val = from_string<enum clickhouse::mode>(args_.mode.inner);
    TENZIR_ASSERT(mode_val);
    auto primary = Option<located<std::string>>{};
    if (args_.primary) {
      // We know that primary is a top level field, as it was validated.
      primary = {args_.primary->path().front().id.name,
                 args_.primary->get_location()};
    }
    auto ssl = args_.tls ? tls_options{*args_.tls} : tls_options{};
    auto const tls_enabled = ssl.get_tls(&ctx.actor_system().config()).inner;
    auto const default_port
      = tls_enabled ? clickhouse_tls_port : clickhouse_plaintext_port;
    auto client_args = easy_client::arguments{
      .host = "", // resolved as secret below.
      .port = args_.port ? *args_.port
                         : located<uint64_t>{default_port, location::unknown},
      .user = "default", // resolved as secret below.
      .password = "",    // resolved as secret below.
      .default_database = None{},
      .ssl = std::move(ssl),
      .table = args_.table,
      .mode = {*mode_val, args_.mode.source},
      .primary = std::move(primary),
      .operator_location = args_.operator_location,
    };
    auto uri = std::string{};
    auto requests = std::vector<secret_request>{};
    auto has_uri = args_.uri.inner != secret::make_literal("");
    if (has_uri) {
      requests.push_back(make_secret_request("uri", args_.uri, uri, dh));
    } else {
      requests.push_back(
        make_secret_request("host", args_.host, client_args.host, dh));
      requests.push_back(
        make_secret_request("user", args_.user, client_args.user, dh));
      requests.push_back(make_secret_request("password", args_.password,
                                             client_args.password, dh));
    }
    auto ok = co_await ctx.resolve_secrets(std::move(requests));
    if (not ok) {
      state_->done.store(true, std::memory_order_release);
      co_return;
    }
    if (has_uri) {
      auto parsed = parse_connection_uri(uri, args_.uri.source, dh);
      if (not parsed) {
        state_->done.store(true, std::memory_order_release);
        co_return;
      }
      apply_connection_uri(client_args, *parsed);
      if (not parsed->has_port()) {
        client_args.port = located<uint64_t>{default_port, location::unknown};
      }
    }
    state_->worker_handles.reserve(args_.jobs);
    for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
      auto client = std::shared_ptr<easy_client>{};
      try {
        client
          = easy_client::make(client_args, ctx.actor_system().config(), dh);
      } catch (const panic_exception&) {
        throw;
      } catch (const std::exception& e) {
        auto diag = diagnostic::error("ClickHouse error: {}", e.what())
                      .primary(args_.operator_location);
        add_tls_client_diagnostic_hints(std::move(diag), tls_enabled,
                                        "ClickHouse", clickhouse_plaintext_port,
                                        clickhouse_tls_port)
          .emit(dh);
        state_->done.store(true, std::memory_order_release);
        co_return;
      }
      TENZIR_ASSERT(client);
      /// We need to wait for our workers on shutdown, so need need to keep
      /// their handles.
      state_->worker_handles.push_back(
        ctx.spawn_task(worker_loop(state_.get(), client)));
      /// We can rely on the operator's async scope cancelling our outstanding
      /// ping tasks.
      std::ignore = ctx.spawn_task(ping_loop(state_.get(), std::move(client)));
    }
    if (state_->worker_handles.empty()) {
      state_->done.store(true, std::memory_order_release);
      co_return;
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    TENZIR_ASSERT(state_);
    if (input.rows() == 0) {
      co_return;
    }
    if (input.columns() == 0) {
      diagnostic::warning("empty event will be dropped").emit(ctx.dh());
      co_return;
    }
    input = resolve_enumerations(std::move(input));
    auto query_id = uuid::random();
    {
      auto guard = std::scoped_lock{state_->pending_inserts_mutex};
      while (state_->pending_inserts.contains(query_id)) {
        query_id = uuid::random();
      }
      state_->pending_inserts.emplace(query_id, std::move(input));
    }
    co_await state_->input_queue.enqueue(Option<uuid>{query_id});
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(state_);
    state_->done.store(true, std::memory_order_release);
    // Enqueue shutdown signals
    for (auto& _ : state_->worker_handles) {
      co_await state_->input_queue.enqueue(None{});
    }
    // Join all workers and wait for them
    auto joins = std::vector<Task<void>>{};
    joins.reserve(state_->worker_handles.size());
    for (auto& handle : state_->worker_handles) {
      joins.push_back(handle.join());
    }
    co_await folly::coro::collectAllRange(std::move(joins));
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    TENZIR_ASSERT(state_);
    return state_->done.load(std::memory_order_acquire)
             ? OperatorState::done
             : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_ASSERT(state_);
    auto const guard = std::scoped_lock{state_->pending_inserts_mutex};
    serde("pending_inserts", state_->pending_inserts);
  }

private:
  static auto worker_loop(runtime_state* shared_state,
                          std::shared_ptr<easy_client> client) -> Task<void> {
    TENZIR_ASSERT(shared_state);
    while (true) {
      auto next = co_await shared_state->input_queue.dequeue();
      // Empty Option is our shutdown signal.
      if (not next) {
        break;
      }
      auto slice = table_slice{};
      // Get the slice from the state.
      {
        auto const guard
          = std::scoped_lock{shared_state->pending_inserts_mutex};
        auto const it = shared_state->pending_inserts.find(*next);
        TENZIR_ASSERT_NEQ(it, shared_state->pending_inserts.end());
        slice = it->second; // It's important we dont move out here, so the
                            // slice stays in the persisted state until success.
      }
      // Try and send the slice.
      auto success = false;
      try {
        auto const query_id = fmt::to_string(*next);
        success = client->insert_dynamic(slice, query_id).is_success();
      } catch (const panic_exception&) {
        throw;
      } catch (const std::exception& e) {
        diagnostic::error("ClickHouse error: {}", e.what()).emit(client->dh());
      } catch (...) {
        diagnostic::error("unexpected exception").emit(client->dh());
      }
      if (not success) {
        shared_state->done.store(true, std::memory_order_release);
        continue;
      }
      // Remove the slice from the persisted state.
      auto const guard = std::scoped_lock{shared_state->pending_inserts_mutex};
      auto const erased = shared_state->pending_inserts.erase(*next);
      TENZIR_ASSERT_EQ(erased, 1);
    }
  }

  static auto ping_loop(runtime_state* shared_state,
                        std::shared_ptr<easy_client> client) -> Task<void> {
    TENZIR_UNUSED(shared_state);
    while (true) {
      co_await folly::coro::sleep(clickhouse_ping_interval);
      client->ping();
    }
  }

  ToClickhouseArgs args_;
  std::unique_ptr<runtime_state> state_;
};

class to_clickhouse final
  : public virtual operator_plugin2<clickhouse_sink_operator>,
    public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, operator_arguments::try_parse(name(), inv, ctx));
    return std::make_unique<clickhouse_sink_operator>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<ToClickhouseArgs, ToClickhouse>{};
    auto uri_arg = d.named_optional("uri", &ToClickhouseArgs::uri);
    auto host_arg = d.named_optional("host", &ToClickhouseArgs::host);
    auto port_arg = d.named("port", &ToClickhouseArgs::port);
    auto user_arg = d.named_optional("user", &ToClickhouseArgs::user);
    auto password_arg
      = d.named_optional("password", &ToClickhouseArgs::password);
    auto table_arg = d.named("table", &ToClickhouseArgs::table, "string");
    auto mode_arg = d.named_optional("mode", &ToClickhouseArgs::mode);

    auto primary_arg = d.named("primary", &ToClickhouseArgs::primary, "field");
    auto jobs_arg = d.named_optional("_jobs", &ToClickhouseArgs::jobs);
    auto tls_validate
      = tls_options{}.add_to_describer(d, &ToClickhouseArgs::tls);
    d.operator_location(&ToClickhouseArgs::operator_location);
    d.validate(
      [uri_arg, host_arg, table_arg, mode_arg, port_arg, user_arg, password_arg,
       primary_arg, jobs_arg,
       tls_validate = std::move(tls_validate)](DescribeCtx& ctx) -> Empty {
        tls_validate(ctx);
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
        if (auto port = ctx.get(port_arg)) {
          if (port->inner == 0 or port->inner > 65535) {
            diagnostic::error("`port` must be between 1 and 65535")
              .primary(port->source, "got `{}`", port->inner)
              .emit(ctx);
          }
        }
        auto mode_enum = Option<enum mode>{};
        if (auto mode_opt = ctx.get(mode_arg)) {
          if (auto x = from_string<enum mode>(mode_opt->inner)) {
            mode_enum = x;
          } else {
            diagnostic::error(
              "`mode` must be one of `create`, `append` or `create_append`")
              .primary(mode_opt->source, "got `{}`", mode_opt->inner)
              .emit(ctx);
          }
        }
        if (auto jobs = ctx.get(jobs_arg)) {
          if (*jobs == 0) {
            diagnostic::error("`_jobs` must be larger than 0")
              .primary(*ctx.get_location(jobs_arg))
              .emit(ctx);
          }
          if (*jobs > 1 and mode_enum != mode::append) {
            diagnostic::error("can only specify jobs > 1 with `mode` `append`")
              .primary(*ctx.get_location(jobs_arg))
              .primary(ctx.get_location(mode_arg).value_or(location::unknown))
              .emit(ctx);
          }
        }
        if (auto table = ctx.get(table_arg)) {
          auto sp = session_provider::make(ctx);
          if (auto table_name = try_const_eval(*table, sp.as_session())) {
            if (const auto* s = try_as<std::string>(*table_name)) {
              (void)validate_table_name<true>(*s, table->get_location(), ctx);
            } else {
              diagnostic::error("`table` must be a `string`")
                .primary(table->get_location())
                .emit(ctx);
            }
          }
        }
        if (auto primary = ctx.get(primary_arg)) {
          auto p = primary->path();
          if (p.size() > 1) {
            diagnostic::error("`primary`, must be a top level field")
              .primary(primary->get_location())
              .emit(ctx);
          }
          if (not validate_identifier(p.front().id.name)) {
            emit_invalid_identifier<true>("primary", p.front().id.name,
                                          primary->get_location(), ctx);
          }
        }
        if (mode_enum == mode::create and not ctx.get(primary_arg)) {
          diagnostic::error("mode `create` requires `primary` to be set")
            .primary(ctx.get_location(mode_arg).value_or(location::unknown))
            .emit(ctx);
        }
        return {};
      });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::to_clickhouse)
