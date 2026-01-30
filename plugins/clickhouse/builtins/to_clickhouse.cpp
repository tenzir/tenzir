//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/easy_client.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/tql2/plugin.hpp"

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

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
    return do_not_optimize(*this);
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
    auto args = easy_client::arguments{
      .host = "",
      .port = args_.port,
      .user = "",
      .password = "",
      .ssl = args_.ssl,
      .table = args_.table,
      .mode = args_.mode,
      .primary = args_.primary,
      .operator_location = args_.operator_location,
    };
    /// GCC 14.2 erroneously warns that the first temporary here may used as a
    /// dangling pointer at the end/suspension of the coroutine. Giving `x` a
    /// name somehow circumvents this warning.
    auto x = ctrl.resolve_secrets_must_yield({
      make_secret_request("host", args_.host, args.host, dh),
      make_secret_request("user", args_.user, args.user, dh),
      make_secret_request("password", args_.password, args.password, dh),
    });
    co_yield std::move(x);
    args.ssl.update_from_config(ctrl);
    auto client = easy_client::make(args, ctrl);
    if (not client) {
      co_return;
    }
    auto disp = detail::weak_run_delayed_loop(
      &ctrl.self(), std::chrono::minutes{10},
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
        continue;
      }
      slice = resolve_enumerations(slice);
      (void)client->insert(slice);
    }
  } catch (const panic_exception& e) {
    throw;
  } catch (const std::exception& e) {
    diagnostic::error("unexpected error: {}", e.what())
      .primary(args_.operator_location)
      .emit(ctrl.diagnostics());
    co_return;
  }

private:
  operator_arguments args_;
};

class to_clickhouse final : public operator_plugin2<clickhouse_sink_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto args, operator_arguments::try_parse(name(), inv, ctx));
    return std::make_unique<clickhouse_sink_operator>(std::move(args));
  }
};

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::to_clickhouse)
