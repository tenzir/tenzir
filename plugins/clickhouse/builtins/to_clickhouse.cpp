//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/easy_client.hpp"
#include "tenzir/tql2/plugin.hpp"

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

class clickhouse_sink_operator final
  : public crtp_operator<clickhouse_sink_operator> {
public:
  clickhouse_sink_operator() = default;

  clickhouse_sink_operator(arguments args) : args_{std::move(args)} {
  }

  friend auto inspect(auto& f, clickhouse_sink_operator& x) -> bool {
    return f.apply(x.args_);
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
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
    auto args = args_;
    args.ssl.update_cacert(ctrl);
    auto client = easy_client::make(args, ctrl.diagnostics());
    if (not client) {
      co_yield {};
      co_return;
    }
    auto running_count = size_t{0};
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
      try {
        (void)client->insert(slice);
      } catch (const std::system_error& e) {
        diagnostic::error("system error: {}", e.what())
          .primary(args_.operator_location)
          .note("while sending {} events (events {} to {})", slice.rows(),
                running_count, running_count + slice.rows())
          .note("this error is currently being investigated")
          .hint("(experimental): increasing the batching that goes into the "
                "operator _may_ help with this.\n"
                "for example: `batch 10000, timeout=5s | to_clickhouse ...`")
          .emit(ctrl.diagnostics());
        co_return;
      }
      running_count += slice.rows();
    }
  } catch (::clickhouse::Error& e) {
    diagnostic::error("unexpected error: {}", e.what())
      .primary(args_.operator_location)
      .emit(ctrl.diagnostics());
    co_return;
  }

private:
  arguments args_;
};

class to_clickhouse final : public operator_plugin2<clickhouse_sink_operator> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    TRY(auto args, arguments::try_parse(name(), inv, ctx));
    return std::make_unique<clickhouse_sink_operator>(std::move(args));
  }
};

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::to_clickhouse)
