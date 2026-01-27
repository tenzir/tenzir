//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/connect_to_node.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/async/unbounded_queue.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/export_bridge.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/metric_handler.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/passive_partition.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/type.h>
#include <caf/actor_companion.hpp>
#include <caf/actor_registry.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <folly/coro/Sleep.h>

namespace tenzir::plugins::export_ {

namespace {

/// Diagnostic handler that writes to an unbounded queue for async-safe usage.
class queued_diagnostic_handler final : public diagnostic_handler {
public:
  explicit queued_diagnostic_handler(
    std::shared_ptr<UnboundedQueue<diagnostic>> queue)
    : queue_{std::move(queue)} {
  }

  auto emit(diagnostic diag) -> void override {
    queue_->enqueue(std::move(diag));
  }

private:
  std::shared_ptr<UnboundedQueue<diagnostic>> queue_;
};

struct ExportArgs {
  bool live = false;
  bool retro = false;
  bool internal = false;
  uint64_t parallel = 3;
};

auto connect_to_node(caf::actor_system& sys, bool internal_connection = false)
  -> Task<node_actor> {
  // Fast path: check local registry for existing node.
  if (auto node = sys.registry().get<node_actor>("tenzir.node")) {
    co_return node;
  }
  // Get configuration.
  const auto& opts = content(sys.config());
  auto node_endpoint = detail::get_node_endpoint(opts);
  if (not node_endpoint) {
    throw std::runtime_error(
      fmt::format("failed to get node endpoint: {}", node_endpoint.error()));
  }
  auto timeout = detail::node_connection_timeout(opts);
  auto retry_delay = detail::get_retry_delay(opts);
  auto deadline = detail::get_deadline(timeout);
  // Spawn connector and request connection.
  auto connector_actor
    = sys.spawn(connector, retry_delay, deadline, internal_connection);
  auto request = connect_request{
    .port = node_endpoint->port->number(),
    .host = node_endpoint->host,
  };
  auto result = co_await async_mail(atom::connect_v, std::move(request))
                  .request(connector_actor);
  caf::anon_send_exit(connector_actor, caf::exit_reason::user_shutdown);
  if (not result) {
    throw std::runtime_error(
      fmt::format("failed to connect to node: {}", result.error()));
  }
  co_return std::move(*result);
}

class Export final : public Operator<void, table_slice> {
public:
  explicit Export(ExportArgs args)
    : expr_{expression{
        predicate{
          meta_extractor{meta_extractor::internal},
          relational_operator::equal,
          data{args.internal},
        },
      }},
      mode_{args.live ? args.retro : true, args.live, args.internal,
            args.parallel} {
  }

  Export(const Export&) = delete;
  Export& operator=(const Export&) = delete;
  Export(Export&&) = default;
  Export& operator=(Export&&) = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    auto node = co_await connect_to_node(ctx.actor_system());
    diag_queue_ = std::make_shared<UnboundedQueue<diagnostic>>();
    auto result
      = co_await async_mail(atom::spawn_v, expr_, mode_).request(node);
    if (not result) {
      throw std::runtime_error(
        fmt::format("failed to spawn export bridge: {}", result.error()));
    }
    bridge_ = std::move(*result);
  }

  auto await_task() const -> Task<std::any> override {
    if (done_) {
      // TODO: Properly suspend.
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await async_mail(atom::get_v).request(bridge_);
  }

  auto process_task(std::any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // Drain any buffered diagnostics from the bridge
    while (auto diag = diag_queue_->try_dequeue()) {
      ctx.dh().emit(std::move(*diag));
    }
    auto expected
      = std::any_cast<caf::expected<table_slice>>(std::move(result));
    if (not expected) {
      diagnostic::error(expected.error()).note("from export-bridge").emit(ctx);
      done_ = true;
      co_return;
    }
    if (expected->rows() == 0) {
      done_ = true;
      co_return;
    }
    co_await push(std::move(*expected));
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

  ~Export() override {
    if (bridge_) {
      caf::anon_send_exit(bridge_, caf::exit_reason::user_shutdown);
    }
  }

private:
  expression expr_;
  export_mode mode_;
  export_bridge_actor bridge_;
  std::shared_ptr<UnboundedQueue<diagnostic>> diag_queue_;
  bool done_ = false;
};

class export_operator final : public crtp_operator<export_operator> {
public:
  export_operator() = default;

  explicit export_operator(expression expr, export_mode mode)
    : expr_{std::move(expr)}, mode_{mode} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto filesystem = ctrl.self().system().registry().get<filesystem_actor>(
      "tenzir.filesystem");
    TENZIR_ASSERT(filesystem);
    auto metrics_handler = ctrl.metrics({
      "tenzir.metrics.export",
      record_type{
        {"schema", string_type{}},
        {"schema_id", string_type{}},
        {"events", uint64_type{}},
        {"queued_events", uint64_type{}},
      },
    });
    auto bridge = spawn_and_link_export_bridge(
      ctrl.self(), expr_, mode_, std::move(filesystem),
      std::move(metrics_handler),
      std::make_unique<shared_diagnostic_handler>(ctrl.shared_diagnostics()));
    co_yield {};
    while (true) {
      auto result = table_slice{};
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::get_v)
        .request(bridge, caf::infinite)
        .then(
          [&](table_slice& slice) {
            ctrl.set_waiting(false);
            result = std::move(slice);
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("from export-bridge")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      if (result.rows() == 0) {
        co_return;
      }
      co_yield std::move(result);
    }
  }

  auto name() const -> std::string override {
    return "export";
  }

  auto detached() const -> bool override {
    return false;
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto internal() const -> bool override {
    return true;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    auto clauses = std::vector<expression>{};
    if (expr_ != caf::none and expr_ != trivially_true_expression()) {
      clauses.push_back(expr_);
    }
    if (filter != caf::none and filter != trivially_true_expression()) {
      clauses.push_back(filter);
    }
    auto expr = clauses.empty()
                  ? trivially_true_expression()
                  : (clauses.size() == 1 ? std::move(clauses[0])
                                         : conjunction{std::move(clauses)});
    return optimize_result{trivially_true_expression(), event_order::ordered,
                           std::make_unique<export_operator>(std::move(expr),
                                                             mode_)};
  }

  friend auto inspect(auto& f, export_operator& x) -> bool {
    return f.object(x).fields(f.field("expression", x.expr_),
                              f.field("mode", x.mode_));
  }

private:
  expression expr_;
  export_mode mode_;
};

class export_plugin final : public virtual operator_plugin<export_operator>,
                            public virtual operator_factory_plugin,
                            public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto describe() const -> Description override {
    auto d = Describer<ExportArgs, Export>{};
    d.named("live", &ExportArgs::live);
    d.named("retro", &ExportArgs::retro);
    d.named("internal", &ExportArgs::internal);
    auto parallel = d.named_optional("parallel", &ExportArgs::parallel);
    d.validate([=](ValidateCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(parallel));
      if (value == 0) {
        diagnostic::error("parallel level must be greater than zero")
          .primary(ctx.get_location(parallel).value())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"export", "https://docs.tenzir.com/"
                                            "operators/export"};
    auto retro = false;
    auto live = false;
    auto internal = false;
    auto parallel = std::optional<located<uint64_t>>{};
    parser.add("--retro", retro);
    parser.add("--live", live);
    parser.add("--internal", internal);
    parser.add("--parallel", parallel, "<level>");
    parser.parse(p);
    if (not live) {
      retro = true;
    }
    if (parallel and parallel->inner == 0) {
      diagnostic::error("parallel level must be greater than zero")
        .primary(parallel->source)
        .throw_();
      return nullptr;
    }
    return std::make_unique<export_operator>(
      expression{
        predicate{
          meta_extractor{meta_extractor::internal},
          relational_operator::equal,
          data{internal},
        },
      },
      export_mode{retro, live, internal, parallel ? parallel->inner : 3});
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto live = false;
    auto retro = false;
    auto internal = false;
    auto parallel = std::optional<located<uint64_t>>{};
    argument_parser2::operator_("export")
      .named("live", live)
      .named("retro", retro)
      .named("internal", internal)
      .named("parallel", parallel)
      .parse(inv, ctx)
      .ignore();
    if (not live) {
      // TODO: export live=false, retro=false
      retro = true;
    }
    if (parallel and parallel->inner == 0) {
      diagnostic::error("parallel level must be greater than zero")
        .primary(parallel->source)
        .emit(ctx);
      return nullptr;
    }
    return std::make_unique<export_operator>(
      expression{
        predicate{
          meta_extractor{meta_extractor::internal},
          relational_operator::equal,
          data{internal},
        },
      },
      export_mode{retro, live, internal, parallel ? parallel->inner : 3});
  }
};

class diagnostics_plugin final : public virtual operator_parser_plugin,
                                 public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "diagnostics";
  };

  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"diagnostics", "https://docs.tenzir.com/"
                                                 "operators/diagnostics"};
    auto live = false;
    auto retro = false;
    const auto internal = true;
    auto parallel = std::optional<located<uint64_t>>{};
    parser.add("--live", live);
    parser.add("--retro", retro);
    parser.add("--parallel", parallel, "<level>");
    parser.parse(p);
    if (not live) {
      retro = true;
    }
    return std::make_unique<export_operator>(
      expression{
        conjunction{
          predicate{
            meta_extractor{meta_extractor::internal},
            relational_operator::equal,
            data{internal},
          },
          predicate{
            meta_extractor{meta_extractor::schema},
            relational_operator::equal,
            data{"tenzir.diagnostic"},
          },
        },
      },
      export_mode{retro, live, internal, parallel ? parallel->inner : 3});
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto live = false;
    auto retro = false;
    const auto internal = true;
    auto parallel = std::optional<located<uint64_t>>{};
    TRY(argument_parser2::operator_("diagnostics")
          .named("live", live)
          .named("retro", retro)
          .named("parallel", parallel)
          .parse(inv, ctx));
    if (not live) {
      retro = true;
    }
    return std::make_unique<export_operator>(
      expression{
        conjunction{
          predicate{
            meta_extractor{meta_extractor::internal},
            relational_operator::equal,
            data{internal},
          },
          predicate{
            meta_extractor{meta_extractor::schema},
            relational_operator::equal,
            data{"tenzir.diagnostic"},
          },
        },
      },
      export_mode{retro, live, internal, parallel ? parallel->inner : 3});
  }
};

class metrics_plugin final : public virtual operator_parser_plugin,
                             public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "metrics";
  };

  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"metrics", "https://docs.tenzir.com/"
                                             "operators/metrics"};
    auto name = std::optional<std::string>{};
    auto live = false;
    auto retro = false;
    const auto internal = true;
    auto parallel = std::optional<located<uint64_t>>{};
    parser.add(name, "<name>");
    parser.add("--live", live);
    parser.add("--retro", retro);
    parser.add("--parallel", parallel, "<level>");
    parser.parse(p);
    if (not live) {
      retro = true;
    }
    static const auto all_metrics = [] {
      auto result = pattern::make("tenzir\\.metrics\\..*");
      TENZIR_ASSERT(result);
      return std::move(*result);
    }();
    return std::make_unique<export_operator>(
      expression{
        conjunction{
          predicate{
            meta_extractor{meta_extractor::internal},
            relational_operator::equal,
            data{internal},
          },
          predicate{
            meta_extractor{meta_extractor::schema},
            relational_operator::equal,
            name ? data{fmt::format("tenzir.metrics.{}", *name)}
                 : data{all_metrics},
          },
        },
      },
      export_mode{retro, live, internal, parallel ? parallel->inner : 3});
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto name = std::optional<located<std::string>>{};
    auto live = false;
    auto retro = false;
    const auto internal = true;
    auto parallel = std::optional<located<uint64_t>>{};
    TRY(argument_parser2::operator_("metrics")
          .positional("name", name)
          .named("live", live)
          .named("retro", retro)
          .named("parallel", parallel)
          .parse(inv, ctx));
    if (not live) {
      retro = true;
    }
    static const auto all_metrics = [] {
      auto result = pattern::make("tenzir\\.metrics\\..*");
      TENZIR_ASSERT(result);
      return std::move(*result);
    }();
    if (name and name->inner == "operator") {
      diagnostic::warning("operator metrics are deprecated")
        .hint("use `pipeline` metrics instead")
        .primary(*name)
        .emit(ctx);
    }
    return std::make_unique<export_operator>(
      expression{
        conjunction{
          predicate{
            meta_extractor{meta_extractor::internal},
            relational_operator::equal,
            data{internal},
          },
          predicate{
            meta_extractor{meta_extractor::schema},
            relational_operator::equal,
            name ? data{fmt::format("tenzir.metrics.{}", name->inner)}
                 : data{all_metrics},
          },
        },
      },
      export_mode{retro, live, internal, parallel ? parallel->inner : 3});
  }
};

} // namespace

} // namespace tenzir::plugins::export_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::export_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::diagnostics_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::metrics_plugin)
