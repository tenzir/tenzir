//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
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
#include <tenzir/passive_partition.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/type.h>
#include <caf/actor_registry.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace tenzir::plugins::export_ {

namespace {

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
                            public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
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
