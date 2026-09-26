//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/async/fetch_node.hpp"
#include "tenzir/async/mail.hpp"
#include "tenzir/async/metrics.hpp"
#include "tenzir/connect_to_node.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/async/unbounded_queue.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/detail/prometheus_metric_shaper.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/export_bridge.hpp>
#include <tenzir/import_conversion.hpp>
#include <tenzir/import_routing.hpp>
#include <tenzir/import_wire.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/metric_handler.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova_flag.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/passive_partition.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/plugin/metrics.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/filter.hpp>
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

TENZIR_ENUM(export_special_filter, none, diagnostics, metrics);
TENZIR_ENUM(metrics_shape, raw, prometheus);

namespace {

struct ExportArgs {
  bool live = false;
  bool retro = false;
  bool internal = false;
  uint64_t parallel = 3;
  bool high_priority = true;
  /// Projection is recorded but not honored at runtime yet (TNZ-1030).
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection> optimization;
  /// Setting for a special filter added by the `diagnostics` or `metrics`
  /// operators.
  export_special_filter special_filter;
  /// A metric name to use in the `metrics` version
  Option<std::string> metrics_name;
  /// The output shape for the `metrics` version.
  std::string shape = "raw";
};

auto uses_prometheus_shape(ExportArgs const& args) -> bool {
  return args.special_filter == export_special_filter::metrics
         and from_string<metrics_shape>(args.shape).value_or(metrics_shape::raw)
               == metrics_shape::prometheus;
}

using prometheus_shaper_map
  = detail::heterogeneous_string_hashmap<detail::prometheus_metric_shaper>;

auto make_prometheus_shapers() -> prometheus_shaper_map {
  auto result = prometheus_shaper_map{};
  for (auto const* plugin : plugins::get<tenzir::metrics_plugin>()) {
    auto const layout = plugin->metric_layout();
    auto layout_with_timestamp = layout.transform({{
      offset{0},
      record_type::insert_before(
        {{"timestamp", metrics::prometheus_ignore(time_type{})}}),
    }});
    TENZIR_ASSERT(layout_with_timestamp);
    auto schema_name = fmt::format("tenzir.metrics.{}", plugin->name());
    result.emplace(schema_name, detail::prometheus_metric_shaper{type{
                                  schema_name,
                                  *layout_with_timestamp,
                                  {{"internal", ""}},
                                }});
  }
  return result;
}

auto add_remainder(Option<ast::expression>& remainder, ast::expression expr)
  -> void {
  if (is_true_literal(expr)) {
    return;
  }
  if (not remainder) {
    remainder = std::move(expr);
    return;
  }
  remainder = ast::expression{
    ast::binary_expr{
      std::move(*remainder),
      ast::binary_op::and_,
      std::move(expr),
    },
  };
}

template <class Output>
class Export final : public Operator<void, Output> {
public:
  explicit Export(ExportArgs args)
    : args_{std::move(args)},
      uses_prometheus_shape_{uses_prometheus_shape(args_)} {
  }

  Export(const Export&) = delete;
  Export& operator=(const Export&) = delete;
  Export(Export&&) = default;
  Export& operator=(Export&&) = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.optimization.limit == uint64_t{0}) {
      done_ = true;
      co_return;
    }
    read_events_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "export",
      },
      MetricsDirection::read, MetricsVisibility::internal_,
      MetricsUnit::events);
    if (not args_.internal) {
      // We will always report `queued_events` as 0. In the old executor, these
      // metrics were emitted by the export bridge itself, which is how it can
      // report these metrics. In the new executor, we would need to mail the
      // metrics receiver to the node on spawn, which potentially mails the
      // actor across a process boundary, introducing shutdown issues.
      export_metrics_
        = make_metric_handler(ctx, type{
                                     "tenzir.metrics.export",
                                     record_type{
                                       {"schema", string_type{}},
                                       {"schema_id", string_type{}},
                                       {"events", uint64_type{}},
                                       {"queued_events", uint64_type{}},
                                     },
                                   });
    }
    if (uses_prometheus_shape_) {
      prometheus_shapers_ = make_prometheus_shapers();
    }
    auto node = co_await fetch_node(ctx.actor_system(), ctx.dh());
    if (not node) {
      co_return;
    }
    diag_queue_ = std::make_shared<UnboundedQueue<diagnostic>>();
    auto legacy_clauses = std::vector<expression>{};
    legacy_clauses.push_back(expression{
      predicate{
        meta_extractor{meta_extractor::internal},
        relational_operator::equal,
        data{args_.internal},
      },
    });
    if (uses_prometheus_shape_) {
      for (const auto& filter : args_.optimization.filter) {
        add_remainder(remainder_, filter);
      }
    } else {
      for (const auto& filter : args_.optimization.filter) {
        auto [legacy, remainder] = split_legacy_expression(filter);
        if (legacy != trivially_true_expression()) {
          legacy_clauses.push_back(std::move(legacy));
        }
        if constexpr (std::same_as<Output, nova::Events>) {
          add_remainder(remainder_, filter);
        } else {
          add_remainder(remainder_, std::move(remainder));
        }
      }
    }
    switch (args_.special_filter) {
      using enum export_special_filter;
      case none: {
        break;
      }
      case diagnostics: {
        legacy_clauses.emplace_back(predicate{
          meta_extractor{meta_extractor::schema},
          relational_operator::equal,
          data{"tenzir.diagnostic"},
        });
        break;
      }
      case metrics: {
        static const auto all_metrics = [] {
          auto result = pattern::make("tenzir\\.metrics\\..*");
          TENZIR_ASSERT(result);
          return std::move(*result);
        }();
        legacy_clauses.emplace_back(predicate{
          meta_extractor{meta_extractor::schema},
          relational_operator::equal,
          args_.metrics_name
            ? data{fmt::format("tenzir.metrics.{}", *args_.metrics_name)}
            : data{all_metrics},
        });
        break;
      }
    }
    auto expr = legacy_clauses.size() == 1
                  ? std::move(legacy_clauses[0])
                  : expression{conjunction{std::move(legacy_clauses)}};
    auto mode
      = export_mode{args_.live ? args_.retro : true, args_.live, args_.internal,
                    args_.parallel, args_.high_priority};
    mode.nova = std::same_as<Output, nova::Events>;
    // The bridge can only count events that pass the full filter. If part of
    // it still runs here, or Prometheus shaping changes the events, the limit
    // must stay local.
    if (not mode.nova and not remainder_ and not uses_prometheus_shape_) {
      mode.limit = args_.optimization.limit;
    } else {
      local_limit_ = args_.optimization.limit;
    }
    if constexpr (std::same_as<Output, nova::Events>) {
      if (remainder_) {
        auto evaluator = nova::Evaluator::make(
          *remainder_, nova::InstantiateCtx{ctx.dh(), ctx.reg()});
        if (not evaluator) {
          co_return;
        }
        evaluator_.emplace(std::move(*evaluator));
      }
    }
    auto result
      = co_await async_mail(atom::spawn_v, std::move(expr), mode).request(*node);
    if (not result) {
      throw std::runtime_error(
        fmt::format("failed to spawn export bridge: {}", result.error()));
    }
    bridge_ = std::move(*result);
    bridge_is_local_ = bridge_->node() == ctx.actor_system().node();
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      // TODO: Properly suspend.
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if constexpr (std::same_as<Output, nova::Events>) {
      if (bridge_is_local_) {
        co_return co_await async_mail(atom::get_v, atom::internal_v)
          .request(bridge_);
      }
      auto wire = co_await async_mail(atom::get_v, atom::internal_v, true)
                    .request(bridge_);
      if (not wire) {
        co_return caf::expected<nova::Events>{wire.error()};
      }
      auto events = from_import_wire(*wire);
      if (not events) {
        co_return caf::expected<nova::Events>{
          caf::make_error(ec::type_clash, std::move(events).unwrap_err())};
      }
      co_return caf::expected<nova::Events>{std::move(events).unwrap()};
    } else {
      co_return co_await async_mail(atom::get_v).request(bridge_);
    }
  }

  auto process_task(Any result, Push<Output>& push, OpCtx& ctx)
    -> Task<void> override {
    // Drain any buffered diagnostics from the bridge
    while (auto diag = diag_queue_->try_dequeue()) {
      ctx.dh().emit(std::move(*diag));
    }
    if constexpr (std::same_as<Output, nova::Events>) {
      auto expected = std::move(result).as<caf::expected<nova::Events>>();
      if (not expected) {
        if (not stopping_) {
          diagnostic::error(expected.error())
            .note("from export-bridge")
            .emit(ctx);
        }
        done_ = true;
        co_return;
      }
      if (expected->active_count() == 0) {
        done_ = true;
        co_return;
      }
      co_await emit_events(std::move(*expected), push, ctx);
      co_return;
    }
    auto expected = std::move(result).as<caf::expected<table_slice>>();
    if (not expected) {
      if (stopping_) {
        done_ = true;
        co_return;
      }
      diagnostic::error(expected.error()).note("from export-bridge").emit(ctx);
      done_ = true;
      co_return;
    }
    if (expected->rows() == 0) {
      done_ = true;
      co_return;
    }
    if (uses_prometheus_shape_) {
      auto shaper = prometheus_shapers_.find(expected->schema().name());
      if (shaper == prometheus_shapers_.end()) {
        if (warned_unsupported_prometheus_schemas_
              .insert(std::string{expected->schema().name()})
              .second) {
          diagnostic::warning("omitting metrics with unsupported Prometheus "
                              "shape schema `{}`",
                              expected->schema().name())
            .emit(ctx);
        }
        co_return;
      }
      for (auto&& output : shaper->second.shape(*expected)) {
        co_await emit_slice(std::move(output), push, ctx);
      }
      co_return;
    }
    co_await emit_slice(std::move(*expected), push, ctx);
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // Pure retro exports drain naturally via the bridge's empty-slice
    // sentinel, so they don't need forced teardown.
    // For any live export (`live=true`, with or without `retro`) the bridge
    // enters an indefinite live-wait after the retro backlog is consumed and
    // will never send the sentinel on its own. Kill it so the pipeline can
    // terminate. In-flight retro reads that haven't been pushed downstream
    // yet may be dropped, which is acceptable under stop() semantics.
    if (not args_.live) {
      co_return;
    }
    stopping_ = true;
    if (bridge_) {
      caf::anon_send_exit(bridge_, caf::exit_reason::user_shutdown);
    } else {
      done_ = true;
    }
    co_return;
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
  auto emit_events(nova::Events events, Push<Output>& push, OpCtx& ctx)
    -> Task<void> {
    if (local_limit_ and *local_limit_ == 0) {
      done_ = true;
      co_return;
    }
    if (args_.special_filter != export_special_filter::none) {
      auto keep = nova::storage::BitMap::Mutable{events.length()};
      for (auto row : nova::storage::true_bits(events.mask)) {
        auto name = *events.meta.name.get(row);
        auto selected = *events.meta.internal.get(row) == args_.internal;
        if (args_.special_filter == export_special_filter::diagnostics) {
          selected = selected and name == "tenzir.diagnostic";
        } else if (args_.special_filter == export_special_filter::metrics) {
          selected
            = selected
              and (args_.metrics_name ? name
                                          == fmt::format("tenzir.metrics.{}",
                                                         *args_.metrics_name)
                                      : name.starts_with("tenzir.metrics."));
        }
        keep.set(row, selected);
      }
      events.mask = std::move(keep).finish();
    }
    if (uses_prometheus_shape_) {
      // The existing Prometheus formatter consumes Arrow; raw exports do not.
      auto groups = group_import_shapes(events);
      if (not groups) {
        diagnostic::error("{}", std::move(groups).unwrap_err()).emit(ctx);
        co_return;
      }
      for (auto const& group : groups.unwrap()) {
        auto shaper = prometheus_shapers_.find(group.key.name);
        if (shaper == prometheus_shapers_.end()) {
          if (warned_unsupported_prometheus_schemas_.insert(group.key.name)
                .second) {
            diagnostic::warning("omitting metrics with unsupported Prometheus "
                                "shape schema `{}`",
                                group.key.name)
              .emit(ctx);
          }
          continue;
        }
        auto conversion
          = ImportConversionBuffer{group.key.name, group.key.internal};
        auto added = conversion.add(events, group.mask);
        if (not added) {
          diagnostic::error("{}", std::move(added).unwrap_err()).emit(ctx);
          co_return;
        }
        auto slices = conversion.snapshot();
        if (not slices) {
          diagnostic::error("{}", std::move(slices).unwrap_err()).emit(ctx);
          co_return;
        }
        for (auto const& slice : slices.unwrap()) {
          for (auto&& output : shaper->second.shape(slice)) {
            co_await emit_slice(std::move(output), push, ctx);
          }
        }
      }
      co_return;
    }
    if (evaluator_) {
      auto result = evaluator_->eval(events, nova::EvalCtx{ctx.dh()});
      auto predicate = result.get_alternative<nova::Bool>();
      if (not predicate) {
        co_return;
      }
      events.mask = events.mask & predicate->present
                    & as<nova::storage::BitMap>(predicate->data.storage());
    }
    if (local_limit_
        and static_cast<uint64_t>(events.active_count()) > *local_limit_) {
      events.mask = events.mask.keep_first(
        static_cast<nova::storage::Index>(*local_limit_));
    }
    auto rows = events.active_count();
    if (rows == 0) {
      co_return;
    }
    if (not args_.internal) {
      for (auto row : nova::storage::true_bits(events.mask)) {
        export_metrics_.emit(
          {{"schema", std::string{*events.meta.name.get(row)}},
           {"schema_id", ""},
           {"events", uint64_t{static_cast<uint64_t>(rows)}},
           {"queued_events", uint64_t{0}}});
        break;
      }
    }
    read_events_counter_.add(rows);
    if (local_limit_) {
      *local_limit_ -= rows;
      if (*local_limit_ == 0) {
        done_ = true;
      }
    }
    co_await push(std::move(events));
  }

  auto emit_slice(table_slice slice, Push<Output>& push, OpCtx& ctx)
    -> Task<void> {
    if (local_limit_ and *local_limit_ == 0) {
      done_ = true;
      co_return;
    }
    auto const schema = slice.schema();
    auto rows = uint64_t{0};
    if constexpr (std::same_as<Output, table_slice>) {
      if (remainder_) {
        slice = filter2(slice, *remainder_, ctx, false);
      }
      if (local_limit_) {
        slice = head(std::move(slice), *local_limit_);
      }
      rows = slice.rows();
      if (rows > 0) {
        co_await push(std::move(slice));
      }
    } else {
      static_assert(std::same_as<Output, nova::Events>);
      auto imported = import_table_slice(slice);
      if (not imported) {
        diagnostic::error("{}", std::move(imported).unwrap_err())
          .note("failed to convert exported events")
          .emit(ctx);
        co_return;
      }
      auto events = std::move(imported).unwrap();
      if (evaluator_) {
        auto result = evaluator_->eval(events, nova::EvalCtx{ctx.dh()});
        auto predicate = result.get_alternative<nova::Bool>();
        if (not predicate) {
          co_return;
        }
        events.mask = events.mask & predicate->present
                      & as<nova::storage::BitMap>(predicate->data.storage());
      }
      if (local_limit_
          and static_cast<uint64_t>(events.active_count()) > *local_limit_) {
        events.mask = events.mask.keep_first(
          static_cast<nova::storage::Index>(*local_limit_));
      }
      rows = events.active_count();
      if (rows > 0) {
        co_await push(std::move(events));
      }
    }
    read_events_counter_.add(rows);
    if (local_limit_) {
      *local_limit_ -= rows;
      if (*local_limit_ == 0) {
        done_ = true;
      }
    }
    if (rows > 0 and not args_.internal) {
      export_metrics_.emit({
        {"schema", std::string{schema.name()}},
        {"schema_id", schema.make_fingerprint()},
        {"events", rows},
        {"queued_events", uint64_t{0}},
      });
    }
  }

  ExportArgs args_;
  bool uses_prometheus_shape_ = false;
  export_bridge_actor bridge_;
  bool bridge_is_local_ = false;
  std::shared_ptr<UnboundedQueue<diagnostic>> diag_queue_;
  Option<ast::expression> remainder_ = None{};
  Option<uint64_t> local_limit_ = None{};
  Option<nova::Evaluator> evaluator_ = None{};
  prometheus_shaper_map prometheus_shapers_ = {};
  detail::heterogeneous_string_hashset warned_unsupported_prometheus_schemas_;
  MetricsCounter read_events_counter_;
  metric_handler export_metrics_ = {};
  bool stopping_ = false;
  bool done_ = false;
};

class export_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "export";
  }

  auto describe() const -> Description override {
    auto d = Describer<ExportArgs>{ExportArgs{
      .optimization = {},
      .special_filter = export_special_filter::none,
      .metrics_name = {},
    }};
    d.spawner([]<class Input>(DescribeCtx&)
                -> failure_or<Option<SpawnWith<ExportArgs, Input>>> {
      if constexpr (std::same_as<Input, void>) {
        if (nova_enabled()) {
          return SpawnWith<ExportArgs, Input>{
            [](ExportArgs args) -> Box<Operator<void, nova::Events>> {
              return Export<nova::Events>{std::move(args)};
            }};
        }
        return SpawnWith<ExportArgs, Input>{
          [](ExportArgs args) -> Box<Operator<void, table_slice>> {
            return Export<table_slice>{std::move(args)};
          }};
      }
      return {};
    });
    d.named("live", &ExportArgs::live);
    d.named("retro", &ExportArgs::retro);
    d.named("internal", &ExportArgs::internal);
    d.named("_high_priority", &ExportArgs::high_priority);
    auto parallel = d.named_optional("parallel", &ExportArgs::parallel);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(parallel));
      if (value == 0) {
        diagnostic::error("parallel level must be greater than zero")
          .primary(ctx.get_location(parallel).value())
          .emit(ctx);
      }
      return {};
    });
    d.optimization(&ExportArgs::optimization);
    return d.without_optimize();
  }
};

class diagnostics_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "diagnostics";
  };

  auto describe() const -> Description override {
    auto d = Describer<ExportArgs, Export<table_slice>>{ExportArgs{
      .internal = true,
      .optimization = {},
      .special_filter = export_special_filter::diagnostics,
      .metrics_name = {},
    }};
    d.named("live", &ExportArgs::live);
    d.named("retro", &ExportArgs::retro);
    d.named("_high_priority", &ExportArgs::high_priority);
    auto parallel = d.named_optional("parallel", &ExportArgs::parallel);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(parallel));
      if (value == 0) {
        diagnostic::error("parallel level must be greater than zero")
          .primary(ctx.get_location(parallel).value())
          .emit(ctx);
      }
      return {};
    });
    d.optimization(&ExportArgs::optimization);
    return d.without_optimize();
  }
};

class metrics_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "metrics";
  };

  auto describe() const -> Description override {
    auto d = Describer<ExportArgs, Export<table_slice>>{ExportArgs{
      .internal = true,
      .optimization = {},
      .special_filter = export_special_filter::metrics,
      .metrics_name = {},
    }};
    auto name = d.positional("name", &ExportArgs::metrics_name);
    d.named("live", &ExportArgs::live);
    d.named("retro", &ExportArgs::retro);
    d.named("_high_priority", &ExportArgs::high_priority);
    auto shape
      = d.named_optional("shape", &ExportArgs::shape, "raw|prometheus");
    auto parallel = d.named_optional("parallel", &ExportArgs::parallel);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(parallel); value and value == uint64_t{0}) {
        diagnostic::error("parallel level must be greater than zero")
          .primary(ctx.get_location(parallel).value())
          .emit(ctx);
      }
      if (auto value = ctx.get(name); value and value == "operator") {
        diagnostic::error("operator metrics have been removed")
          .primary(ctx.get_location(name).value())
          .emit(ctx);
      }
      if (auto value = ctx.get(shape);
          value and not from_string<metrics_shape>(*value)) {
        diagnostic::error("shape must be 'raw' or 'prometheus'")
          .primary(ctx.get_location(shape).value())
          .emit(ctx);
      }
      return {};
    });
    d.optimization(&ExportArgs::optimization);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::export_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::export_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::diagnostics_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::export_::metrics_plugin)
