//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/catalog.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/export_bridge.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/passive_partition.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/shared_diagnostic_handler.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/taxonomies.hpp>

#include <caf/actor_registry.hpp>
#include <caf/event_based_actor.hpp>

#include <queue>

namespace tenzir {

namespace {

struct bridge_state {
  static constexpr auto name = "export-bridge";

  export_bridge_actor::pointer self = {};

  caf::actor_addr importer_address = {};
  tenzir::taxonomies taxonomies = {};
  expression expr = {};
  std::unordered_map<type, caf::expected<expression>> bound_exprs = {};

  export_mode mode = {};

  bool checked_candidates = {};
  size_t inflight_partitions = {};
  size_t open_partitions = {};
  std::queue<std::pair<partition_info, query_context>> queued_partitions = {};
  std::optional<std::vector<table_slice>> unpersisted_events = {};

  filesystem_actor filesystem = {};

  struct metric {
    size_t emitted = {};
    size_t queued = {};
  };

  detail::flat_map<type, metric> metrics = {};
  size_t num_queued_total = {};
  metric_handler metrics_handler = {};

  std::unique_ptr<diagnostic_handler> diagnostics_handler = {};

  std::deque<std::pair<table_slice, caf::typed_response_promise<void>>> buffer;
  caf::typed_response_promise<table_slice> buffer_rp;

  auto bind_expr(const type& schema, const expression& expr)
    -> const expression* {
    auto it = bound_exprs.find(schema);
    if (it == bound_exprs.end()) {
      it = bound_exprs.emplace_hint(
        it, schema, tailor(check(normalize_and_validate(expr)), schema));
    }
    if (not it->second) {
      return nullptr;
    }
    return &*it->second;
  }

  auto is_done() const -> bool {
    return not mode.live and buffer.empty() and inflight_partitions == 0
           and open_partitions == 0 and checked_candidates
           and queued_partitions.empty() and not unpersisted_events;
  }

  auto try_pop_partition() -> void {
    const auto size_threshold = defaults::max_partition_size * mode.parallel;
    if (num_queued_total >= size_threshold) {
      return;
    }
    for (auto i = inflight_partitions; i < mode.parallel; ++i) {
      detail::weak_run_delayed(self, duration::zero(), [this] {
        pop_partition();
      });
    }
  }

  auto pop_partition() -> void {
    if (queued_partitions.empty()) {
      if (open_partitions > 0) {
        --open_partitions;
      }
      if (buffer_rp.pending() and is_done()) {
        buffer_rp.deliver(table_slice{});
      }
      return;
    }
    // Now, open one partition.
    auto [info, ctx] = std::move(queued_partitions.front());
    queued_partitions.pop();
    ++inflight_partitions;
    auto next = [this] {
      --inflight_partitions;
      try_pop_partition();
    };
    // TODO: We may want to monitor the spawned partitions to be able to return
    // better diagnostics. As-is, we only get a caf::sec::request_receiver_down
    // if they quit, but not their actual error message.
    const auto partition = self->spawn(
      passive_partition, info.uuid, filesystem,
      std::filesystem::path{fmt::format("index/{:l}", info.uuid)});
    self->mail(atom::query_v, std::move(ctx))
      .request(partition, caf::infinite)
      .then(
        [next](uint64_t) {
          next();
        },
        [this, next, uuid = info.uuid](const caf::error& error) mutable {
          diagnostic::warning(error)
            .note("failed to open partition {}", uuid)
            .emit(*diagnostics_handler);
          next();
        });
  }

  auto emit_metrics() -> void {
    TENZIR_ASSERT(not mode.internal);
    TENZIR_DEBUG("{} emits {}", *self, metrics.size());
    for (auto& [schema, metric] : metrics) {
      metrics_handler.emit({
        {"schema", std::string{schema.name()}},
        {"schema_id", schema.make_fingerprint()},
        {"events", std::exchange(metric.emitted, {})},
        {"queued_events", metric.queued},
      });
    }
  }

  auto add_events(table_slice slice, event_source source,
                  caf::typed_response_promise<void> rp) -> void {
    if (slice.rows() == 0) {
      if (rp.pending()) {
        rp.deliver();
      }
      return;
    }
    // We ignore live events if we're not asked to listen to live events.
    if (source == event_source::live and not mode.live) {
      if (rp.pending()) {
        rp.deliver();
      }
      return;
    }
    // Live and unpersisted events we still need to filter.
    if (source != event_source::retro) {
      const auto resolved = check(resolve(taxonomies, expr, slice.schema()));
      const auto* bound_expr = bind_expr(slice.schema(), resolved);
      if (not bound_expr) {
        // Failing to bind is not an error.
        if (rp.pending()) {
          rp.deliver();
        }
        return;
      }
      auto filtered = filter(slice, *bound_expr);
      if (not filtered) {
        if (rp.pending()) {
          rp.deliver();
        }
        return;
      }
      slice = std::move(*filtered);
    }
    if (source == event_source::live) {
      // We load up to N partitions depending on our parallel level, and then
      // limit our buffer to N+1 to account for live data.
      const auto size_threshold
        = (mode.parallel + 1) * defaults::max_partition_size;
      if (num_queued_total >= size_threshold) {
        diagnostic::warning("export failed to keep up and dropped events")
          .emit(*diagnostics_handler);
        if (rp.pending()) {
          rp.deliver();
        }
        return;
      }
    }
    if (buffer_rp.pending()) {
      TENZIR_ASSERT(buffer.empty());
      TENZIR_ASSERT(not is_done());
      metrics[slice.schema()].emitted += slice.rows();
      buffer_rp.deliver(std::move(slice));
      if (rp.pending()) {
        rp.deliver();
      }
      return;
    }
    metrics[slice.schema()].queued += slice.rows();
    num_queued_total += slice.rows();
    buffer.emplace_back(std::move(slice), std::move(rp));
  }

  ~bridge_state() noexcept {
    if (not mode.internal) {
      emit_metrics();
    }
    if (buffer_rp.pending()) {
      buffer_rp.deliver(caf::none);
    }
    for (auto& [_, rp] : buffer) {
      rp.deliver();
    }
  }
};

auto make_bridge(export_bridge_actor::stateful_pointer<bridge_state> self,
                 expression expr, export_mode mode, filesystem_actor filesystem,
                 metric_handler metrics_handler,
                 std::unique_ptr<diagnostic_handler> diagnostics_handler)
  -> export_bridge_actor::behavior_type {
  self->state().self = self;
  self->state().taxonomies.concepts = modules::concepts();
  self->state().expr = normalize(std::move(expr));
  self->state().mode = mode;
  self->state().metrics_handler = std::move(metrics_handler);
  TENZIR_ASSERT(diagnostics_handler);
  self->state().diagnostics_handler = std::move(diagnostics_handler);
  self->state().filesystem = std::move(filesystem);
  TENZIR_ASSERT(self->state().filesystem);
  if (not self->state().mode.internal) {
    detail::weak_run_delayed_loop(self, defaults::metrics_interval, [self] {
      self->state().emit_metrics();
    });
  }
  const auto importer
    = self->system().registry().get<importer_actor>("tenzir.importer");
  TENZIR_ASSERT(importer);
  self->state().importer_address = importer->address();
  self->state().unpersisted_events.emplace();
  self
    ->mail(atom::get_v, caf::actor_cast<receiver_actor<table_slice>>(self),
           self->state().mode.internal,
           /*live=*/self->state().mode.live,
           /*recent=*/self->state().mode.retro)
    .request(importer, caf::infinite)
    .await(
      [self, mode](std::vector<table_slice>& unpersisted_events) {
        TENZIR_DEBUG("{} subscribed to importer", *self);
        if (mode.retro) {
          TENZIR_ASSERT(self->state().unpersisted_events);
          TENZIR_ASSERT(self->state().unpersisted_events->empty());
          *self->state().unpersisted_events = std::move(unpersisted_events);
        }
      },
      [self](const caf::error& err) {
        self->quit(diagnostic::error(err)
                     .note("{} failed to subscribe to importer", *self)
                     .to_error());
      });
  // If we're retro, then we can query the catalog immediately.
  if (mode.retro) {
    const auto catalog
      = self->system().registry().get<catalog_actor>("tenzir.catalog");
    TENZIR_ASSERT(catalog);
    auto query_context
      = tenzir::query_context::make_extract("export", self, self->state().expr);
    query_context.id = uuid::random();
    TENZIR_DEBUG("export operator starts catalog lookup with id {} and "
                 "expression {}",
                 query_context.id, self->state().expr);
    self->mail(atom::candidates_v, query_context)
      .request(catalog, caf::infinite)
      .then(
        [self, query_context](catalog_lookup_result& result) {
          self->state().checked_candidates = true;
          auto max_import_time = time::min();
          for (auto& [type, info] : result.candidate_infos) {
            if (info.partition_infos.empty()) {
              continue;
            }
            const auto* bound_expr = self->state().bind_expr(type, info.exp);
            if (not bound_expr) {
              // failing to bind is not an error.
              continue;
            }
            auto ctx = query_context;
            ctx.expr = *bound_expr;
            for (auto& partition_info : info.partition_infos) {
              max_import_time
                = std::max(max_import_time, partition_info.max_import_time);
              self->state().queued_partitions.emplace(std::move(partition_info),
                                                      ctx);
            }
            while (self->state().open_partitions
                   < self->state().mode.parallel) {
              ++self->state().open_partitions;
              detail::weak_run_delayed(self, duration::zero(), [self] {
                self->state().pop_partition();
              });
            }
          }
          TENZIR_ASSERT(self->state().unpersisted_events);
          for (auto& slice : *self->state().unpersisted_events) {
            if (slice.import_time() > max_import_time) {
              self->state().add_events(std::move(slice),
                                       event_source::unpersisted,
                                       caf::typed_response_promise<void>{});
            }
          }
          self->state().unpersisted_events.reset();
          // In case we get zero partitions back from the catalog we need to
          // already signal that we're done here.
          if (self->state().buffer_rp.pending() and self->state().is_done()) {
            self->state().buffer_rp.deliver(table_slice{});
          }
        },
        [self](const caf::error& err) {
          self->quit(
            diagnostic::error(err)
              .note("{} failed to retrieve candidates from catalog", *self)
              .to_error());
        });
  }
  return {
    [self](table_slice& slice) -> caf::result<void> {
      TENZIR_ASSERT(self->current_sender());
      // Calling `current_sender()` after `make_response_promise` is broken in
      // the current version of CAF, as the sender will be moved-from. Hence we
      // do it this way.
      auto is_importer
        = self->current_sender()->address() == self->state().importer_address;
      auto rp = self->make_response_promise<void>();
      self->state().add_events(
        std::move(slice),
        is_importer ? event_source::live : event_source::retro, rp);
      return rp;
    },
    [self](atom::get) -> caf::result<table_slice> {
      // Forbid concurrent requests.
      TENZIR_ASSERT(not self->state().buffer_rp.pending());
      if (self->state().is_done()) {
        return table_slice{};
      }
      if (not self->state().buffer.empty()) {
        auto [slice, rp] = std::move(self->state().buffer.front());
        self->state().buffer.pop_front();
        TENZIR_ASSERT(slice.rows() > 0);
        auto& metric = self->state().metrics[slice.schema()];
        TENZIR_ASSERT(metric.queued >= slice.rows());
        metric.emitted += slice.rows();
        metric.queued -= slice.rows();
        self->state().num_queued_total -= slice.rows();
        self->state().try_pop_partition();
        rp.deliver();
        return slice;
      }
      self->state().buffer_rp = self->make_response_promise<table_slice>();
      return self->state().buffer_rp;
    },
    [self](caf::exit_msg& msg) {
      self->quit(std::move(msg.reason));
    },
  };
}

} // namespace

auto spawn_and_link_export_bridge(
  caf::scheduled_actor& parent, expression expr, export_mode mode,
  filesystem_actor filesystem, metric_handler metrics_handler,
  std::unique_ptr<diagnostic_handler> diagnostics_handler)
  -> export_bridge_actor {
  return parent.spawn<caf::linked>(make_bridge, std::move(expr), mode,
                                   std::move(filesystem),
                                   std::move(metrics_handler),
                                   std::move(diagnostics_handler));
}

auto spawn_export_bridge(caf::actor_system& sys, expression expr,
                         export_mode mode, filesystem_actor filesystem,
                         std::unique_ptr<diagnostic_handler> diagnostics_handler)
  -> export_bridge_actor {
  // Skip metrics for now (empty metric_handler does nothing)
  return sys.spawn(make_bridge, std::move(expr), mode, std::move(filesystem),
                   metric_handler{}, std::move(diagnostics_handler));
}

} // namespace tenzir
