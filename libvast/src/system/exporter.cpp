//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/exporter.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/tracepoint.hpp"
#include "vast/error.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/query_context.hpp"
#include "vast/system/query_cursor.hpp"
#include "vast/system/query_status.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"

#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/stream_slot.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

namespace {

void shutdown_stream(
  caf::stream_source_ptr<caf::broadcast_downstream_manager<table_slice>> stream) {
  if (!stream)
    return;
  stream->shutdown();
  stream->out().fan_out_flush();
  stream->out().close();
  stream->out().force_emit_batches();
}

void attach_stream(exporter_actor::stateful_pointer<exporter_state> self) {
  struct stream_state {
    exporter_actor self;
    exporter_actor::stateful_pointer<exporter_state> self_ptr{nullptr};
  };
  auto continuous = has_continuous_option(self->state.options);
  self->state.source
    = caf::attach_stream_source(
        self, self->state.sink,
        [self](stream_state& state) {
          state.self = self;
          state.self_ptr = self;
        },
        [](stream_state& state, caf::downstream<table_slice>& out,
           size_t hint) mutable {
          auto& results = state.self_ptr->state.results;
          for (size_t pushed = 0; pushed < hint && !results.empty(); ++pushed) {
            auto& top = results.front();
            out.push(std::move(top));
            results.pop();
          }
        },
        [continuous](const stream_state& state) {
          if (continuous)
            return false;
          auto should_end = state.self_ptr->state.query_status.received
                              == state.self_ptr->state.query_status.expected
                            && state.self_ptr->state.results.empty();
          if (should_end)
            shutdown_stream(state.self_ptr->state.source);
          return should_end;
        })
        .ptr();
}

void ship_results(exporter_actor::stateful_pointer<exporter_state> self) {
  auto transformed = self->state.pipeline.finish();
  if (!transformed) {
    VAST_ERROR("exporter failed to finish the transformation: {}",
               transformed.error());
    return;
  }
  if (transformed->empty())
    return;
  for (auto& t : *transformed)
    self->state.results.push(std::move(t));
  // if (!self->state.source) [[unlikely]]
  //   attach_stream(self);
}

void buffer_results(exporter_actor::stateful_pointer<exporter_state> self,
                    table_slice slice) {
  VAST_TRACE_SCOPE("");
  auto& st = self->state;
  VAST_DEBUG("{} relays {} events", *self, slice.rows());
  // Ship the slice and update state.
  st.query_status.shipped += slice.rows();
  if (auto err = self->state.pipeline.add(std::move(slice))) {
    VAST_ERROR("exporter failed to apply the transformation: {}", err);
    return;
  }
  if (!self->state.pipeline.is_blocking())
    ship_results(self);
}

void report_statistics(exporter_actor::stateful_pointer<exporter_state> self) {
  auto& st = self->state;
  if (st.statistics_subscriber)
    self->anon_send(st.statistics_subscriber, st.name, st.query_status);
  if (st.accountant) {
    auto processed = st.query_status.processed;
    auto shipped = st.query_status.shipped;
    auto results = shipped + st.results.size();
    auto selectivity = processed != 0
                         ? detail::narrow_cast<double>(results)
                             / detail::narrow_cast<double>(processed)
                         : 1.0;
    auto msg = report{
      .data = {
        {"exporter.processed", processed},
        {"exporter.results", results},
        {"exporter.shipped", shipped},
        {"exporter.selectivity", selectivity},
        {"exporter.runtime", st.query_status.runtime},
      },
      .metadata = {
        {"query", fmt::to_string(self->state.query_context.id)},
      },
    };
    self->send(st.accountant, atom::metrics_v, std::move(msg));
  }
}

void shutdown(exporter_actor::stateful_pointer<exporter_state> self,
              caf::error err) {
  VAST_DEBUG("{} initiates shutdown with error {}", *self, render(err));
  self->send_exit(self, std::move(err));
}

void shutdown(exporter_actor::stateful_pointer<exporter_state> self) {
  if (has_continuous_option(self->state.options))
    return;
  VAST_DEBUG("{} initiates shutdown", *self);
  self->send_exit(self, caf::exit_reason::normal);
}

void request_more_hits(exporter_actor::stateful_pointer<exporter_state> self) {
  auto& st = self->state;
  if (st.query_status.received + st.query_status.scheduled
      == st.query_status.expected)
    return;
  // Sanity check.
  if (!has_historical_option(st.options)) {
    VAST_DEBUG("{} requested more hits for continuous query", *self);
    return;
  }
  // The `received < expected` must hold.
  // Otherwise, we would receive results for more partitions than qualified as
  // hits by the INDEX.
  VAST_ASSERT(st.query_status.received < st.query_status.expected);
  auto remaining = st.query_status.expected - st.query_status.received;
  // TODO: Figure out right number of partitions to ask for. For now, we
  // bound the number by an arbitrary constant.
  auto n = std::min(remaining, size_t{2});
  // Store how many partitions we schedule with our request. When receiving
  // 'done', we add this number to `received`.
  st.query_status.scheduled = n;
  // Request more hits from the INDEX.
  VAST_DEBUG("{} asks index to process {} more partitions", *self, n);
  self->send(st.index, atom::query_v, st.id, detail::narrow<uint32_t>(n));
}

void handle_batch(exporter_actor::stateful_pointer<exporter_state> self,
                  table_slice slice) {
  VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
  VAST_DEBUG("{} got batch of {} events", *self, slice.rows());
  // Construct a candidate checker if we don't have one for this type.
  auto schema = slice.schema();
  auto it = self->state.checkers.find(schema);
  if (it == self->state.checkers.end()) {
    auto x = tailor(self->state.query_context.expr, schema);
    if (!x) {
      VAST_ERROR("{} failed to tailor expression: {}", *self,
                 render(x.error()));
      shutdown(self);
      return;
    }
    VAST_DEBUG("{} tailored AST to {}: {}", *self, schema, x);
    std::tie(it, std::ignore)
      = self->state.checkers.emplace(schema, std::move(*x));
  }
  auto& checker = it->second;
  // Perform candidate check, splitting the slice into subsets if needed.
  self->state.query_status.processed += slice.rows();
  auto selection = evaluate(checker, slice, {});
  auto selection_size = rank(selection);
  if (selection_size == 0) {
    // No rows qualify.
    return;
  }
  for (auto&& selected : select(slice, expression{}, selection)) {
    buffer_results(self, std::move(selected));
  }
}

} // namespace

exporter_actor::behavior_type
exporter(exporter_actor::stateful_pointer<exporter_state> self, expression expr,
         query_options options, std::vector<pipeline>&& pipelines,
         index_actor index) {
  auto normalized_expr = normalize_and_validate(std::move(expr));
  if (!normalized_expr) {
    self->quit(caf::make_error(ec::format_error,
                               fmt::format("{} failed to normalize and "
                                           "validate expression: {}",
                                           *self, normalized_expr.error())));
    return exporter_actor::behavior_type::make_empty_behavior();
  }
  expr = *normalized_expr;
  self->state.options = options;
  self->state.query_context
    = vast::query_context::make_extract("export", self, std::move(expr));
  self->state.query_context.priority
    = has_low_priority_option(self->state.options)
        ? query_context::priority::low
        : query_context::priority::normal;
  VAST_DEBUG("spawned exporter with {} pipelines", pipelines.size());
  self->state.pipeline = pipeline_executor{std::move(pipelines)};
  // Always fetch all partitions for blocking pipelines.
  if (self->state.pipeline.is_blocking()) {
    VAST_INFO("pipeline is blocking. Size:{}", pipelines.size());
    self->state.query_context.taste = std::numeric_limits<uint32_t>::max();
  }
  self->state.index = std::move(index);
  if (has_continuous_option(options)) {
    if (self->state.pipeline.is_blocking()) {
      self->quit(caf::make_error(ec::invalid_configuration,
                                 fmt::format("{} cannot use blocking pipeline "
                                             "in continuous mode",
                                             *self)));
      return exporter_actor::behavior_type::make_empty_behavior();
    }
    VAST_DEBUG("{} has continuous query option", *self);
    self->monitor(self->state.index);
  }
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG("{} received exit from {} with reason: {}", *self, msg.source,
               msg.reason);
    if (msg.reason != caf::exit_reason::kill)
      report_statistics(self);
    shutdown_stream(self->state.source);
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    VAST_DEBUG("{} received DOWN from {}", *self, msg.source);
    if (has_continuous_option(self->state.options)
        && msg.source == self->state.index)
      report_statistics(self);
    // Without sinks and resumable sessions, there's no reason to proceed.
    shutdown_stream(self->state.source);
    self->quit(msg.reason);
  });
  return {
    [self](atom::set, accountant_actor accountant) {
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, atom::announce_v, self->name());
    },
    [self](atom::sink, caf::actor& sink) {
      VAST_DEBUG("{} registers sink {}", *self, sink);
      self->state.sink = sink;
      self->monitor(self->state.sink);
    },
    [self](atom::run) {
      VAST_VERBOSE("{} executes query: {}", *self, self->state.query_context);
      self->state.start = std::chrono::system_clock::now();
      if (!has_historical_option(self->state.options))
        return;
      self
        ->request(self->state.index, caf::infinite, atom::evaluate_v,
                  self->state.query_context)
        .then(
          [=](const query_cursor& cursor) {
            VAST_VERBOSE("{} got lookup handle {}, scheduled {}/{} "
                         "partitions",
                         *self, cursor.id, cursor.scheduled_partitions,
                         cursor.candidate_partitions);
            if (cursor.candidate_partitions == 0) {
              self->send_exit(self->state.sink,
                              caf::exit_reason::user_shutdown);
              self->quit();
              return;
            }
            self->state.id = cursor.id;
            self->state.query_status.expected = cursor.candidate_partitions;
            self->state.query_status.scheduled = cursor.scheduled_partitions;
            if (cursor.scheduled_partitions == 0)
              request_more_hits(self);
          },
          [=](const caf::error& e) {
            shutdown(self, e);
          });
    },
    [self](atom::statistics, const caf::actor& statistics_subscriber) {
      VAST_DEBUG("{} registers statistics subscriber {}", *self,
                 statistics_subscriber);
      self->state.statistics_subscriber = statistics_subscriber;
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      return caf::attach_stream_sink(
               self, in,
               [](caf::unit_t&) {
                 // nop
               },
               [=](caf::unit_t&, table_slice slice) {
                 handle_batch(self, std::move(slice));
               },
               [=](caf::unit_t&, const caf::error& err) {
                 if (err)
                   VAST_ERROR("{} got error during streaming: {}", *self, err);
                 shutdown_stream(self->state.source);
               })
        .inbound_slot();
    },
    // -- status_client_actor --------------------------------------------------
    [self](atom::status, status_verbosity v) {
      auto result = record{};
      if (v >= status_verbosity::info) {
        record exp;
        exp["expression"] = to_string(self->state.query_context.expr);
        if (v >= status_verbosity::detailed) {
          exp["start"] = caf::deep_to_string(self->state.start);
          auto pipeline_names = list{};
          for (const auto& t : self->state.pipeline.pipelines())
            pipeline_names.emplace_back(t.name());
          exp["pipelines"] = std::move(pipeline_names);
          if (v >= status_verbosity::debug)
            detail::fill_status_map(exp, self);
        }
        auto xs = list{};
        xs.emplace_back(std::move(exp));
        result["queries"] = std::move(xs);
      }
      return result;
    },
    // -- receiver_actor<table_slice> ------------------------------------------
    [self](table_slice slice) { //
      VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
      VAST_DEBUG("{} got batch of {} events", *self, slice.rows());
      self->state.query_status.processed += slice.rows();
      // Ship slices to connected SINKs.
      buffer_results(self, std::move(slice));
    },
    [self](atom::done) {
      using namespace std::string_literals;
      // Figure out if we're done by bumping the counter for `received`
      // and check whether it reaches `expected`.
      self->state.query_status.received += self->state.query_status.scheduled;
      self->state.query_status.scheduled = 0u;
      if (self->state.query_status.received
          < self->state.query_status.expected) {
        VAST_DEBUG("{} received hits from {}/{} partitions", *self,
                   self->state.query_status.received,
                   self->state.query_status.expected);
        caf::timespan runtime
          = std::chrono::system_clock::now() - self->state.start;
        self->state.query_status.runtime = runtime;
        request_more_hits(self);
      } else {
        ship_results(self);
        caf::timespan runtime
          = std::chrono::system_clock::now() - self->state.start;
        self->state.query_status.runtime = runtime;
        VAST_DEBUG("{} received all hits from {} partition(s) in {}", *self,
                   self->state.query_status.expected, vast::to_string(runtime));
        VAST_TRACEPOINT(query_done, self->state.id.as_u64().first);
        if (self->state.accountant)
          self->send(
            self->state.accountant, atom::metrics_v, "exporter.hits.runtime",
            runtime,
            metrics_metadata{
              {"query", fmt::to_string(self->state.query_context.id)}});
        if (!self->state.source)
          self->send_exit(self->state.sink, caf::exit_reason::user_shutdown);
      }
    },
  };
}

} // namespace vast::system
