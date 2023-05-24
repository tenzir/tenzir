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
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/pipeline.hpp"
#include "vast/query_context.hpp"
#include "vast/query_options.hpp"
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
  VAST_DEBUG("exporter: shutting down stream");
  stream->shutdown();
  stream->out().fan_out_flush();
  stream->out().close();
  stream->out().force_emit_batches();
}

void attach_result_stream(
  exporter_actor::stateful_pointer<exporter_state> self) {
  struct stream_state {
    exporter_actor self;
    exporter_actor::stateful_pointer<exporter_state> self_ptr{nullptr};
  };
  self->state.result_stream = // [formatting]
    caf::attach_stream_source(
      self, self->state.sink,
      [self](stream_state& state) {
        state.self = self;
        state.self_ptr = self;
      },
      [](stream_state& state, caf::downstream<table_slice>& out, size_t hint) {
        auto& results = state.self_ptr->state.sink_buffer;
        (void)hint; // We could consider using `hint`.
        if (!results.empty()) {
          out.push(std::move(results.front()));
          results.pop_front();
        }
      },
      [](const stream_state& state) {
        // This call to `unsafe_current` is fine because we do not dereference
        // the iterator.
        auto should_end = state.self_ptr->state.executor.unsafe_current()
                            == state.self_ptr->state.executor.end()
                          && state.self_ptr->state.sink_buffer.empty();
        if (should_end)
          shutdown_stream(state.self_ptr->state.result_stream);
        return should_end;
      })
      .ptr();
}

auto index_exhausted(const query_status& qs) -> bool {
  if (qs.received > qs.expected) {
    VAST_WARN("exporter received more partitions than expected: {}/{}",
              qs.received, qs.expected);
    return true;
  }
  return qs.received == qs.expected;
}

auto query_in_flight(const query_status& qs) -> bool {
  return qs.scheduled > 0;
}

void continue_execution(exporter_actor::stateful_pointer<exporter_state> self) {
  // This call is fine, because we advance the iterator before dereferencing it.
  auto it = self->state.executor.unsafe_current();
  while (it != self->state.executor.end()) {
    ++it;
    if (it == self->state.executor.end()) {
      VAST_DEBUG("{} has exhausted its executor", *self);
      break;
    }
    auto result = *it;
    if (!result) {
      self->state.result_stream->stop(caf::make_error(
        ec::unspecified, fmt::format("{} encountered an error during "
                                     "execution and shuts down: {}",
                                     *self, result.error())));
      return;
    }
    if (!self->state.source_buffer.empty()) {
      // Execute at least until the source buffer is empty (or the executor
      // becomes exhausted).
      continue;
    }
    if (has_historical_option(self->state.options)) {
      // Make sure that the source requests more data, if possible.
      if (!index_exhausted(self->state.query_status)
          && !query_in_flight(self->state.query_status)) {
        VAST_DEBUG("{} waits for source to request more data", *self);
        continue;
      }
    }
    // Do not pause if we can see that the source will become exhausted.
    if (!has_continuous_option(self->state.options)
        && index_exhausted(self->state.query_status)) {
      VAST_DEBUG("{} will advance until executor is done", *self);
      continue;
    }
    VAST_DEBUG("{} paused execution", *self);
    break;
  }
}

void provide_to_source(exporter_actor::stateful_pointer<exporter_state> self,
                       table_slice slice) {
  auto& st = self->state;
  VAST_DEBUG("{} relays {} events", *self, slice.rows());
  // Ship the slice and update state.
  st.query_status.shipped += slice.rows();
  self->state.source_buffer.push_back(std::move(slice));
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
      VAST_DEBUG("{} failed to tailor expression and drops slice: {}", *self,
                 x.error());
      std::tie(it, std::ignore)
        = self->state.checkers.emplace(schema, std::nullopt);
    } else {
      VAST_DEBUG("{} tailored AST to {}: {}", *self, schema, x);
      std::tie(it, std::ignore)
        = self->state.checkers.emplace(schema, std::move(*x));
    }
  }
  auto& checker = it->second;
  // Perform candidate check, splitting the slice into subsets if needed.
  self->state.query_status.processed += slice.rows();
  if (not checker) {
    return;
  }
  auto selection = evaluate(*checker, slice, {});
  auto selection_size = rank(selection);
  if (selection_size == 0) {
    // No rows qualify.
    return;
  }
  for (auto&& selected : select(slice, expression{}, selection)) {
    provide_to_source(self, std::move(selected));
  }
  VAST_DEBUG("{} continues execution because of input stream batch", *self);
  continue_execution(self);
}

using exporter_ptr = exporter_actor::stateful_pointer<exporter_state>;

class exporter_source final : public crtp_operator<exporter_source> {
public:
  explicit exporter_source(exporter_ptr exporter) : exporter_{exporter} {
  }

  auto operator()() const -> generator<table_slice> {
    auto& state = exporter_->state;
    while (true) {
      if (state.source_buffer.empty()) {
        // This operator is only responsible to request historical data.
        // Continuous data is feed to `source_buffer` by the exporter actor.
        if (has_historical_option(state.options)) {
          if (state.id == uuid{}) {
            VAST_DEBUG("{} source stalls to await cursor", *exporter_);
          } else if (!index_exhausted(state.query_status)) {
            // We stall because there is more historical data to receive.
            if (!query_in_flight(state.query_status)) {
              VAST_DEBUG("{} source sends query to index", *exporter_);
              exporter_->send(state.index, atom::query_v, state.id, 1u);
              state.query_status.scheduled += 1;
            }
            VAST_DEBUG("{} source stalls to await data (got {}/{} partitions)",
                       *exporter_, state.query_status.received,
                       state.query_status.expected);
          } else if (!has_continuous_option(state.options)) {
            // All historical data has been received, we processed it completely,
            // and there is no continuous data coming. Hence, we are done.
            break;
          }
        }
        co_yield {};
      } else {
        auto slice = std::move(state.source_buffer.front());
        state.source_buffer.pop_front();
        VAST_DEBUG("{} source popped {} events from queue", *exporter_,
                   slice.rows());
        co_yield std::move(slice);
      }
    }
    VAST_DEBUG("{} source is done", *exporter_);
  }

  auto to_string() const -> std::string override {
    return "<exporter_source>";
  }

private:
  exporter_ptr exporter_;
};

class exporter_sink final : public crtp_operator<exporter_sink> {
public:
  explicit exporter_sink(exporter_ptr exporter) : exporter_{exporter} {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<std::monostate> {
    for (auto&& slice : input) {
      if (slice.rows() != 0) {
        VAST_DEBUG("{} sink stores {} events in result buffer", *exporter_,
                   slice.rows());
        exporter_->state.sink_buffer.push_back(std::move(slice));
      }
      co_yield {};
    }
  }

  auto to_string() const -> std::string override {
    return "<exporter_sink>";
  }

private:
  exporter_ptr exporter_;
};

} // namespace

auto exporter(exporter_actor::stateful_pointer<exporter_state> self,
              query_options options, pipeline pipe, index_actor index)
  -> exporter_actor::behavior_type {
  VAST_DEBUG("spawned {} with pipeline {}", *self, pipe);
  self->state.pipeline_str = pipe.to_string();
  auto expr = trivially_true_expression();
  if (auto pushdown = pipe.predicate_pushdown_pipeline(expr)) {
    std::tie(expr, pipe) = std::move(*pushdown);
  }
  auto normalized = normalize_and_validate(std::move(expr));
  if (!normalized) {
    self->quit(caf::make_error(ec::format_error,
                               fmt::format("{} failed to normalize and "
                                           "validate expression: {}",
                                           *self, normalized.error())));
    return exporter_actor::behavior_type::make_empty_behavior();
  }
  expr = std::move(*normalized);
  pipe.prepend(std::make_unique<exporter_source>(self));
  pipe.append(std::make_unique<exporter_sink>(self));
  VAST_DEBUG("{} uses filter {} and pipeline {}", *self, expr, pipe);
  self->state.options = options;
  self->state.query_context
    = vast::query_context::make_extract("export", self, std::move(expr));
  self->state.query_context.priority
    = has_low_priority_option(self->state.options)
        ? query_context::priority::low
        : query_context::priority::normal;
  self->state.executor = make_local_executor(std::move(pipe));
  self->state.index = std::move(index);
  if (has_continuous_option(options)) {
    VAST_DEBUG("{} has continuous query option", *self);
    self->monitor(self->state.index);
  }
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG("{} received exit from {} with reason: {}", *self, msg.source,
               msg.reason);
    shutdown_stream(self->state.result_stream);
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    VAST_DEBUG("{} received DOWN from {}", *self, msg.source);
    // Without sinks and resumable sessions, there's no reason to proceed.
    shutdown_stream(self->state.result_stream);
    self->quit(msg.reason);
  });
  return {
    [self](atom::set, accountant_actor accountant) {
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, atom::announce_v, self->name());
    },
    [self](atom::sink, caf::actor& sink) -> caf::result<void> {
      if (self->state.sink) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot stream results to {} "
                                           "because it already streams to {}",
                                           *self, sink, self->state.sink));
      }
      VAST_DEBUG("{} registers sink {}", *self, sink);
      self->state.sink = sink;
      self->monitor(self->state.sink);
      attach_result_stream(self);
      return {};
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
            VAST_DEBUG("{} is setting cursor ({})", *self, cursor.id);
            self->state.id = cursor.id;
            self->state.query_status.expected = cursor.candidate_partitions;
            self->state.query_status.scheduled = cursor.scheduled_partitions;
            VAST_DEBUG("{} continues execution due to received cursor", *self);
            continue_execution(self);
          },
          [=](const caf::error& e) {
            if (self->state.result_stream) {
              self->state.result_stream->stop(e);
            } else {
              VAST_WARN("{} shuts down before sink is attached: {}", *self, e);
              self->quit(e);
            }
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
                 shutdown_stream(self->state.result_stream);
               })
        .inbound_slot();
    },
    // -- status_client_actor --------------------------------------------------
    [self](atom::status, status_verbosity v, duration) {
      auto result = record{};
      if (v >= status_verbosity::info) {
        record exp;
        exp["expression"] = to_string(self->state.query_context.expr);
        if (v >= status_verbosity::detailed) {
          exp["start"] = caf::deep_to_string(self->state.start);
          auto pipeline_names = list{};
          // TODO: Is this what we want?
          pipeline_names.emplace_back(self->state.pipeline_str);
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
      provide_to_source(self, std::move(slice));
    },
    [self](atom::done) {
      // Figure out if we're done by bumping the counter for `received`
      // and check whether it reaches `expected`.
      self->state.query_status.received += self->state.query_status.scheduled;
      self->state.query_status.scheduled = 0u;
      VAST_DEBUG("{} received hits from {}/{} partitions", *self,
                 self->state.query_status.received,
                 self->state.query_status.expected);
      caf::timespan runtime
        = std::chrono::system_clock::now() - self->state.start;
      self->state.query_status.runtime = runtime;
      VAST_DEBUG("{} continues execution due partition completion", *self);
      continue_execution(self);
      if (index_exhausted(self->state.query_status)) {
        VAST_DEBUG("{} received all hits from {} partition(s) in {}", *self,
                   self->state.query_status.expected, vast::to_string(runtime));
        VAST_TRACEPOINT(query_done, self->state.id.as_u64().first);
        if (self->state.accountant)
          self->send(
            self->state.accountant, atom::metrics_v, "exporter.hits.runtime",
            runtime,
            metrics_metadata{
              {"query", fmt::to_string(self->state.query_context.id)}});
        if (!self->state.result_stream)
          self->send_exit(self->state.sink, caf::exit_reason::user_shutdown);
      }
    },
  };
}

} // namespace vast::system
