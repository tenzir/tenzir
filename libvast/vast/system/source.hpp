/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <unordered_map>

#include "vast/logger.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/none.hpp>
#include <caf/send.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/stream_source.hpp>

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/const_table_slice_handle.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/schema.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/atoms.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_handle.hpp"

namespace vast::system {

#if 0
/// The *Reader* concept.
struct Reader {
  Reader();

  expected<result> read();

  expected<void> schema(vast::schema&);

  expected<vast::schema> schema() const;

  const char* name() const;
};
#endif

/// The source state.
/// @tparam Reader The reader type, which must model the *Reader* concept.
template <class Reader>
struct source_state {
  // -- member types -----------------------------------------------------------

  using factory_type = table_slice_builder_ptr (*)(record_type);

  using downstream_manager
    = caf::broadcast_downstream_manager<table_slice_handle>;

  // -- constructors, destructors, and assignment operators --------------------

  source_state(caf::scheduled_actor* selfptr) : self(selfptr) {
    // nop
  }

  // -- member variables -------------------------------------------------------

  /// Filters events, i.e., causes the source to drop all matching events.
  expression filter;

  /// Maps types to the tailored filter.
  std::unordered_map<type, expression> checkers;

  /// Actor for collecting statistics.
  accountant_type accountant;

  /// Wraps the format-specific parser.
  Reader reader;

  /// Generates layout-specific table slice builders.
  factory_type factory;

  /// Maps layout type names to table slice builders.
  std::map<std::string, table_slice_builder_ptr> builders;

  /// Pretty name for log files.
  const char* name = "source";

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr;

  /// Points to the owning actor.
  caf::scheduled_actor* self;

  // -- utility functions ------------------------------------------------------

  /// Initializes the state.
  template <class ActorImpl>
  void init(ActorImpl* self, Reader rd, factory_type f) {
    // Initialize members from given arguments.
    reader = std::move(rd);
    name = reader.name();
    factory = std::move(f);
    // Fetch accountant from the registry.
    if (auto acc = self->system().registry().get(accountant_atom::value))
      accountant = caf::actor_cast<accountant_type>(acc);
    // We link to the importers and fail for the same reason, but still report to
    // the accountant.
    self->set_exit_handler(
      [=](const caf::exit_msg& msg) {
        if (accountant) {
          timestamp now = std::chrono::system_clock::now();
          self->send(accountant, "source.end", now);
        }
        self->quit(msg.reason);
      }
    );
  }

  /// Tries to access the builder for `layout`.
  table_slice_builder* builder(const type& layout) {
    auto i = builders.find(layout.name());
    if (i != builders.end())
      return i->second.get();
    return caf::visit(
      detail::overload(
        [&](const record_type& rt) -> table_slice_builder* {
          // We always add a timestamp as first column to the layout.
          auto internal = rt;
          record_field tstamp_field{"timestamp", timestamp_type{}};
          internal.fields.insert(internal.fields.begin(),
                                 std::move(tstamp_field));
          auto& ref = builders[layout.name()];
          ref = factory(internal);
          return ref.get();
        },
        [&](auto&) -> table_slice_builder* {
          VAST_ERROR(layout.name(), "is not a record type");
          return nullptr;
        }),
      layout);
  }

  // Extracts events from the source until input is exhausted or until the
  // maximum is reached.
  // @returns The number of produced events and whether we've reached the end.
  template <class PushSlice>
  std::pair<size_t, bool> extract_events(size_t max_events,
                                         size_t table_slice_size,
                                         PushSlice& push_slice) {
    auto finish_slice = [&](table_slice_builder* bptr) {
      if (!bptr)
        return;
      auto slice = bptr->finish();
      if (slice == nullptr)
        VAST_ERROR(self, "failed to finish a slice");
      else
        push_slice(std::move(slice));
    };
    size_t produced = 0;
    // The streaming operates on slices, while the reader operates on events.
    // Hence, we can produce up to num * table_slice_size events per run.
    while (produced < max_events) {
      auto maybe_e = reader.read();
      if (!maybe_e) {
        // Try again when receiving default-generated errors.
        if (!maybe_e.error())
          continue;
        // Skip bogus input that failed to parse.
        auto& err = maybe_e.error();
        if (err == ec::parse_error) {
          VAST_WARNING(self->system().render(err));
          continue;
        }
        // Log unexpected errors and when reaching the end of input.
        if (err == ec::end_of_input) {
          VAST_INFO(self->system().render(err));
        } else {
          VAST_ERROR(self->system().render(err));
        }
        /// Produce one final slices if possible.
        for (auto& kvp : builders) {
          auto bptr = kvp.second.get();
          if (kvp.second != nullptr && bptr->rows() > 0)
            finish_slice(bptr);
        }
        return {produced, true};
      }
      auto& e = *maybe_e;
      auto bptr = builder(e.type());
      if (bptr == nullptr)
        continue;
      if (!caf::holds_alternative<caf::none_t>(filter)) {
        auto& checker = checkers[e.type()];
        if (caf::holds_alternative<caf::none_t>(checker)) {
          auto x = tailor(filter, e.type());
          VAST_ASSERT(x);
          checker = std::move(*x);
        }
        if (!caf::visit(event_evaluator{e}, checker)) {
          // Skip events that don't satisfy our filter.
          continue;
        }
      }
      /// Add meta column(s).
      if (auto ts = e.timestamp(); !bptr->add(ts))
        VAST_INFO(self, "add timestamp failed", ts);
      /// Add data column(s).
      if (auto data = e.data(); !bptr->recursive_add(data, e.type()))
        VAST_INFO(self, "add data failed", data);
      ++produced;
      if (bptr->rows() == table_slice_size)
        finish_slice(bptr);
    }
    return {produced, false};
  }

  // Sends stats to the accountant after producing events.
  template <class Timepoint>
  void report_stats(size_t produced ,Timepoint start, Timepoint stop) {
    using namespace std::chrono;
    if (produced > 0) {
      auto runtime = stop - start;
      auto unit = duration_cast<microseconds>(runtime).count();
      auto rate = (produced * 1e6) / unit;
      auto events = uint64_t{produced};
      VAST_INFO(self, "produced", events, "events in", runtime,
                '(' << size_t(rate), "events/sec)");
      if (accountant != nullptr) {
        using caf::anon_send;
        auto rt = duration_cast<timespan>(runtime);
        anon_send(accountant, "source.batch.runtime", rt);
        anon_send(accountant, "source.batch.events", events);
        anon_send(accountant, "source.batch.rate", rate);
      }
    }
  }
};

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
template <class Reader>
caf::behavior source(caf::stateful_actor<source_state<Reader>>* self,
                     Reader reader,
                     typename source_state<Reader>::factory_type factory,
                     size_t table_slice_size) {
  using namespace caf;
  using namespace std::chrono;
  // Initialize state.
  self->state.init(self, std::move(reader), std::move(factory));
  // Spin up the stream manager for the source.
  self->state.mgr = self->make_continuous_source(
    // init
    [=](bool& done) {
      done = false;
      timestamp now = system_clock::now();
      self->send(self->state.accountant, "source.start", now);
    },
    // get next element
    [=](bool& done, downstream<table_slice_handle>& out, size_t num) {
      auto& st = self->state;
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      auto start = steady_clock::now();
      auto push_slice = [&](table_slice_handle slice) {
        out.push(std::move(slice));
      };
      auto [produced, eof] = st.extract_events(num * table_slice_size,
                                               table_slice_size, push_slice);
      auto stop = steady_clock::now();
      if (eof)
        done = true;
      st.report_stats(produced, start, stop);
      // TODO: if the source is unable to generate new events then we should
      //       trigger CAF to poll the source after a predefined interval of
      //       time again via delayed_send
    },
    // done?
    [](const bool& done) {
      return done;
    }
  );
  return {
    [=](get_atom, schema_atom) -> result<schema> {
      auto sch = self->state.reader.schema();
      if (sch)
        return *sch;
      return sch.error();
    },
    [=](put_atom, const schema& sch) -> result<void> {
      auto r = self->state.reader.schema(sch);
      if (r)
        return {};
      return r.error();
    },
    [=](expression& expr) {
      VAST_DEBUG(self, "sets filter expression to:", expr);
      self->state.filter = std::move(expr);
    },
    [=](sink_atom, const actor& sink) {
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      VAST_ASSERT(sink != nullptr);
      VAST_DEBUG(self, "registers sink", sink);
      // We currently support only a single sink.
      self->unbecome();
      // Start streaming.
      self->state.mgr->add_outbound_path(sink);
    },
  };
}

/// An event producer with default table slice settings.
template <class Reader>
caf::behavior default_source(caf::stateful_actor<source_state<Reader>>* self,
                             Reader reader) {
  auto slice_size = get_or(self->system().config(), "system.table-slice-size",
                           defaults::system::table_slice_size);
  return source(self, std::move(reader), default_table_slice::make_builder,
                slice_size);
}

} // namespace vast::system

