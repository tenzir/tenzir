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
#include "vast/default_table_slice_builder.hpp"
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
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast::system {

/// The source state.
/// @tparam Reader The reader type, which must model the *Reader* concept.
template <class Reader, class Self = caf::event_based_actor>
struct source_state {
  // -- member types -----------------------------------------------------------

  using downstream_manager
    = caf::broadcast_downstream_manager<table_slice_ptr>;

  // -- constructors, destructors, and assignment operators --------------------

  source_state(Self* selfptr) : self(selfptr) {
    // nop
  }

  ~source_state() {
    self->send(accountant, "source.end", caf::make_timestamp());
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
  table_slice_builder_factory factory;

  /// Maps layout type names to table slice builders.
  std::map<std::string, table_slice_builder_ptr> builders;

  /// Pretty name for log files.
  const char* name = "source";

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr;

  /// Points to the owning actor.
  Self* self;

  // -- utility functions ------------------------------------------------------

  /// Initializes the state.
  template <class ActorImpl>
  void init(ActorImpl* self, Reader rd, table_slice_builder_factory f) {
    // Initialize members from given arguments.
    reader = std::move(rd);
    name = reader.name();
    factory = f;
    // Fetch accountant from the registry.
    if (auto acc = self->system().registry().get(accountant_atom::value)) {
      VAST_DEBUG(self, "uses registry accountant:", accountant);
      accountant = caf::actor_cast<accountant_type>(acc);
      self->send(accountant, announce_atom::value, name);
    }
  }

  /// Tries to access the builder for `layout`.
  table_slice_builder* builder(const type& layout, size_t table_slice_size) {
    auto i = builders.find(layout.name());
    if (i != builders.end())
      return i->second.get();
    return caf::visit(
      detail::overload(
        [&](const record_type& t) {
          auto& builder = builders[layout.name()];
          builder = factory(t);
          builder->reserve(table_slice_size);
          return builder.get();
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
          VAST_WARNING(self, self->system().render(err));
          continue;
        }
        // Log unexpected errors and when reaching the end of input.
        if (err == ec::end_of_input) {
          VAST_DEBUG(self, self->system().render(err));
        } else {
          VAST_ERROR(self, self->system().render(err));
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
      auto bptr = builder(e.type(), table_slice_size);
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
      /// Add data column(s).
      if (auto data = e.data(); !bptr->recursive_add(data, e.type()))
        VAST_WARNING(self, "failed to add data", data);
      ++produced;
      if (bptr->rows() == table_slice_size)
        finish_slice(bptr);
    }
    return {produced, false};
  }

  measurement measurement_;

  void send_report() {
    if (accountant && measurement_.events > 0) {
      performance_report r = {{{std::string{name}, measurement_}}};
      measurement_ = measurement{};
      self->send(accountant, std::move(r));
    }
  }
};

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
template <class Reader>
caf::behavior source(caf::stateful_actor<source_state<Reader>>* self,
                     Reader reader, table_slice_builder_factory factory,
                     size_t table_slice_size) {
  using namespace caf;
  using namespace std::chrono;
  namespace defs = defaults::system;
  // Initialize state.
  self->state.init(self, std::move(reader), factory);
  // Spin up the stream manager for the source.
  self->state.mgr = self->make_continuous_source(
    // init
    [=](bool& done) {
      done = false;
      timestamp now = system_clock::now();
      self->send(self->state.accountant, "source.start", now);
    },
    // get next element
    [=](bool& done, downstream<table_slice_ptr>& out, size_t num) {
      auto& st = self->state;
      auto t = timer::start(st.measurement_);
      // Extract events until the source has exhausted its input or until
      // we have completed a batch.
      auto push_slice = [&](table_slice_ptr slice) {
        out.push(std::move(slice));
      };
      auto [produced, eof] = st.extract_events(num * table_slice_size,
                                               table_slice_size, push_slice);
      t.stop(produced);
      if (eof) {
        VAST_DEBUG(self, "completed slice production");
        done = true;
        st.send_report();
        self->quit();
      }
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
    [=](accountant_type accountant) {
      VAST_DEBUG(self, "sets accountant to", accountant);
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, announce_atom::value,
                 self->state.name);
    },
    [=](sink_atom, const actor& sink) {
      // TODO: Currently, we use a broadcast downstream manager. We need to
      //       implement an anycast downstream manager and use it for the
      //       source, because we mustn't duplicate data.
      VAST_ASSERT(sink != nullptr);
      // We currently support only a single sink.
      // Switch to streaming and periodic reporting mode.
      self->become([=](telemetry_atom) {
        auto& st = self->state;
        st.send_report();
        if (!self->state.mgr->done())
          self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
      });
      self->delayed_send(self, defs::telemetry_rate, telemetry_atom::value);
      VAST_DEBUG(self, "registers sink", sink);
      // Start streaming.
      self->state.mgr->add_outbound_path(sink);
    },
  };
}

/// An event producer with default table slice settings.
template <class Reader>
caf::behavior default_source(caf::stateful_actor<source_state<Reader>>* self,
                             Reader reader) {
  auto slice_size = get_or(self->system().config(), "vast.table-slice-size",
                           defaults::system::table_slice_size);
  auto factory = default_table_slice_builder::make;
  return source(self, std::move(reader), factory, slice_size);
}

} // namespace vast::system
