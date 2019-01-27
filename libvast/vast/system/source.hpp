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
#include "vast/table_slice_builder_factory.hpp"

namespace vast::system {

/// The source state.
/// @tparam Reader The reader type, which must model the *Reader* concept.
template <class Reader, class Self = caf::event_based_actor>
struct source_state {
  // -- member types -----------------------------------------------------------

  using downstream_manager
    = caf::broadcast_downstream_manager<table_slice_ptr>;

  // -- constructors, destructors, and assignment operators --------------------

  source_state(Self* selfptr) : self(selfptr), initialized(false) {
    // nop
  }

  ~source_state() {
    self->send(accountant, "source.end", caf::make_timestamp());
    if (initialized)
      reader.~Reader();
  }

  // -- member variables -------------------------------------------------------

  /// Filters events, i.e., causes the source to drop all matching events.
  expression filter;

  /// Maps types to the tailored filter.
  std::unordered_map<type, expression> checkers;

  /// Actor for collecting statistics.
  accountant_type accountant;

  /// Wraps the format-specific parser.
  union {
    Reader reader;
  };

  /// Generates layout-specific table slice builders.
  vast::factory<table_slice_builder>::signature factory;

  /// Maps layout type names to table slice builders.
  std::map<std::string, table_slice_builder_ptr> builders;

  /// Pretty name for log files.
  const char* name = "source";

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr;

  /// Points to the owning actor.
  Self* self;

  /// Stores whether `reader` is constructed.
  bool initialized;

  // -- utility functions ------------------------------------------------------

  /// Initializes the state.
  void init(Reader rd, vast::factory<table_slice_builder>::signature f) {
    // Initialize members from given arguments.
    name = reader.name();
    factory = f;
    new (&reader) Reader(std::move(rd));
    initialized = true;
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

  measurement measurement_;

  void send_report() {
    if (accountant && measurement_.events > 0) {
      auto r = performance_report{{{std::string{name}, measurement_}}};
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
                     Reader reader,
                     factory<table_slice_builder>::signature factory,
                     size_t table_slice_size) {
  using namespace caf;
  using namespace std::chrono;
  namespace defs = defaults::system;
  // Initialize state.
  self->state.init(std::move(reader), factory);
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
      auto push_slice = [&](table_slice_ptr x) { out.push(std::move(x)); };
      // We can produce up to num * table_slice_size events per run.
      auto [err,produced] = st.reader.read(num * table_slice_size,
                                         table_slice_size, push_slice);
      // TODO: if the source is unable to generate new events (returns 0)
      //       then we should trigger CAF to poll the source after a
      //       predefined interval of time again, e.g., via delayed_send
      t.stop(produced);
      if (err != caf::none) {
        done = true;
        st.send_report();
        self->quit();
      }
    },
    // done?
    [](const bool& done) {
      return done;
    }
  );
  return {
    [=](get_atom, schema_atom) {
      return self->state.reader.schema();
    },
    [=](put_atom, schema& sch) -> result<void> {
      if (auto err = self->state.reader.schema(std::move(sch)))
        return err;
      return caf::unit;
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
