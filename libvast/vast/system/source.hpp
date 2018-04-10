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

#include <caf/actor_pool.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/stream_source.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/schema.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/atoms.hpp"

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
  std::vector<event> events;
  expression filter;
  std::unordered_map<type, expression> checkers;
  std::chrono::steady_clock::time_point start;
  accountant_type accountant;
  caf::actor sink;
  Reader reader;
  const char* name = "source";
  caf::stream_source_ptr<caf::broadcast_downstream_manager<event>> mgr;
};

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
template <class Reader>
caf::behavior
source(caf::stateful_actor<source_state<Reader>>* self, Reader&& reader) {
  using namespace caf;
  using namespace std::chrono;
  // Initialize state.
  self->state.reader = std::move(reader);
  self->state.name = self->state.reader.name();
  // Fetch accountant from the registry.
  if (auto acc = self->system().registry().get(accountant_atom::value))
    self->state.accountant = actor_cast<accountant_type>(acc);
  // We link to the importers and fail for the same reason, but still report to
  // the accountant.
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      auto& st = self->state;
      if (st.accountant) {
        timestamp now = system_clock::now();
        self->send(st.accountant, "source.end", now);
      }
      self->quit(msg.reason);
    }
  );
  // Spin up the stream manager for the source.
  self->state.mgr = self->make_source(
    // init
    [=](bool& done) {
      done = false;
      timestamp now = system_clock::now();
      self->send(self->state.accountant, "source.start", now);
    },
    // get next element
    [=](bool& done, downstream<event>& out, size_t num) {
      auto& st = self->state;
      // Extract events until the source has exhausted its input or until we
      // have completed a batch.
      auto start = steady_clock::now();
      size_t produced = 0;
      while (produced < num) {
        auto e = st.reader.read();
        if (e) {
          if (!is<none>(st.filter)) {
            auto& checker = st.checkers[e->type()];
            if (is<none>(checker)) {
              auto x = tailor(st.filter, e->type());
              VAST_ASSERT(x);
              checker = std::move(*x);
            }
            if (!visit(event_evaluator{*e}, checker))
              continue;
          }
          ++produced;
          out.push(std::move(*e));
        } else if (!e.error()) {
          continue; // Try again.
        } else {
          if (e.error() == ec::parse_error) {
            VAST_WARNING(self->system().render(e.error()));
            continue; // Just skip bogous events.
          } else if (e.error() == ec::end_of_input) {
            VAST_INFO(self->system().render(e.error()));
          } else {
            VAST_ERROR(self->system().render(e.error()));
          }
          done = true;
          break;
        }
      }
      auto stop = steady_clock::now();
      // Produce stats for this run.
      if (produced > 0) {
        auto runtime = stop - start;
        auto unit = duration_cast<microseconds>(runtime).count();
        auto rate = self->state.events.size() * 1e6 / unit;
        auto events = uint64_t{produced};
        VAST_INFO(self, "produced", events, "events in", runtime,
                  '(' << size_t(rate), "events/sec)");
        if (st.accountant) {
          auto rt = duration_cast<timespan>(runtime);
          self->send(st.accountant, "source.batch.runtime", rt);
          self->send(st.accountant, "source.batch.events", events);
          self->send(st.accountant, "source.batch.rate", rate);
        }
      }
      // TODO: if the source is unable to generate new events then we should
      //       trigger CAF to poll the source after a predefined interval of
      //       time again via delayed_send
    },
    // done?
    [](const bool& done) {
      return done;
    }
  ).ptr();
  auto eu = self->system().dummy_execution_unit();
  self->state.sink = actor_pool::make(eu, actor_pool::round_robin());
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
      VAST_ASSERT(self->state.mgr->out().num_paths() == 0);
      VAST_ASSERT(sink);
      VAST_DEBUG(self, "registers sink", sink);
      self->link_to(sink);
      self->state.mgr->add_outbound_path(sink);
    },
  };
}

} // namespace vast::system

