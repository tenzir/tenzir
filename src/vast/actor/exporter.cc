#include "vast/event.h"
#include "vast/logger.h"
#include "vast/actor/atoms.h"
#include "vast/actor/exporter.h"
#include "vast/concept/printable/stream.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/expression.h"
#include "vast/concept/printable/vast/time.h"
#include "vast/expr/evaluator.h"
#include "vast/expr/resolver.h"
#include "vast/util/assert.h"

namespace vast {

exporter::state::state(local_actor* self)
  : basic_state{self, "exporter"},
    id{uuid::random()} {
}

// Prefetches the next chunk and sets the "inflight" chunk status. If we
// don't have a chunk yet, we look for the chunk corresponding to the last
// unprocessed hit. If we have a chunk, we try to get the next chunk in the
// ID space. If no such chunk exists, we try to get a chunk located before
// the current one. If neither exist, we don't do anything.
void exporter::state::prefetch() {
  if (inflight)
    return;
  if (current_chunk.events() == 0) {
    auto last = unprocessed.find_last();
    if (last != bitstream_type::npos) {
      VAST_DEBUG_AT(self, "prefetches chunk for ID", last);
      for (auto& a : archives)
        self->send(a, last);
      inflight = true;
    }
  } else {
    VAST_DEBUG_AT(self, "looks for next unprocessed ID after",
               current_chunk.meta().ids.find_last());
    auto next = unprocessed.find_next(current_chunk.meta().ids.find_last());
    if (next != bitstream_type::npos) {
      VAST_DEBUG_AT(self, "prefetches chunk for next ID", next);
      for (auto& a : archives)
        self->send(a, next);
      inflight = true;
    } else {
      auto prev = unprocessed.find_prev(current_chunk.meta().ids.find_first());
      if (prev != bitstream_type::npos) {
        VAST_DEBUG_AT(self, "prefetches chunk for previous ID", prev);
        for (auto& a : archives)
          self->send(a, prev);
        inflight = true;
      }
    }
  }
}

behavior exporter::make(stateful_actor<state>* self, expression expr,
                        query_options opts) {
  auto incorporate_hits = [=](bitstream_type const& hits) {
    auto stop = time::snapshot();
    VAST_DEBUG_AT(self, "got index hit covering", '[' << hits.find_first()
                  << ',' << (hits.find_last() + 1) << ')');
    VAST_ASSERT(!hits.all_zeros());           // Empty hits are useless.
    VAST_ASSERT((self->state.hits & hits).count() == 0); // Duplicates, too.
    auto runtime = stop - self->state.start_time;
    if (self->state.total_hits == 0 && self->state.accountant) {
      self->send(self->state.accountant, "exporter", "first-hit", runtime);
    }
    self->state.total_hits += hits.count();
    self->state.hits |= hits;
    self->state.unprocessed |= hits;
    self->state.prefetch();
  };
  auto handle_progress =
    [=](progress_atom, uint64_t remaining, uint64_t total) {
        self->state.progress = (total - double(remaining)) / total;
        for (auto& s : self->state.sinks)
          self->send(s, self->state.id, progress_atom::value,
                     self->state.progress, self->state.total_hits);
    };
  auto handle_down = [=](down_msg const& msg) {
    VAST_DEBUG_AT("got DOWN from", msg.source);
    auto a = actor_cast<actor>(msg.source);
    if (self->state.archives.erase(a) > 0)
      return;
    if (self->state.indexes.erase(a) > 0)
      return;
    if (self->state.sinks.erase(a) > 0)
      return;
  };
  auto complete = [=] {
    auto runtime = time::snapshot() - self->state.start_time;
    for (auto& s : self->state.sinks)
      self->send(s, self->state.id, done_atom::value, runtime);
    VAST_VERBOSE_AT(self, "took", runtime, "for:", expr);
    if (self->state.accountant)
      self->send(self->state.accountant, "exporter", "completion", runtime);
    self->quit(exit::done);
  };
  auto extracting = std::make_shared<behavior>(); // break cyclic dependency
  behavior waiting = {
    handle_down,
    handle_progress,
    incorporate_hits,
    [=](chunk const& chk) {
      VAST_DEBUG_AT(self, "got chunk [" << chk.base() << ','
                                     << (chk.base() + chk.events()) << ")");
      self->state.inflight = false;
      self->state.current_chunk = chk;
      VAST_ASSERT(!self->state.reader);
      self->state.reader =
        std::make_unique<chunk::reader>(self->state.current_chunk);
      VAST_DEBUG_AT(self, "becomes extracting");
      self->become(*extracting);
      if (self->state.pending > 0)
        self->send(self, extract_atom::value);
      self->state.prefetch();
    }
  };
  behavior idle = {
    handle_down,
    handle_progress,
    [=](actor const& task) {
      VAST_TRACE(self, "received task from index");
      self->send(task, subscriber_atom::value, self);
    },
    [=](bitstream_type const& hits) {
      incorporate_hits(hits);
      if (self->state.inflight) {
        VAST_DEBUG_AT(self, "becomes waiting (pending in-flight chunks)");
        self->become(waiting);
      }
    },
    [=](done_atom, time::extent runtime, expression const&) // from INDEX
    {
      VAST_VERBOSE_AT(self, "completed index interaction in", runtime);
      if (self->state.accountant)
        self->send(self->state.accountant, "exporter", "last-hit", runtime);
      complete();
    }
  };
  *extracting = {
    handle_down,
    handle_progress,
    incorporate_hits,
    [=](stop_atom) {
      VAST_DEBUG_AT(self, "got request to drain and terminate");
      self->state.draining = true;
    },
    [=](extract_atom, uint64_t requested) {
      auto show_events = [](uint64_t n) -> std::string {
        return n == max_events ? "all" : to_string(n);
      };
      if (requested == 0)
        requested = max_events;
      VAST_DEBUG_AT(self, "got request to extract", show_events(requested),
                 "events (" << to_string(self->state.pending), "pending)");
      if (self->state.pending == max_events) {
        VAST_WARN(self, "ignores extract request, already getting all events");
        return;
      }
      if (self->state.pending > 0) {
        if (self->state.pending > max_events - requested)
          self->state.pending = max_events;
        else
          self->state.pending += requested;
        VAST_VERBOSE_AT(self, "raises pending events to",
                        show_events(self->state.pending), "events");
        return;
      }
      self->state.pending = std::min(max_events, requested);
      VAST_DEBUG_AT(self, "extracts", show_events(self->state.pending),
                    "events");
      self->send(self, extract_atom::value);
    },
    [=](extract_atom) {
      VAST_ASSERT(self->state.pending > 0);
      VAST_ASSERT(self->state.reader);
      // We construct a new mask for each extraction request, because hits may
      // continuously update in every state.
      bitstream_type mask{self->state.current_chunk.meta().ids};
      mask &= self->state.unprocessed;
      VAST_ASSERT(mask.count() > 0);
      // Go through the current chunk and perform a candidate check for each
      // hit, relaying event to the sink on success.
      auto extracted = uint64_t{0};
      auto last = event_id{0};
      for (auto id : mask) {
        last = id;
        auto e = self->state.reader->read(id);
        if (e) {
          auto& checker = self->state.expressions[e->type()];
          if (is<none>(checker)) {
            auto t = visit(expr::schema_resolver{e->type()}, expr);
            if (!t) {
              VAST_ERROR_AT(self, "failed to resolve", expr << ',', t.error());
              self->quit(exit::error);
              return;
            }
            checker = visit(expr::type_resolver{e->type()}, *t);
            VAST_DEBUG_AT(self, "resolved AST for", e->type() << ':', checker);
          }
          if (visit(expr::event_evaluator{*e}, checker)) {
            auto msg = make_message(self->state.id, std::move(*e));
            for (auto& s : self->state.sinks)
              self->send(s, msg);
            if (self->state.total_results == 0 && self->state.accountant) {
              auto runtime = time::snapshot() - self->state.start_time;
              self->send(self->state.accountant, "exporter", "taste", runtime);
            }
            ++self->state.total_results;
            if (++extracted == self->state.pending)
              break;
          } else {
            VAST_WARN(self, "ignores false positive:", *e);
          }
        } else {
          if (e.empty())
            VAST_ERROR_AT(self, "failed to extract event", id);
          else
            VAST_ERROR_AT(self, "failed to extract event", id << ':',
                          e.error());
          self->quit(exit::error);
          return;
        }
      }
      self->state.pending -= extracted;
      bitstream_type partial{last + 1, true};
      partial &= mask;
      self->state.processed |= partial;
      self->state.unprocessed -= partial;
      mask -= partial;
      VAST_DEBUG_AT(self, "extracted", extracted,
                 "events (" << partial.count() << '/' << mask.count(),
                 "processed/remaining hits in current chunk)");
      VAST_ASSERT(!mask.empty());
      if (self->state.pending == 0 && self->state.draining) {
        VAST_DEBUG_AT(self, "stops after having drained all pending events");
        complete();
      }
      if (!mask.all_zeros()) {
        // We continue extracting until we have processed all requested
        // events.
        if (self->state.pending > 0)
          self->send(self, self->current_message());
        return;
      }
      self->state.reader.reset();
      self->state.current_chunk = {};
      if (self->state.inflight) {
        VAST_DEBUG_AT(self, "becomes waiting (pending in-flight chunks)");
        self->become(waiting);
      } else {
        // No in-flight chunk implies that we have no more unprocessed hits,
        // because arrival of new hits automatically triggers prefetching.
        VAST_ASSERT(!self->state.unprocessed.empty());
        VAST_ASSERT(self->state.unprocessed.all_zeros());
        VAST_DEBUG_AT(self, "becomes idle (no in-flight chunks)");
        self->become(idle);
        if (self->state.progress == 1.0 && self->state.unprocessed.count() == 0)
          complete();
      }
    }
  };
  return {
    handle_down,
    [=](put_atom, archive_atom, actor const& a) {
      VAST_DEBUG_AT(self, "registers archive", a);
      self->monitor(a);
      self->state.archives.insert(a);
    },
    [=](put_atom, index_atom, actor const& a) {
      VAST_DEBUG_AT(self, "registers index", a);
      self->monitor(a);
      self->state.indexes.insert(a);
    },
    [=](put_atom, sink_atom, actor const& a) {
      VAST_DEBUG_AT(self, "registers sink", a);
      self->monitor(a);
      self->state.sinks.insert(a);
    },
    [=](run_atom) {
      if (self->state.archives.empty()) {
        VAST_ERROR_AT(self, "cannot run without archive(s)");
        self->quit(exit::error);
        return;
      }
      if (self->state.indexes.empty()) {
        VAST_ERROR_AT(self, "cannot run without index(es)");
        self->quit(exit::error);
        return;
      }
      for (auto& i : self->state.indexes) {
        VAST_DEBUG_AT(self, "sends query to index" << i);
        self->send(i, expr, opts, self);
      }
      self->become(idle);
      self->state.start_time = time::snapshot();
    }
  };
}

} // namespace vast
