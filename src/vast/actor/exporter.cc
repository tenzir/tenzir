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

using namespace std::string_literals;

namespace vast {

exporter::state::state(local_actor* self)
  : basic_state{self, "exporter"},
    id{uuid::random()} {
}

behavior exporter::make(stateful_actor<state>* self, expression expr,
                        query_options opts) {
  // Prefetches the next chunk and sets the "inflight" chunk status. If we
  // don't have a chunk yet, we look for the chunk corresponding to the last
  // unprocessed hit. If we have a chunk, we try to get the next chunk in the
  // ID space. If no such chunk exists, we try to get a chunk located before
  // the current one. If neither exist, we don't do anything.
  auto prefetch = [=] {
    if (self->state.inflight)
      return;
    if (self->state.current_chunk.events() == 0) {
      auto last = self->state.unprocessed.find_last();
      if (last != bitstream_type::npos) {
        VAST_DEBUG_AT(self, "prefetches chunk for ID", last);
        for (auto& a : self->state.archives)
          self->send(a, last);
        self->state.inflight = true;
      }
    } else {
      VAST_DEBUG_AT(self, "looks for next unprocessed ID after",
                 self->state.current_chunk.meta().ids.find_last());
      auto next = self->state.unprocessed.find_next(
        self->state.current_chunk.meta().ids.find_last());
      if (next != bitstream_type::npos) {
        VAST_DEBUG_AT(self, "prefetches chunk for next ID", next);
        for (auto& a : self->state.archives)
          self->send(a, next);
        self->state.inflight = true;
      } else {
        auto prev = self->state.unprocessed.find_prev(
          self->state.current_chunk.meta().ids.find_first());
        if (prev != bitstream_type::npos) {
          VAST_DEBUG_AT(self, "prefetches chunk for previous ID", prev);
          for (auto& a : self->state.archives)
            self->send(a, prev);
          self->state.inflight = true;
        }
      }
    }
  };
  // Integrate hits from INDEX.
  auto incorporate_hits = [=](bitstream_type const& hits) {
    auto now = time::snapshot();
    auto num_hits = hits.count();
    if (self->state.accountant) {
      if (self->state.total_hits == 0)
        self->send(self->state.accountant, "exporter", "hits.first", now);
      self->send(self->state.accountant, "exporter", "hits.arrived", now);
      self->send(self->state.accountant, "exporter", "hits.count", num_hits);
    }
    VAST_TRACE_AT(self, "got index hit covering", '[' << hits.find_first()
                  << ',' << (hits.find_last() + 1) << ')');
    VAST_ASSERT(!hits.all_zeros());                      // No empty hits
    VAST_ASSERT((self->state.hits & hits).count() == 0); // No duplicates
    self->state.total_hits += num_hits;
    self->state.hits |= hits;
    self->state.unprocessed |= hits;
    prefetch();
  };
  // Handle progress updates from INDEX.
  auto handle_progress =
    [=](progress_atom, uint64_t remaining, uint64_t total) {
        self->state.progress = (total - double(remaining)) / total;
        for (auto& s : self->state.sinks)
          self->send(s, self->state.id, progress_atom::value,
                     self->state.progress, self->state.total_hits);
    };
  // Handle DOWN from source.
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
  // Finish query execution.
  auto complete = [=] {
    auto now = time::snapshot();
    auto runtime = now - self->state.start_time;
    for (auto& s : self->state.sinks)
      self->send(s, self->state.id, done_atom::value, runtime);
    VAST_VERBOSE_AT(self, "took", runtime, "for:", expr);
    if (self->state.accountant) {
      self->send(self->state.accountant, "exporter", "end", now);
      self->send(self->state.accountant, "exporter", "hits",
                 self->state.total_hits);
      self->send(self->state.accountant, "exporter", "results",
                 self->state.total_results);
      self->send(self->state.accountant, "exporter", "chunks",
                 self->state.total_chunks);
      self->send(self->state.accountant, "exporter", "selectivity",
                 double(self->state.total_results) / self->state.total_hits);
    }
    self->quit(exit::done);
  };
  auto extracting = std::make_shared<behavior>(); // break cyclic dependency
  // In "waiting" state, EXPORTER has submitted requests for specific IDs to
  // ARCHIVE and waits for the corresponding chunks to return. As EXPORTER
  // receives a chunk, it instantiates a chunk reader and transitions to
  // "extracting" state.
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
      if (self->state.requested > 0)
        self->send(self, extract_atom::value);
      prefetch();
    }
  };
  // In "idle" state, EXPORTER has received the task from INDEX and hangs
  // around waiting for hits. If EXPORTER receives new hits, it asks ARCHIVE
  // for the corresponding chunks and enters "waiting" state. If INDEX returns
  // with zero hits, EXPORTER terminates directly.
  behavior idle = {
    handle_down,
    handle_progress,
    [=](bitstream_type const& hits) {
      incorporate_hits(hits);
      if (self->state.inflight) {
        VAST_DEBUG_AT(self, "becomes waiting (pending in-flight chunks)");
        self->become(waiting);
      }
    },
    [=](done_atom, time::moment end, time::extent runtime, expression const&) {
      VAST_VERBOSE_AT(self, "completed index interaction in", runtime);
      if (self->state.accountant)
        self->send(self->state.accountant, "exporter", "hits.done", end);
      // If EXPORTER never leaves "idle" state, it hasn't received any hits,
      // unless it's been told to drain.
      VAST_ASSERT(self->state.unprocessed.count() == 0);
      // Otherwise, it has processed hits in "extracting" state and
      // transitioned back to "idle". Since hits can arrive in any state and
      // always cause prefetching of corresponding chunks, a transition back to
      // "idle" ipmlies that there exist no more in-flight chunks.
      // Consequently, there exist no more unprocessed hits and EXPORTER can
      // terminate.
      complete();
    }
  };
  // In "extracting" state, EXPORTER has received at least one chunk from
  // ARCHIVE that it can process and extract results from by peforming a
  // candidate check against the hits.
  *extracting = {
    handle_down,
    handle_progress,
    incorporate_hits,
    [=](stop_atom) {
      VAST_DEBUG_AT(self, "got request to drain and terminate");
      self->state.draining = true;
    },
    [=](extract_atom, uint64_t requested) {
      if (requested == 0)
        requested = max_events;
      auto show_events = [](uint64_t n) {
        return n == (max_events ? "all" : to_string(n)) + "events"s;
      };
      if (self->state.requested == max_events) {
        VAST_WARN(self, "ignores extract request, already getting all events");
        return;
      }
      // Add requested results to the existing outstanding ones.
      if (self->state.requested > 0) {
        if (self->state.requested > max_events - requested)
          self->state.requested = max_events;
        else
          self->state.requested += requested;
        VAST_VERBOSE_AT(self, "raises requested events to",
                        show_events(self->state.requested));
        return;
      }
      self->state.requested = std::min(max_events, requested);
      VAST_DEBUG_AT(self, "extracts", show_events(self->state.requested));
      self->send(self, extract_atom::value);
    },
    [=](extract_atom) {
      VAST_ASSERT(self->state.requested > 0);
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
      std::vector<event> results;
      for (auto id : mask) {
        last = id;
        auto candidate = self->state.reader->read(id);
        ++self->state.chunk_candidates;
        if (candidate) {
          auto& checker = self->state.checkers[candidate->type()];
          // Construct a candidate checker if we don't have one for this type.
          if (is<none>(checker)) {
            auto t = visit(expr::schema_resolver{candidate->type()}, expr);
            if (!t) {
              VAST_ERROR_AT(self, "failed to resolve", expr << ',', t.error());
              self->quit(exit::error);
              return;
            }
            checker = visit(expr::type_resolver{candidate->type()}, *t);
            VAST_DEBUG_AT(self, "resolved AST for",
                          candidate->type() << ':', checker);
          }
          // Perform candidate check and keep event as result on success.
          if (visit(expr::event_evaluator{*candidate}, checker)) {
            results.push_back(std::move(*candidate));
            if (++extracted == self->state.requested)
              break;
          } else {
            VAST_WARN(self, "ignores false positive:", *candidate);
          }
        } else {
          if (candidate.empty())
            VAST_ERROR_AT(self, "failed to extract event", id);
          else
            VAST_ERROR_AT(self, "failed to extract event", id << ':',
                          candidate.error());
          self->quit(exit::error);
          return;
        }
      }
      // Send results to SINKs.
      if (!results.empty()) {
        auto msg = make_message(self->state.id, std::move(results));
        for (auto& s : self->state.sinks)
          self->send(s, msg);
        if (self->state.total_results == 0 && self->state.accountant) {
          auto now = time::snapshot();
          self->send(self->state.accountant, "exporter", "taste", now);
        }
        self->state.total_results += extracted;
        self->state.chunk_results += extracted;
      }
      // Record processed events.
      self->state.requested -= extracted;
      bitstream_type partial{last + 1, true};
      partial &= mask;
      self->state.unprocessed -= partial;
      mask -= partial;
      VAST_DEBUG_AT(self, "extracted", extracted,
                 "events (" << partial.count() << '/' << mask.count(),
                 "processed/remaining hits in current chunk)");
      if (!mask.all_zeros()) {
        // We continue in "extracting" state until we have processed the
        // current chunk in its entirety. But we only do work if the client
        // requested it.
        if (self->state.requested > 0)
          self->send(self, self->current_message());
      } else {
        ++self->state.total_chunks;
        if (self->state.inflight) {
          VAST_DEBUG_AT(self, "becomes waiting (pending in-flight chunks)");
          self->become(waiting);
        } else {
          // After having finished a chunk and having no more in-flight chunks,
          // we're transitioning back to *idle*.
          VAST_DEBUG_AT(self, "becomes idle (no more in-flight chunks)");
          self->become(idle);
        }
        if (self->state.accountant) {
          auto now = time::snapshot();
          self->send(self->state.accountant, "exporter", "chunk.done", now);
          self->send(self->state.accountant, "exporter", "chunk.candidates",
                     self->state.chunk_candidates);
          self->send(self->state.accountant, "exporter", "chunk.results",
                     self->state.chunk_results);
          self->send(self->state.accountant, "exporter", "chunk.events",
                     self->state.current_chunk.events());
        }
        self->state.reader.reset();
        self->state.current_chunk = {};
        self->state.chunk_candidates = 0;
        self->state.chunk_results = 0;
      }
      if (self->state.requested == 0 && self->state.draining) {
        VAST_DEBUG_AT(self, "stops after having drained all requested events");
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
    [=](accountant::type const& accountant) {
      VAST_DEBUG_AT(self, "registers accountant#" << accountant->id());
      self->state.accountant = accountant;
    },
    [=](run_atom) {
      auto now = time::snapshot();
      self->state.start_time = now;
      if (self->state.accountant)
        self->send(self->state.accountant, "exporter", "start", now);
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
      self->become(
        [=](actor const& task) {
          VAST_DEBUG_AT(self, "received task from index");
          self->send(task, subscriber_atom::value, self);
          self->become(idle);
        }
      );
    }
  };
}

} // namespace vast
