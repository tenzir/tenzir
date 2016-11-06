#include "vast/chunk.hpp"
#include "vast/event.hpp"
#include "vast/actor/archive.hpp"
#include "vast/concept/serializable/io.hpp"
#include "vast/concept/serializable/vast/chunk.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/util/assert.hpp"

namespace vast {

archive::state::state(local_actor* self)
  : basic_state{self, "archive"} {
}

trial<void> archive::state::flush() {
  // Don't touch filesystem if we have nothing to do.
  if (current.empty())
    return nothing;
  auto start = time::snapshot();
  // Store segment on file system.
  if (!exists(dir) && !mkdir(dir))
    return error{"failed to create directory: ", dir};
  auto id = uuid::random();
  auto filename = dir / to_string(id);
  if (!save(filename, current))
    return error{"failed to save segment to ", filename};
  // Record each chunk of segment in registry.
  for (auto& chk : current) {
    auto first = chk.meta().ids.find_first();
    auto last = chk.meta().ids.find_last();
    VAST_ASSERT(first != invalid_event_id && last != invalid_event_id);
    segments.inject(first, last + 1, id);
  }
  cache.insert(std::move(id), std::move(current));
  // Report about the time it took.
  if (accountant) {
    auto stop = time::snapshot();
    auto unit = time::duration_cast<time::microseconds>(stop - start).count();
    auto rate = current_size * 1e6 / unit;
    self->send(accountant, "archive", "flush.rate", rate);
  }
  current = {};
  current_size = 0;
  // Update meta data on filessytem.
  auto t = save(dir / "meta", segments);
  if (!t)
    return error{"failed to write segment meta data ", t.error()};
  return nothing;
}

using flush_response_promise =
  typed_response_promise<either<ok_atom>::or_else<error>>;

using lookup_response_promise =
  typed_response_promise<either<chunk>::or_else<empty_atom, event_id>>;

archive::behavior archive::make(stateful_pointer self, path dir,
                                size_t capacity, size_t max_segment_size,
                                io::compression compression) {
  VAST_ASSERT(max_segment_size > 0);
  self->state.dir = std::move(dir);
  self->state.max_segment_size = max_segment_size;
  self->state.compression = compression;
  self->state.cache.capacity(capacity);
  if (exists(self->state.dir / "meta")) {
    auto t = load(self->state.dir / "meta", self->state.segments);
    if (!t) {
      VAST_ERROR_AT(self, "failed to unarchive meta data:", t.error());
      self->quit(exit::error);
    }
  }
  self->trap_exit(true);
  return {
    [=](exit_msg const& msg) {
      if (self->current_mailbox_element()->mid.is_high_priority()) {
        VAST_DEBUG_AT(self, "delays EXIT from", msg.source);
        self->send(message_priority::normal, self, self->current_message());
      } else {
        VAST_VERBOSE_AT(self, "flushes current segment");
        self->state.flush();
        self->quit(msg.reason);
      }
    },
    [=](accountant::type const& acc) {
      VAST_DEBUG_AT(self, "registers accountant#" << acc->id());
      self->state.accountant = acc;
    },
    [=](std::vector<event> const& events) {
      VAST_DEBUG_AT(self, "got", events.size(),
                    "events [" << events.front().id() << ','
                               << (events.back().id() + 1) << ')');
      auto start = time::snapshot();
      chunk chk{events, self->state.compression};
      if (self->state.accountant) {
        auto stop = time::snapshot();
        auto runtime = stop - start;
        auto unit = time::duration_cast<time::microseconds>(runtime).count();
        auto rate = events.size() * 1e6 / unit;
        self->send(self->state.accountant, "archive", "compression.rate", rate);
      }
      auto too_large = !self->state.current.empty()
                         && self->state.current_size + chk.bytes()
                              >= self->state.max_segment_size;
      if (too_large) {
        VAST_VERBOSE_AT(self, "flushes current segment");
        auto t = self->state.flush();
        if (!t) {
          VAST_ERROR_AT(self, "failed to flush segment:", t.error());
          self->quit(exit::error);
          return;
        }
      }
      self->state.current_size += chk.bytes();
      self->state.current.insert(std::move(chk));
    },
    [=](flush_atom) -> flush_response_promise {
      // FIXME: wait for CAF fix on typed response promises, then uncomment.
      //auto t = self->state.flush();
      //if (!t) {
      //  VAST_ERROR_AT(self, "failed to flush segment:", t.error());
      //  self->quit(exit::error);
      //  return std::move(t.error());
      //}
      //return ok_atom::value;
      flush_response_promise rp = self->make_response_promise();
      auto t = self->state.flush();
      if (!t) {
        VAST_ERROR_AT(self, "failed to flush segment:", t.error());
        self->quit(exit::error);
        rp.deliver(std::move(t.error()));
        return rp;
      }
      rp.deliver(ok_atom::value);
      return rp;
    },
    [=](event_id eid) -> lookup_response_promise {
      lookup_response_promise rp = self->make_response_promise();
      VAST_DEBUG_AT(self, "got request for event", eid);
      // First check the buffered segment in memory.
      for (size_t i = 0; i < self->state.current.size(); ++i)
        if (eid < self->state.current[i].meta().ids.size()
            && self->state.current[i].meta().ids[eid]) {
          VAST_DEBUG_AT(self, "delivers chunk from cache");
          rp.deliver(self->state.current[i]);
          return rp;
        }
      // Then inspect the existing segments.
      if (auto id = self->state.segments.lookup(eid)) {
        auto s = self->state.cache.lookup(*id);
        if (s == nullptr) {
          VAST_DEBUG_AT(self, "experienced cache miss for", *id);
          segment seg;
          auto filename = self->state.dir / to_string(*id);
          auto t = load(filename, seg);
          if (!t) {
            VAST_ERROR_AT(self, "failed to unarchive segment:", t.error());
            self->quit(exit::error);
            rp.deliver(empty_atom::value, eid);
            return rp;
          }
          s = self->state.cache.insert(*id, std::move(seg)).first;
        }
        for (size_t i = 0; i < s->size(); ++i)
          if (eid < (*s)[i].meta().ids.size() && (*s)[i].meta().ids[eid]) {
            VAST_DEBUG_AT(self, "delivers chunk",
                          '[' << (*s)[i].meta().ids.find_first() << ','
                              << (*s)[i].meta().ids.find_last() + 1 << ')');
            rp.deliver((*s)[i]);
            return rp;
          }
        VAST_ASSERT(!"segment must contain looked up id");
      }
      VAST_WARN_AT(self, "no segment for id", eid);
      rp.deliver(empty_atom::value, eid);
      return rp;
    }
  };
}

} // namespace vast
