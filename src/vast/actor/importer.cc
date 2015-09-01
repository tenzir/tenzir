#include "vast/event.h"
#include "vast/actor/atoms.h"
#include "vast/actor/importer.h"
#include "vast/concept/printable/vast/error.h"

namespace vast {
namespace importer {

state::state(event_based_actor* self) : basic_state{self, "importer"} {
}

behavior actor(stateful_actor<state>* self) {
  using caf::actor;
  self->trap_exit(true);
  auto dependencies_alive = [=] {
    if (self->state.identifier == invalid_actor) {
      VAST_ERROR_AT(self, "has no identifier configured");
      self->quit(exit::error);
      return false;
    }
    if (self->state.archive == invalid_actor) {
      VAST_ERROR_AT(self, "has no archive configured");
      self->quit(exit::error);
      return false;
    }
    if (self->state.index == invalid_actor) {
      VAST_ERROR_AT(self, "has no index configured");
      self->quit(exit::error);
      return false;
    }
    return true;
  };
  return {
    downgrade_exit_msg(self),
    [=](down_msg const& msg) {
      if (msg.source == self->state.identifier)
        self->state.identifier = invalid_actor;
      else if (msg.source == self->state.archive)
        self->state.archive = invalid_actor;
      else if (msg.source == self->state.index)
        self->state.index = invalid_actor;
      else if (msg.source == self->state.controller)
        self->state.controller = invalid_actor;
    },
    [=](put_atom, identifier_atom, actor const& a) {
      VAST_DEBUG_AT(self, "registers identifier", a);
      self->monitor(a);
      self->state.identifier = a;
    },
    [=](put_atom, archive_atom, actor const& a) {
      VAST_DEBUG_AT(self, "registers archive", a);
      self->send(a, upstream_atom::value, self);
      self->monitor(a);
      self->state.archive = a;
      if (self->state.controller)
        self->send(self->state.controller, add_atom::value, self, a);
    },
    [=](put_atom, index_atom, actor const& a) {
      VAST_DEBUG_AT(self, "registers index", a);
      self->send(a, upstream_atom::value, self);
      self->monitor(a);
      self->state.index = a;
      if (self->state.controller)
        self->send(self->state.controller, add_atom::value, self, a);
    },
    [=](put_atom, controller_atom, actor const& c) {
      VAST_DEBUG_AT(self, "registers controller", c);
      self->monitor(c);
      self->state.controller = c;
    },
    [=](std::vector<event>& events) {
      if (! dependencies_alive())
        return;
      event_id needed = events.size();
      self->state.batch = std::move(events);
      self->send(self->state.identifier, request_atom::value, needed);
      self->become(
        keep_behavior,
        downgrade_exit_msg(self),
        [=](std::vector<event> const&) {
          // Wait until we've processed the current batch.
          return skip_message();
        },
        [=](id_atom, event_id from, event_id to)  {
          auto n = to - from;
          self->state.got += n;
          VAST_DEBUG_AT(self, "got", n, "IDs [" << from << "," << to << ")");
          for (auto i = 0u; i < n; ++i)
            self->state.batch[i].id(from++);
          if (self->state.got < needed) {
            if (self->state.got > 0) {
              // We take the front slice and ship it separately until
              // IDENTIFIER has calibrated itself.
              auto remainder = std::vector<event>(
                std::make_move_iterator(self->state.batch.begin() + n),
                std::make_move_iterator(self->state.batch.end()));
              self->state.batch.resize(n);
              auto msg = make_message(std::move(self->state.batch));
              self->send(self->state.archive, msg);
              self->send(self->state.index, msg);
              self->state.batch = std::move(remainder);
            }
            VAST_VERBOSE_AT(self, "asks for more IDs: got", self->state.got,
                            "so far, still need", needed - self->state.got);
            self->send(self->state.identifier, request_atom::value,
                       needed - self->state.got);
          } else {
            // Ship the batch directly if we got enough IDs.
            auto msg = make_message(std::move(self->state.batch));
            self->send(self->state.archive, msg);
            self->send(self->state.index, msg);
            self->state.got = 0;
            self->unbecome();
          }
        },
        [=](error const& e) {
          VAST_ERROR_AT(self, e);
          self->quit(exit::error);
        }
      );
    },
    log_others(self)
  };
}

} // namespace importer
} // namespace vast
