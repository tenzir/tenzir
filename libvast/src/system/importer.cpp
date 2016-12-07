#include "vast/concept/printable/vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/importer.hpp"

using namespace caf;

namespace vast {
namespace system {

behavior importer(stateful_actor<importer_state>* self) {
  return {
    [=](down_msg const& msg) {
      if (msg.source == self->state.identifier)
        self->state.identifier = {};
      else if (msg.source == self->state.archive)
        self->state.archive = archive_type{};
      else if (msg.source == self->state.index)
        self->state.index = {};
    },
    [=](put_atom, identifier_atom, actor const& a) {
      VAST_DEBUG(self, "registers identifier");
      self->monitor(a);
      self->state.identifier = a;
    },
    [=](archive_type const& a) {
      VAST_DEBUG(self, "registers archive");
      self->monitor(a);
      self->state.archive = a;
    },
    [=](put_atom, index_atom, actor const& a) {
      VAST_DEBUG(self, "registers index", a);
      self->monitor(a);
      self->state.index = a;
    },
    [=](std::vector<event>& events) {
      VAST_DEBUG(self, "got", events.size(), "events");
      if (!self->state.identifier) {
        self->quit(make_error(ec::unspecified, "no identifier configured"));
        return;
      }
      if (!self->state.archive) {
        self->quit(make_error(ec::unspecified, "no archive configured"));
        return;
      }
      if (!self->state.index) {
        self->quit(make_error(ec::unspecified, "no index configured"));
        return;
      }
      event_id needed = events.size();
      self->state.batch = std::move(events);
      self->send(self->state.identifier, request_atom::value, needed);
      self->become(
        keep_behavior,
        [=](id_atom, event_id from, event_id to)  {
          auto n = to - from;
          self->state.got += n;
          VAST_DEBUG(self, "got", n, "IDs [" << from << "," << to << ")");
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
              // FIXME: how to make this type-safe?
              auto archive = actor_cast<actor>(self->state.archive);
              self->send(archive, msg);
              self->send(self->state.index, msg);
              self->state.batch = std::move(remainder);
            }
            VAST_DEBUG(self, "asks for more IDs: got", self->state.got,
                       "so far, still need", needed - self->state.got);
            self->send(self->state.identifier, request_atom::value,
                       needed - self->state.got);
          } else {
            // Ship the batch directly if we got enough IDs.
            auto msg = make_message(std::move(self->state.batch));
            // FIXME: how to make this type-safe?
            auto archive = actor_cast<actor>(self->state.archive);
            self->send(archive, msg);
            self->send(self->state.index, msg);
            self->state.got = 0;
            self->unbecome();
          }
        }
      );
    }
  };
}

} // namespace system
} // namespace vast
