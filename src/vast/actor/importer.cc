#include <caf/all.hpp>

#include "vast/event.h"
#include "vast/actor/importer.h"
#include "vast/concept/printable/vast/error.h"

namespace vast {

using namespace caf;

importer::importer() : flow_controlled_actor{"importer"} {
}

void importer::on_exit() {
  identifier_ = invalid_actor;
  archive_ = invalid_actor;
  index_ = invalid_actor;
}

behavior importer::make_behavior() {
  trap_exit(true);
  auto exit_handler = [=](exit_msg const& msg) {
    if (downgrade_exit())
      return;
    quit(msg.reason);
  };
  auto dependencies_alive = [=] {
    if (identifier_ == invalid_actor) {
      VAST_ERROR(this, "has no identifier configured");
      quit(exit::error);
      return false;
    }
    if (archive_ == invalid_actor) {
      VAST_ERROR(this, "has no archive configured");
      quit(exit::error);
      return false;
    }
    if (index_ == invalid_actor) {
      VAST_ERROR(this, "has no index configured");
      quit(exit::error);
      return false;
    }
    return true;
  };
  return {
    forward_overload(),
    forward_underload(),
    register_upstream_node(),
    exit_handler,
    [=](down_msg const& msg) {
      if (remove_upstream_node(msg.source))
        return;
      if (msg.source == identifier_)
        identifier_ = invalid_actor;
      else if (msg.source == archive_)
        archive_ = invalid_actor;
      else if (msg.source == index_)
        index_ = invalid_actor;
    },
    [=](put_atom, identifier_atom, actor const& a) {
      VAST_DEBUG(this, "registers identifier", a);
      monitor(a);
      identifier_ = a;
    },
    [=](put_atom, archive_atom, actor const& a) {
      VAST_DEBUG(this, "registers archive", a);
      send(a, upstream_atom::value, this);
      monitor(a);
      archive_ = a;
    },
    [=](put_atom, index_atom, actor const& a) {
      VAST_DEBUG(this, "registers index", a);
      send(a, upstream_atom::value, this);
      monitor(a);
      index_ = a;
    },
    [=](std::vector<event>& events) {
      if (! dependencies_alive())
        return;
      event_id needed = events.size();
      batch_ = std::move(events);
      send(identifier_, request_atom::value, needed);
      become(
        keep_behavior,
        [=](std::vector<event> const&) {
          // Wait until we've processed the current batch.
          return skip_message();
        },
        [=](id_atom, event_id from, event_id to)  {
          auto n = to - from;
          got_ += n;
          VAST_DEBUG(this, "got", n, "IDs [" << from << "," << to << ")");
          for (auto i = 0u; i < n; ++i)
            batch_[i].id(from++);
          if (got_ < needed) {
            if (got_ > 0) {
              // We take the front slice and ship it separately until
              // IDENTIFIER has calibrated itself.
              auto remainder = std::vector<event>(
                std::make_move_iterator(batch_.begin() + n),
                std::make_move_iterator(batch_.end()));
              batch_.resize(n);
              auto msg = make_message(std::move(batch_));
              send(archive_, msg);
              send(index_, msg);
              batch_ = std::move(remainder);
            }
            VAST_VERBOSE(this, "asks for more IDs: got", got_,
                         "so far, still need", needed - got_);
            send(identifier_, request_atom::value, needed - got_);
          } else {
            // Ship the batch directly if we got enough IDs.
            auto msg = make_message(std::move(batch_));
            send(archive_, msg);
            send(index_, msg);
            got_ = 0;
            unbecome();
          }
        },
        [=](error const& e) {
          VAST_ERROR(this, e);
          quit(exit::error);
        }
      );
    },
    catch_unexpected()
  };
}

} // namespace vast
