#include <caf/all.hpp>

#include "vast/event.h"
#include "vast/actor/importer.h"
#include "vast/concept/printable/vast/error.h"

namespace vast {

using namespace caf;

importer::importer()
  : flow_controlled_actor{"importer"}
{
}

void importer::on_exit()
{
  identifier_ = invalid_actor;
  archive_ = invalid_actor;
  index_ = invalid_actor;
}

behavior importer::make_behavior()
{
  trap_exit(true);
  return
  {
    forward_overload(),
    forward_underload(),
    register_upstream_node(),
    [=](exit_msg const& msg)
    {
      if (downgrade_exit())
        return;
      quit(msg.reason);
    },
    [=](down_msg const& msg)
    {
      if (remove_upstream_node(msg.source))
        return;
      if (msg.source == identifier_)
        identifier_ = invalid_actor;
      else if (msg.source == archive_)
        archive_ = invalid_actor;
      else if (msg.source == index_)
        index_ = invalid_actor;
    },
    [=](put_atom, identifier_atom, actor const& a)
    {
      VAST_DEBUG(this, "registers identifier", a);
      monitor(a);
      identifier_ = a;
    },
    [=](put_atom, archive_atom, actor const& a)
    {
      VAST_DEBUG(this, "registers archive", a);
      send(a, upstream_atom::value, this);
      monitor(a);
      archive_ = a;
    },
    [=](put_atom, index_atom, actor const& a)
    {
      VAST_DEBUG(this, "registers index", a);
      send(a, upstream_atom::value, this);
      monitor(a);
      index_ = a;
    },
    [=](std::vector<event>& events)
    {
      if (identifier_ == invalid_actor)
      {
        VAST_ERROR(this, "has no identifier configured");
        quit(exit::error);
        return;
      }
      if (archive_ == invalid_actor)
      {
        VAST_ERROR(this, "has no archive configured");
        quit(exit::error);
        return;
      }
      if (index_ == invalid_actor)
      {
        VAST_ERROR(this, "has no index configured");
        quit(exit::error);
        return;
      }
      sync_send(identifier_, request_atom::value, uint64_t{events.size()}).then(
        [=](id_atom, event_id from, event_id to) mutable
        {
          auto n = to - from;
          VAST_DEBUG(this, "got", n,
                     "IDs for chunk [" << from << "," << to << ")");
          if (n < events.size())
          {
            VAST_ERROR(this, "got", n, "IDs, needed", events.size());
            quit(exit::error);
            return;
          }
          for (auto& e : events)
            e.id(from++);
          auto t = make_message(std::move(events));
          send(archive_, t);
          send(index_, t);
        },
        [=](error const& e)
        {
          VAST_ERROR(this, e);
          quit(exit::error);
        },
        catch_unexpected()
      );
    },
    catch_unexpected()
  };
}

} // namespace vast
