#include "vast/actor/receiver.h"

#include <caf/all.hpp>
#include "vast/chunk.h"

namespace vast {

using namespace caf;

receiver::receiver()
  : flow_controlled_actor{"receiver"}
{
}

void receiver::on_exit()
{
  identifier_ = invalid_actor;
  archive_ = invalid_actor;
  index_ = invalid_actor;
}

behavior receiver::make_behavior()
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
        quit(exit::error);
      else if (msg.source == archive_)
        archive_ = invalid_actor;
      else if (msg.source == index_)
        index_ = invalid_actor;
    },
    [=](set_atom, identifier_atom, actor const& a)
    {
      VAST_DEBUG(this, "registers identifier", a);
      monitor(a);
      identifier_ = a;
      return ok_atom::value;
    },
    [=](add_atom, archive_atom, actor const& a)
    {
      VAST_DEBUG(this, "registers archive", a);
      send(a, upstream_atom::value, this);
      monitor(a);
      archive_ = a;
      return ok_atom::value;
    },
    [=](add_atom, index_atom, actor const& a)
    {
      VAST_DEBUG(this, "registers index", a);
      send(a, upstream_atom::value, this);
      monitor(a);
      index_ = a;
      return ok_atom::value;
    },
    [=](chunk& chk)
    {
      assert(identifier_ != invalid_actor);
      if (archive_ == invalid_actor)
      {
        VAST_ERROR(this, "not linked to archive");
        quit(exit::error);
        return;
      }
      if (index_ == invalid_actor)
      {
        VAST_ERROR(this, "not linked to index");
        quit(exit::error);
        return;
      }
      sync_send(identifier_, request_atom::value, chk.events()).then(
        [=](id_atom, event_id from, event_id to) mutable
        {
          auto n = to - from;
          VAST_DEBUG(this, "got", n,
                     "IDs for chunk [" << from << "," << to << ")");
          if (n < chk.events())
          {
            VAST_ERROR(this, "got", n, "IDs, needed", chk.events());
            quit(exit::error);
            return;
          }
          default_bitstream ids;
          ids.append(from, false);
          ids.append(n, true);
          chk.ids(std::move(ids));
          auto t = make_message(std::move(chk));
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
