#include "vast/receiver.h"

#include <caf/all.hpp>
#include "vast/chunk.h"

namespace vast {

using namespace caf;

receiver::receiver(actor tracker, actor archive, actor index, actor search)
  : tracker_(tracker),
    archive_(archive),
    index_(index),
    search_(search)
{
}

message_handler receiver::act()
{
  trap_exit(true);

  attach_functor(
      [=](uint32_t)
      {
        tracker_ = invalid_actor;
        archive_ = invalid_actor;
        index_ = invalid_actor;
        search_ = invalid_actor;
      });

  send(this, atom("backlog"));

  return
  {
    [=](exit_msg const& e)
    {
      quit(e.reason);
    },
    [=](down_msg const&)
    {
      for (auto& a : importers_)
        if (a == last_sender())
        {
          importers_.erase(a);
          break;
        }
    },
    [=](chunk const& chk, actor source)
    {
      if (! importers_.count(source))
      {
        // We keep track of the importers to be able to send flow control
        // messages back to them.
        monitor(source);
        importers_.insert(source);
      }

      send(search_, chk.meta().schema);

      message last = last_dequeued();

      sync_send(tracker_, atom("request"), chk.events()).then(
        on(atom("id"), arg_match) >> [=](event_id from, event_id to) mutable
        {
          VAST_LOG_ACTOR_DEBUG("got " << to - from <<
                               " IDs for chunk [" << from << ", " << to << ")");

          auto msg = last.apply(
              on_arg_match >> [=](chunk& c, actor)
              {
                auto n = to - from;
                if (n < c.events())
                {
                  VAST_LOG_ACTOR_ERROR("got " << n << " IDs, needed " <<
                                       c.events());
                  quit(exit::error);
                  return;
                }

                ewah_bitstream ids;
                ids.append(from, false);
                ids.append(n, true);
                c.ids(std::move(ids));

                auto t = make_message(std::move(c));
                send_tuple(archive_, t);
                send_tuple(index_, t);
              });
        });
    },
    on(atom("backlog")) >> [=]
    {
      send_tuple(index_, last_dequeued());
      delayed_send_tuple(this, std::chrono::milliseconds(100), last_dequeued());
    },
    on(atom("backlog"), arg_match) >> [=](uint64_t chunks, uint64_t backlog)
    {
      // TODO: Make flow-control backlog dynamic.
      auto backlogged = chunks > 10 || backlog > 10000;
      if ((backlogged && ! paused_) || (! backlogged && paused_))
      {
        paused_ = ! paused_;
        for (auto& a : importers_)
          send(a, atom("backlog"), backlogged);

        VAST_LOG_ACTOR_DEBUG(
            "notifies ingestors to " << (paused_ ? "pause" : "resume") <<
            " chunk delivery");
      }
    }
  };

}

std::string receiver::describe() const
{
  return "receiver";
}

} // namespace vast
