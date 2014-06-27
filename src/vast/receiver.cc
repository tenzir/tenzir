#include "vast/receiver.h"

#include <cppa/cppa.hpp>

namespace vast {

using namespace cppa;

receiver_actor::receiver_actor(actor tracker,
                               actor archive,
                               actor index,
                               actor search)
  : tracker_(tracker),
    archive_(archive),
    index_(index),
    search_(search)
{
}

partial_function receiver_actor::act()
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
      for (auto& a : ingestors_)
        if (a == last_sender())
        {
          ingestors_.erase(a);
          break;
        }
    },
    [=](segment& s, actor source)
    {
      VAST_LOG_ACTOR_DEBUG("got segment " << s.id());

      // For flow control messages.
      monitor(source);
      ingestors_.insert(source);

      send(search_, s.schema());

      any_tuple last = last_dequeued();

      sync_send(tracker_, atom("request"), uint64_t{s.events()}).then(
        on(atom("id"), arg_match) >> [=](event_id from, event_id to)
        {
          VAST_LOG_ACTOR_DEBUG("got " << to - from <<
                               " IDs in [" << from << ", " << to << ")");
          match(last)(
              on_arg_match >> [=](segment& s, actor)
              {
                auto n = to - from;
                if (n < s.events())
                {
                  VAST_LOG_ACTOR_ERROR("got " << n << " IDs, needed " <<
                                       s.events());

                  quit(exit::error);
                  return make_any_tuple(atom("nack"), s.id());
                }

                s.base(from);

                auto id = s.id();
                auto t = make_any_tuple(std::move(s));
                send_tuple(archive_, t);
                send_tuple(index_, t);

                return make_any_tuple(atom("ack"), id);
              });
        });
    },
    on(atom("backlog")) >> [=]
    {
      send_tuple(index_, last_dequeued());
      delayed_send_tuple(this, std::chrono::milliseconds(100), last_dequeued());
    },
    on(atom("backlog"), arg_match) >> [=](uint64_t segments, uint64_t backlog)
    {
      // TODO: Make flow-control backlog dynamic.
      auto backlogged = segments > 0 || backlog > 10000;
      if ((backlogged && ! paused_) || (! backlogged && paused_))
      {
        paused_ = ! paused_;
        for (auto& a : ingestors_)
          send(a, atom("backlog"), backlogged);

        VAST_LOG_ACTOR_DEBUG("notifies ingestors to " <<
                             (paused_ ? "pause" : "resume") << " processing");
      }
    }
  };

}

std::string receiver_actor::describe() const
{
  return "receiver";
}

} // namespace vast
