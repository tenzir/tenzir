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

behavior receiver_actor::act()
{
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

      auto id = s.id();
      send(tracker_, atom("request"), uint64_t(s.events()));
      segments_.push(std::move(s));

      return make_any_tuple(atom("ack"), id);
    },
    on(atom("id"), arg_match) >> [=](event_id from, event_id to)
    {
      auto n = to - from;
      assert(! segments_.empty());
      VAST_LOG_ACTOR_DEBUG("got " << n <<
                           " IDs in [" << from << ", " << to << ")");

      auto& s = segments_.front();
      if (n < s.events())
      {
        VAST_LOG_ACTOR_ERROR("did not get enough ids " <<
                             "(got " << n << ", needed " << s.events());
        quit(exit::error);
      }

      s.base(from);
      auto t = make_any_tuple(std::move(s));
      segments_.pop();

      send_tuple(archive_, t);
      send_tuple(index_, t);
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

char const* receiver_actor::describe() const
{
  return "receiver";
}

} // namespace vast
