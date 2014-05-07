#include "vast/receiver.h"

#include <cppa/cppa.hpp>

namespace vast {

using namespace cppa;

receiver_actor::receiver_actor(actor_ptr tracker,
                               actor_ptr archive,
                               actor_ptr index,
                               actor_ptr search)
  : tracker_(tracker),
    archive_(archive),
    index_(index),
    search_(search)
{
}

void receiver_actor::act()
{
  become(
      on_arg_match >> [=](segment& s)
      {
        VAST_LOG_ACTOR_DEBUG("got segment " << s.id());

        send(search_, s.schema());

        auto id = s.id();
        send(tracker_, atom("request"), uint64_t(s.events()));
        segments_.push_back(std::move(s));

        return make_any_tuple(atom("ack"), id);
      },
      on(atom("id"), arg_match) >> [=](event_id from, event_id to)
      {
        VAST_LOG_ACTOR_DEBUG("got " << to - from <<
                             " IDs in [" << from << ", " << to << ")");
        assert(! segments_.empty());
        auto& s = segments_.front();
        auto n = to - from;
        assert(n <= s.events());
        if (n < s.events())
        {
          VAST_LOG_ACTOR_ERROR("did not get enough ids " <<
                               "(got " << n << ", needed " << s.events());
          quit();
        }
        s.base(from);
        auto t = make_any_tuple(std::move(s));
        archive_ << t;
        index_ << t;
        segments_.pop_front();
      });
}

char const* receiver_actor::description() const
{
  return "receiver";
}

} // namespace vast
