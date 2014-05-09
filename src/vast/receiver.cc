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
      on(atom("DOWN"), arg_match) >> [=](uint32_t /* reason */)
      {
        ingestors_.erase(last_sender());
      },
      on_arg_match >> [=](segment& s)
      {
        VAST_LOG_ACTOR_DEBUG("got segment " << s.id());

        // For flow control messages.
        monitor(last_sender());
        ingestors_.insert(last_sender());

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

        archive_ << t;
        index_ << t;

        send(index_, atom("backlog"));
      },
      on(atom("backlog"), arg_match) >> [=](uint64_t backlog, uint64_t last_rate)
      {
        // To make flow control decision, we respect the indexer with and the
        // highest backlog b and its last indexing rate r in events/sec. We can
        // then compute the delay as b / r, which tells us how many seconds
        // behind the indexing is. The ingestors use the delay to decide when
        // to send the next buffered segment.
        auto delay = last_rate > 0 ? backlog / last_rate : 0;
        for (auto& a : ingestors_)
          a << make_any_tuple(atom("delay"), delay);

        VAST_LOG_ACTOR_DEBUG("relays delay to ingestors: " << delay);
      }
    );
}

char const* receiver_actor::description() const
{
  return "receiver";
}

} // namespace vast
