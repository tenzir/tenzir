#include "vast/receiver.h"

#include "vast/logger.h"

#define VAST_THIS_ACTOR "receiver"

namespace vast {

using namespace cppa;

receiver::receiver(actor_ptr tracker, actor_ptr archive, actor_ptr index)
  : tracker_(tracker),
    archive_(archive),
    index_(index)
{
}

void receiver::init()
{
  VAST_LOG_ACT_VERBOSE("receiver", "spawned");

  become(
      on(atom("kill")) >> [=]
      {
        quit();
      },
      on_arg_match >> [=](segment& s)
      {
        VAST_LOG_ACT_DEBUG("receiver", "got segment " << s.id());
        reply(atom("ack"), s.id());
        send(tracker_, atom("request"), uint64_t(s.events()));
        segments_.push_back(std::move(s));
      },
      on(atom("id"), arg_match) >> [=](uint64_t from, uint64_t to)
      {
        VAST_LOG_ACT_DEBUG("receiver",
                           "got " << to - from <<
                           " IDs in [" << from << ", " << to << ")");
        assert(! segments_.empty());
        auto& s = segments_.front();
        auto n = to - from;
        assert(n <= s.events());
        if (n < s.events())
        {
          VAST_LOG_ACT_ERROR("receiver",
                             "did not get enough ids " <<
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

void receiver::on_exit()
{
  VAST_LOG_ACT_VERBOSE("receiver", "terminated");
}

} // namespace vast
