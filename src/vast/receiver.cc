#include "vast/receiver.h"

#include <caf/all.hpp>
#include "vast/chunk.h"

namespace vast {

using namespace caf;

receiver::receiver()
{
  // We trap exit only to ensure that the sync_send call in the chunk handler
  // doesn't get shot down from an exit message before having received an
  // answer.
  trap_exit(true);

  attach_functor([=](uint32_t)
                 {
                   identifier_ = invalid_actor;
                   archive_ = invalid_actor;
                   index_ = invalid_actor;
                  });
}

void receiver::at_exit(caf::exit_msg const& msg)
{
  quit(msg.reason);
}

message_handler receiver::make_handler()
{
  return
  {
    on(atom("link"), atom("identifier"), arg_match) >> [=](actor const& a)
    {
      identifier_ = a;
    },
    on(atom("link"), atom("archive"), arg_match) >> [=](actor const& a)
    {
      send(a, flow_control::announce{this});
      archive_ = a;
    },
    on(atom("link"), atom("index"), arg_match) >> [=](actor const& a)
    {
      send(a, flow_control::announce{this});
      index_ = a;
    },
    [=](chunk const& chk)
    {
      assert(identifier_);
      assert(archive_);
      assert(index_);

      message last = last_dequeued();
      sync_send(identifier_, atom("request"), chk.events()).then(
        on(atom("id"), arg_match) >> [=](event_id from, event_id to) mutable
        {
          auto n = to - from;
          VAST_LOG_ACTOR_DEBUG("got " << n << " IDs for chunk [" << from <<
                               ", " << to << ")");

          auto msg = last.apply(
              on_arg_match >> [=](chunk& c, actor)
              {
                if (n < c.events())
                {
                  VAST_LOG_ACTOR_ERROR("got " << n << " IDs, needed " <<
                                       c.events());
                  quit(exit::error);
                  return;
                }

                default_bitstream ids;
                ids.append(from, false);
                ids.append(n, true);
                c.ids(std::move(ids));

                auto t = make_message(std::move(c));
                send_tuple(archive_, t);
                send_tuple(index_, t);
              });
        });
    }
  };

}

std::string receiver::name() const
{
  return "receiver";
}

} // namespace vast
