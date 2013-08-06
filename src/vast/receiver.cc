#include "vast/receiver.h"

#include "vast/logger.h"
#include "vast/segment.h"

namespace vast {

using namespace cppa;

receiver::receiver()
{
  VAST_LOG_VERBOSE("spawning receiver @" << id());
}

void receiver::init()
{
  become(
      on_arg_match >> [=](segment& s)
      {
        VAST_LOG_DEBUG("receiver @" << id() << " got segment " << s.id());
        reply(atom("ack"), s.id());
        // TODO: forward segment to archive
      });
}

} // namespace vast
