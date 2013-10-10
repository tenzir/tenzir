#include "vast/archive.h"

#include <cppa/cppa.hpp>
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/exception.h"
#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/segment_manager.h"

using namespace cppa;

namespace vast {

archive::archive(std::string const& directory, size_t max_segments)
  : directory_(directory)
{
  segment_manager_ = spawn<segment_manager_actor>(max_segments, directory_);
}

void archive::act()
{
  trap_exit(true);
  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        // TODO: write ranges to disk.
        quit(reason);
      },
      on_arg_match >> [=](uuid const&)
      {
        forward_to(segment_manager_);
      },
      on_arg_match >> [=](event_id eid)
      {
        if (auto id = ranges_.lookup(eid))
          send(segment_manager_, *id, last_sender());
        else
          reply(eid);
      },
      on(arg_match) >> [=](segment const& s)
      {
        if (! ranges_.insert(s.base(), s.base() + s.events(), s.id()))
        {
          VAST_LOG_ACTOR_ERROR("failed to register segment " << s.id());
          quit(exit::error);
          return;
        }
        forward_to(segment_manager_);
        reply(atom("segment"), atom("ack"), s.id());
      });

  path p(directory_);
  if (! exists(p))
  {
    VAST_LOG_ACTOR_INFO("creates new directory " << directory_);
    if (! mkdir(p))
    {
      VAST_LOG_ACTOR_ERROR("failed to create directory " << directory_);
      quit(exit::error);
      return;
    }
  }
  else
  {
    // TODO: load ranges from disk.
  }
}

char const* archive::description() const
{
  return "archive";
}


} // namespace vast
