#include "vast/archive.h"

#include "vast/exception.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/segment_manager.h"

namespace vast {

archive::archive(std::string const& directory, size_t max_segments)
{
  VAST_LOG_VERBOSE("spawning archive @" << id());
  using namespace cppa;
  init_state = (
      on(atom("load")) >> [=]
      {
        path p(directory);
        if (! exists(p))
        {
          VAST_LOG_INFO("archive @" << id() << " creates new directory " << directory);
          if (! mkdir(p))
            VAST_LOG_ERROR("archive @" << id() << 
                           " failed to create directory " << directory);
        }
        segment_manager_ = spawn<segment_manager>(max_segments, directory);
        forward_to(segment_manager_);
      },
      on(atom("get"), atom("ids")) >> [=]
      {
        forward_to(segment_manager_);
      },
      on(atom("get"), arg_match) >> [=](uuid const& /* id */)
      {
        forward_to(segment_manager_);
      },
      on(arg_match) >> [=](segment const& /* s */)
      {
        forward_to(segment_manager_);
      },
      on(atom("kill")) >> [=]()
      {
        // TODO: wait for a signal from the ingestor that all segments have
        // been shipped.
        segment_manager_ << last_dequeued();

        quit();
        VAST_LOG_ERROR("archive @" << id() << " terminated");
      });
}

} // namespace vast
