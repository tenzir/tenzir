#include "vast/archive.h"

#include "vast/exception.h"
#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/segment_manager.h"

using namespace cppa;

namespace vast {

archive::archive(std::string const& directory, size_t max_segments)
  : directory_(directory)
{
  segment_manager_ = spawn<segment_manager>(max_segments, directory_);
}

void archive::act()
{
  become(
      on(atom("init")) >> [=]
      {
        path p(directory_);
        if (! exists(p))
        {
          VAST_LOG_ACTOR_INFO("creates new directory " << directory_);
          if (! mkdir(p))
          {
            VAST_LOG_ACTOR_ERROR("failed to create directory " << directory_);
            quit();
          }
        }
        segment_manager_ << last_dequeued();
      },
      on(atom("kill")) >> [=]()
      {
        segment_manager_ << last_dequeued();
        quit();
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
      });
}

char const* archive::description() const
{
  return "archive";
}


} // namespace vast
