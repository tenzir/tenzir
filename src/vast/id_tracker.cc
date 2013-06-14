#include "vast/id_tracker.h"

#include <cassert>
#include "vast/file_system.h"
#include "vast/logger.h"

using namespace cppa;

namespace vast {

id_tracker::id_tracker(std::string file_name)
  : file_name_(std::move(file_name))
{
}

void id_tracker::init()
{
  VAST_LOG_VERBOSE("spawning id tracker @" << id() <<
                   " with id file " << file_name_);
  if (exists(path(string(file_name_))))
  {
    std::ifstream ifs(file_name_);
    if (! ifs)
    {
      quit(); // TODO: use apt actor exit code.
      return;
    }
    ifs >> id_;
    VAST_LOG_INFO("id tracker @" << id() <<
                  " found an id file with highest id " << id_);
  }

  file_.open(file_name_);
  file_.seekp(0);

  become(
    on(atom("request"), arg_match) >> [=](size_t n)
    {
      assert(file_);
      if (std::numeric_limits<uint64_t>::max() - id_ < n)
      {
        VAST_LOG_ERROR(
            "id tracker @" << id() <<
            " has not enough ids available to hand out " << n << " ids");

        reply(atom("id"), atom("failure"));
        return;
      }

      VAST_LOG_DEBUG("id tracker @" << id() <<
                     " hands out [" << id_ << ',' << id_ + n << ')' <<
                     " to @" << last_sender()->id());

      file_ << id_ + n << std::endl;
      if (file_)
        reply(atom("id"), id_, id_ + n);
      else
        reply(atom("id"), atom("failure"));

      id_ += n;

      file_.seekp(0);
    },
    on(atom("kill")) >> [=]
    {
      file_ << id_ << std::endl;
      VAST_LOG_DEBUG("id tracker @" << id() << " saves last event id " << id_);

      if (! file_)
        VAST_LOG_ERROR("id tracker @" << id() <<
                       " could not save current event id");
      quit();
    });
}

void id_tracker::on_exit()
{
  VAST_LOG_VERBOSE("id tracker @" << id() << " terminated");
}

} // namespace vast
