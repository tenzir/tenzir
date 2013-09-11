#include "vast/id_tracker.h"

#include <cassert>
#include "vast/file_system.h"
#include "vast/logger.h"

using namespace cppa;

namespace vast {

id_tracker::id_tracker(std::string filename)
  : filename_(std::move(filename))
{
}

void id_tracker::init()
{
  VAST_LOG_ACT_VERBOSE("id-tracker", "spawned");
  if (exists(path{string{filename_}}))
  {
    std::ifstream ifs{filename_};
    if (! ifs)
    {
      quit(); // TODO: use actor exit code denoting failure.
      return;
    }
    ifs >> id_;
    VAST_LOG_ACT_INFO("id-tracker", "found existing id " << id_);
  }

  file_.open(filename_);
  file_.seekp(0);

  become(
    on(atom("kill")) >> [=]
    {
      VAST_LOG_ACT_DEBUG("id-tracker", "saves last event id " << id_);
      file_ << id_ << std::endl;
      if (! file_)
        VAST_LOG_ACT_ERROR("id-tracker", "could not save current event id");
      quit();
    },
    on(atom("request"), arg_match) >> [=](uint64_t n)
    {
      assert(file_);
      if (std::numeric_limits<uint64_t>::max() - id_ < n)
      {
        VAST_LOG_ACT_ERROR("id-tracker",
                           "has not enough ids available to hand out " <<
                           n << " ids");
        reply(atom("id"), atom("failure"));
        return;
      }

      VAST_LOG_ACT_DEBUG("id-tracker",
                         "hands out [" << id_ << ',' << id_ + n << ')' <<
                         " to @" << last_sender()->id());

      file_ << id_ + n << std::endl;
      file_.seekp(0);
      if (file_)
        reply(atom("id"), id_, id_ + n);
      else
        reply(atom("id"), atom("failure"));
      id_ += n;
    });
}

void id_tracker::on_exit()
{
  VAST_LOG_ACT_VERBOSE("id-tracker", "terminated");
}

} // namespace vast
