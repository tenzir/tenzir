#include "vast/id_tracker.h"

#include <cassert>
#include <cppa/cppa.hpp>
#include "vast/file_system.h"

using namespace cppa;

namespace vast {

id_tracker::id_tracker(std::string filename)
  : filename_(std::move(filename))
{
}

void id_tracker::act()
{
  if (exists(path{string{filename_}}))
  {
    std::ifstream ifs{filename_};
    if (! ifs)
    {
      quit(); // TODO: use actor exit code denoting failure.
      return;
    }
    ifs >> id_;
    VAST_LOG_ACTOR_INFO("found existing id " << id_);
  }

  file_.open(filename_);
  file_.seekp(0);

  become(
    on(atom("kill")) >> [=]
    {
      VAST_LOG_ACTOR_DEBUG("saves last event id " << id_);
      file_ << id_ << std::endl;
      if (! file_)
        VAST_LOG_ACTOR_ERROR("could not save current event id");
      quit();
    },
    on(atom("request"), arg_match) >> [=](uint64_t n)
    {
      assert(file_);
      if (std::numeric_limits<uint64_t>::max() - id_ < n)
      {
        VAST_LOG_ACTOR_ERROR("cannot hand out " << n << " ids (too many)");
        reply(atom("id"), atom("failure"));
        return;
      }

      VAST_LOG_ACTOR_DEBUG("hands out [" << id_ << ',' << id_ + n << ')' <<
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

char const* id_tracker::description() const
{
  return "id-tracker";
}

} // namespace vast
