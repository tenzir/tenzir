#include "vast/id_tracker.h"

#include <cassert>
#include <fstream>
#include <cppa/cppa.hpp>
#include "vast/convert.h"

using namespace cppa;

namespace vast {

id_tracker::id_tracker(path dir)
  : dir_{std::move(dir)}
{
}

bool id_tracker::load()
{
  if (! exists(dir_ / "id"))
    return true;
  std::ifstream file{to<std::string>(dir_ / "id")};
  if (! file)
    return false;
  file >> id_;
  VAST_LOG_INFO("tracker found existing next event ID " << id_);
  return true;
}

bool id_tracker::save()
{
  if (! exists(dir_))
      return false;
  std::ofstream file{to<std::string>(dir_ / "id")};
  if (! file)
    return false;
  file << id_ << std::endl;
  return true;
}

event_id id_tracker::next_id() const
{
  return id_;
}

bool id_tracker::hand_out(uint64_t n)
{
  if (std::numeric_limits<event_id>::max() - id_ < n)
    return false;
  id_ += n;
  if (! save())
  {
    id_ -= n;
    return false;
  }
  return true;
}


id_tracker_actor::id_tracker_actor(path dir)
  : id_tracker_{std::move(dir)}
{
}

void id_tracker_actor::act()
{
  trap_exit(true);
  become(
    on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
    {
      if (! id_tracker_.save())
        VAST_LOG_ACTOR_ERROR("could not save current event ID");
      quit(reason);
    },
    on(atom("request"), arg_match) >> [=](uint64_t n)
    {
      auto next = id_tracker_.next_id();
      if (! id_tracker_.hand_out(n))
      {
        VAST_LOG_ACTOR_ERROR(
            "failed to hand out " << n << " ids (current ID: " << next << ")");
        reply(atom("id"), atom("failure"));
      }
      else
      {
        VAST_LOG_ACTOR_DEBUG("hands out [" << next << ',' << next + n << ')');
        reply(atom("id"), next, next + n);
      }
    });
  
  if (! id_tracker_.load())
    VAST_LOG_ACTOR_ERROR("failed to load existing tracker ID from filesystem");
}

char const* id_tracker_actor::description() const
{
  return "id-tracker";
}

} // namespace vast
