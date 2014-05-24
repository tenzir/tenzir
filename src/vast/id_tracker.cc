#include "vast/id_tracker.h"

#include <cassert>
#include <fstream>
#include <cppa/cppa.hpp>
#include "vast/print.h"

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

  std::ifstream file{to_string(dir_ / "id")};
  if (! file)
    return false;

  file >> id_;

  VAST_LOG_INFO("tracker found existing next event ID " << id_);
  return true;
}

bool id_tracker::save()
{
  assert(exists(dir_));
  if (id_ == 1)
    return true;

  std::ofstream file{to_string(dir_ / "id")};
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

behavior id_tracker_actor::act()
{
  trap_exit(true);

  if (! id_tracker_.load())
  {
    VAST_LOG_ACTOR_ERROR("failed to load existing tracker ID from filesystem");
    quit(exit::error);
  }

  return
  {
    [=](exit_msg const& e)
    {
      if (! id_tracker_.save())
        VAST_LOG_ACTOR_ERROR(
            "could not save current event ID " << id_tracker_.next_id());

      quit(e.reason);
    },
    on(atom("request"), arg_match) >> [=](uint64_t n)
    {
      auto next = id_tracker_.next_id();
      if (! id_tracker_.hand_out(n))
      {
        VAST_LOG_ACTOR_ERROR(
            "failed to hand out " << n << " ids (current ID: " << next << ")");
        return make_any_tuple(atom("id"), atom("failure"));
      }
      else
      {
        VAST_LOG_ACTOR_DEBUG("hands out [" << next << ',' << next + n << ')');
        return make_any_tuple(atom("id"), next, next + n);
      }
    }
  };
}

std::string id_tracker_actor::describe() const
{
  return "id-tracker";
}

} // namespace vast
