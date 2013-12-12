#include "vast/segment_manager.h"

#include <cppa/cppa.hpp>
#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/io/file_stream.h"
#include "vast/io/serialization.h"
#include "vast/serialization.h"

namespace vast {

segment_manager::segment_manager(size_t capacity, path dir)
  : dir_{std::move(dir)},
    cache_{capacity, [&](uuid const& id) { return on_miss(id); }}
{
  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        VAST_LOG_VERBOSE("found segment " << p.trim(-2));
        segment_files_.emplace(to_string(p.basename()), p);
        return true;
      });
}

bool segment_manager::store(cow<segment> const& s)
{
  if (segment_files_.empty())
  {
    assert(! exists(dir_));
    if (! mkdir(dir_))
    {
      VAST_LOG_ERROR("failed to create directory " << dir_);
      return false;
    }
  }

  assert(segment_files_.find(s->id()) == segment_files_.end());
  auto const filename = dir_ / path{to_string(s->id())};
  io::archive(filename, *s);
  segment_files_.emplace(s->id(), filename);
  cache_.insert(s->id(), s);

  VAST_LOG_VERBOSE("wrote segment to " << filename);
  return true;
}

cow<segment> segment_manager::lookup(uuid const& id)
{
  return cache_.retrieve(id);
}

cow<segment> segment_manager::on_miss(uuid const& uid)
{
  assert(segment_files_.find(uid) != segment_files_.end());
  VAST_LOG_DEBUG("experienced cache miss for " << uid <<
                       ", going to file system");

  segment s;
  io::unarchive(dir_ / path{to_string(uid)}, s);
  return {std::move(s)};
}

using namespace cppa;

segment_manager_actor::segment_manager_actor(size_t capacity, path dir)
  : segment_manager_{capacity, std::move(dir)}
{
}

void segment_manager_actor::act()
{
  become(
      on_arg_match >> [=](segment const& s)
      {
        if (! segment_manager_.store(*tuple_cast<segment>(last_dequeued())))
        {
          send_exit(self, exit::error);
          return make_any_tuple(atom("segment"), atom("nack"), s.id());
        }

        return make_any_tuple(atom("segment"), atom("ack"), s.id());
      },
      on_arg_match >> [=](uuid const& id, actor_ptr const& sink)
      {
        VAST_LOG_ACTOR_DEBUG("retrieves segment " << id);
        sink << segment_manager_.lookup(id);
      });
}

char const* segment_manager_actor::description() const
{
  return "segment-manager";
}

} // namespace vast
