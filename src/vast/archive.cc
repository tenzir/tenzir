#include "vast/archive.h"

#include <cppa/cppa.hpp>
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/serialization.h"
#include "vast/io/serialization.h"

namespace vast {

using namespace cppa;

archive::archive(path directory, size_t capacity)
  : dir_{std::move(directory)},
    cache_{capacity, [&](uuid const& id) { return on_miss(id); }}
{
}

path const& archive::dir() const
{
  return dir_;
}

void archive::initialize()
{
  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        segment_files_.emplace(to_string(p.basename()), p);

        segment::header header;
        io::unarchive(p, header);
        VAST_LOG_DEBUG("found segment " << p.basename() <<
                       " for ID range [" << header.base << ", " <<
                       header.base + header.n << ")");

        if (! ranges_.insert(header.base, header.base + header.n, header.id))
        {
          VAST_LOG_ERROR("inconsistency in ID space for [" <<
                         header.base << ", " << header.base + header.n << ")");
          return false;
        }

        return true;
      });
}

bool archive::store(any_tuple msg)
{
  if (! exists(dir_) && ! mkdir(dir_))
  {
    VAST_LOG_ERROR("failed to create directory " << dir_);
    return false;
  }

  match(msg)(
      on_arg_match >> [&](segment const& s)
      {
        assert(segment_files_.find(s.id()) == segment_files_.end());

        auto const filename = dir_ / path{to_string(s.id())};

        auto t = io::archive(filename, s);
        if (t)
          VAST_LOG_VERBOSE("wrote segment to " << filename);
        else
          VAST_LOG_ERROR(t.error());

        segment_files_.emplace(s.id(), filename);
        ranges_.insert(s.base(), s.base() + s.events(), s.id());
        cache_.insert(s.id(), msg);
      });

  return true;
}

trial<any_tuple> archive::load(event_id eid)
{
  if (auto id = ranges_.lookup(eid))
    return cache_.retrieve(*id);
  else
    return error{"no segment for id", id};
}

any_tuple archive::on_miss(uuid const& id)
{
  VAST_LOG_DEBUG("experienced cache miss for " << id);
  assert(segment_files_.find(id) != segment_files_.end());

  segment s;
  io::unarchive(dir_ / path{to_string(id)}, s);

  return make_any_tuple(std::move(s));
}


archive_actor::archive_actor(path const& directory, size_t max_segments)
  : archive_{directory / "archive", max_segments}
{
}

cppa::behavior archive_actor::act()
{
  archive_.initialize();

  return
  {
    [=](segment const& s)
    {
      if (! archive_.store(last_dequeued()))
      {
        VAST_LOG_ACTOR_ERROR("failed to register segment " << s.id());
        quit(exit::error);
        return make_any_tuple(atom("nack"), s.id());
      }

      return make_any_tuple(atom("ack"), s.id());
    },
    [=](event_id eid, actor sink)
    {
      auto t = archive_.load(eid);
      if (t)
      {
        VAST_LOG_ACTOR_DEBUG("delivering segment for event " << eid);
        send_tuple(sink, *t);
      }
      else
      {
        VAST_LOG_ACTOR_WARN(t.error());
        send(sink, atom("no segment"), eid);
      }
    }
  };
}

char const* archive_actor::describe() const
{
  return "archive";
}

} // namespace vast
