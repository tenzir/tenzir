#include "vast/archive.h"

#include <cppa/cppa.hpp>
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/segment_manager.h"
#include "vast/serialization.h"
#include "vast/io/serialization.h"

namespace vast {

archive::archive(path directory)
  : directory_{std::move(directory)}
{
}

path const& archive::dir() const
{
  return directory_;
}

void archive::load()
{
  traverse(
      directory_,
      [&](path const& p) -> bool
      {
        // FIXME: factor the segment header and load it as a single unit.
        uint32_t magic;
        uint8_t version;
        uuid id;
        io::compression c;
        event_id base;
        uint32_t n;
        io::unarchive(p, magic, version, id, c, base, n);
        VAST_LOG_DEBUG("found segment " << p.basename() <<
                       " for ID range [" << base << ", " << base + n << ")");

        if (magic != segment::magic)
        {
          VAST_LOG_ERROR("got invalid segment magic for " << id);
          return false;
        }
        else if (! ranges_.insert(base, base + n, std::move(id)))
        {
          VAST_LOG_ERROR("inconsistency in ID space for [" << base
                         << ", " << base + n << ")");
          return false;
        }

        return true;
      });
}

bool archive::store(segment const& s)
{
  return ranges_.insert(s.base(), s.base() + s.events(), s.id());
}

std::tuple<uuid const*, event_id, event_id> archive::lookup(event_id eid) const
{
  return ranges_.find(eid);
}

using namespace cppa;

archive_actor::archive_actor(path directory, size_t max_segments)
  : archive_{std::move(directory)}
{
  segment_manager_ =
    spawn<segment_manager_actor, linked>(max_segments, archive_.dir());
}

void archive_actor::act()
{
  archive_.load();
  become(
      on_arg_match >> [=](uuid const& id)
      {
        send(segment_manager_, id, last_sender());
      },
      on(atom("uuid"), arg_match) >> [=](event_id eid)
      {
        auto t = archive_.lookup(eid);
        auto id = get<0>(t);
        if (id)
         return make_any_tuple(*id, get<1>(t), get<2>(t));
        else
          return make_any_tuple(eid);
      },
      on(atom("segment"), arg_match) >> [=](event_id eid)
      {
        auto t = archive_.lookup(eid);
        auto id = get<0>(t);
        if (id)
        {
          VAST_LOG_ACTOR_VERBOSE("got segment " << *id << " for event " << eid);
          send(segment_manager_, *id, last_sender());
        }
        else
        {
          VAST_LOG_ACTOR_WARN("no segment found for event " << eid);
          send(last_sender(), atom("no segment"), eid);
        }
      },
      on(arg_match) >> [=](segment const& s)
      {
        if (! archive_.store(s))
        {
          VAST_LOG_ACTOR_ERROR("failed to register segment " << s.id());
          quit(exit::error);
        }

        forward_to(segment_manager_);
        return make_any_tuple(atom("segment"), atom("ack"), s.id());
      });
}

char const* archive_actor::description() const
{
  return "archive";
}

} // namespace vast
