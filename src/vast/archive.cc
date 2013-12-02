#include "vast/archive.h"

#include <cppa/cppa.hpp>
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/exception.h"
#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/segment_manager.h"
#include "vast/serialization.h"
#include "vast/io/file_stream.h"

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
  assert(exists(directory_));
  traverse(
      directory_,
      [&](path const& p) -> bool
      {
        file f{p};
        // FIXME: factor the segment header and load it as a single unit.
        uint32_t m;
        uint8_t v;
        uuid id;
        io::compression c;
        event_id base;
        uint32_t n;
        f.open(file::read_only);
        io::file_input_stream source(f);
        binary_deserializer d{source};
        d >> m >> v >> id >> c >> base >> n;
        VAST_LOG_DEBUG("found segment " << p.basename() <<
                       " for ID range [" << base << ", " << base + n << ")");
        if (m != segment::magic)
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

void archive::store(segment const& s)
{
  if (! ranges_.insert(s.base(), s.base() + s.events(), s.id()))
    VAST_LOG_ERROR("failed to register segment " << s.id());
}

uuid const* archive::lookup(event_id eid) const
{
  return ranges_.lookup(eid);
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
  if (! exists(archive_.dir()) && ! mkdir(archive_.dir()))
  {
    VAST_LOG_ACTOR_ERROR("failed to create directory " << archive_.dir());
    quit(exit::error);
    return;
  }
  archive_.load();
  become(
      on_arg_match >> [=](uuid const& id)
      {
        send(segment_manager_, id, last_sender());
      },
      on(atom("uuid"), arg_match) >> [=](event_id eid)
      {
        if (auto id = archive_.lookup(eid))
         reply(*id);
        else
          reply(eid);
      },
      on(atom("segment"), arg_match) >> [=](event_id eid)
      {
        if (auto id = archive_.lookup(eid))
        {
          VAST_LOG_ACTOR_VERBOSE("got segment " << *id << " for event " << eid);
          send(segment_manager_, *id, last_sender());
        }
        else
        {
          VAST_LOG_ACTOR_WARN("no segment found for event " << eid);
          reply(eid);
        }
      },
      on(arg_match) >> [=](segment const& s)
      {
        archive_.store(s);
        forward_to(segment_manager_);
        reply(atom("segment"), atom("ack"), s.id());
      });
}

char const* archive_actor::description() const
{
  return "archive";
}

} // namespace vast
