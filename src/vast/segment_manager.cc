#include "vast/segment_manager.h"

#include <cppa/cppa.hpp>
#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/io/file_stream.h"
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

void segment_manager::store(cow<segment> const& s)
{
  assert(segment_files_.find(s->id()) == segment_files_.end());
  auto filename = dir_ / path(to_string(s->id()));
  segment_files_.emplace(s->id(), filename);
  {
    file f(filename);
    f.open(file::write_only);
    io::file_output_stream out(f);
    binary_serializer sink(out);
    sink << *s;
  }
  cache_.insert(s->id(), s);
  VAST_LOG_VERBOSE("wrote segment to " << filename);
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
  file f(dir_ / path(to_string(uid)));
  f.open(file::read_only);
  io::file_input_stream in(f);
  binary_deserializer source(in);
  segment s;
  source >> s;
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
        segment_manager_.store(*tuple_cast<segment>(last_dequeued()));
        reply(atom("segment"), atom("ack"), s.id());
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
