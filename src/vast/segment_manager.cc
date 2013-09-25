#include "vast/segment_manager.h"

#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/io/file_stream.h"
#include "vast/serialization.h"

using namespace cppa;

namespace vast {

segment_manager::segment_manager(size_t capacity, std::string const& dir)
  : dir_(dir),
    cache_(capacity, [&](uuid const& id) { return on_miss(id); })
{
  chaining(false);
}

void segment_manager::act()
{
  become(
      on(atom("init")) >> [=]
      {
        traverse(
            dir_,
            [&](path const& p) -> bool
            {
              VAST_LOG_ACTOR_VERBOSE("found segment " << p);
              segment_files_.emplace(to_string(p.basename()), p);
              return true;
            });

        if (segment_files_.empty())
          VAST_LOG_ACTOR_VERBOSE("did not find any segments in " << dir_);
      },
      on(atom("kill")) >> [=]
      {
        segment_files_.clear();
        cache_.clear();
        quit();
      },
      on_arg_match >> [=](segment const& s)
      {
        store(*tuple_cast<segment>(last_dequeued()));
        reply(atom("segment"), atom("ack"), s.id());
      },
      on(atom("get"), atom("ids")) >> [=]
      {
        VAST_LOG_ACTOR_DEBUG("retrieves all ids");
        std::vector<uuid> ids;
        std::transform(segment_files_.begin(),
                       segment_files_.end(),
                       std::back_inserter(ids),
                       [](std::pair<uuid, path> const& p) { return p.first; });
        reply(ids);
      },
      on(atom("get"), arg_match) >> [=](uuid const& uid)
      {
        VAST_LOG_ACTOR_DEBUG("retrieves segment " << uid);
        reply_tuple(cache_.retrieve(uid));
      });
}

char const* segment_manager::description() const
{
  return "segment-manager";
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
  VAST_LOG_ACTOR_VERBOSE("wrote segment to " << filename);
}

cow<segment> segment_manager::on_miss(uuid const& uid)
{
  assert(segment_files_.find(uid) != segment_files_.end());
  VAST_LOG_ACTOR_DEBUG("experienced cache miss for " << uid <<
                       ", going to stable storage");

  file f(dir_ / path(to_string(uid)));
  f.open(file::read_only);
  io::file_input_stream in(f);
  binary_deserializer source(in);
  segment s;
  source >> s;
  return {std::move(s)};
}

} // namespace vast
