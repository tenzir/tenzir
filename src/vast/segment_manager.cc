#include "vast/segment_manager.h"

#include "vast/file_system.h"
#include "vast/segment.h"
#include "vast/logger.h"
#include "vast/io/file_stream.h"
#include "vast/io/serialization.h"

namespace vast {

segment_manager::segment_manager(size_t capacity, std::string const& dir)
  : cache_(capacity, [&](uuid const& id) { return on_miss(id); })
  , dir_(dir)
{
  VAST_LOG_VERBOSE("spawning segment manager @" << id() <<
                   " with capacity " << capacity);

  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("load")) >> [=]
      {
        traverse(
            dir_,
            [&](path const& p) -> bool
            {
              VAST_LOG_VERBOSE("segment manager @" << id() << " found segment " << p);
              segment_files_.emplace(to_string(p.basename()), p);
              return true;
            });

        if (segment_files_.empty())
          VAST_LOG_VERBOSE("segment manager @" << id() <<
                           " did not find any segments");
      },
      on_arg_match >> [=](segment const& s)
      {
        auto opt = tuple_cast<segment>(last_dequeued());
        assert(opt.valid());
        store_segment(*opt);
        reply(atom("segment"), atom("ack"), s.id());
      },
      on(atom("get"), atom("ids")) >> [=]
      {
        VAST_LOG_DEBUG("segment manager @" << id() << " retrieves all ids");
        std::vector<uuid> ids;
        std::transform(segment_files_.begin(),
                       segment_files_.end(),
                       std::back_inserter(ids),
                       [](std::pair<uuid, path> const& p)
                       {
                         return p.first;
                       });

        reply(ids);
      },
      on(atom("get"), arg_match) >> [=](uuid const& uid)
      {
        VAST_LOG_DEBUG("segment manager @" << id() <<
                       " retrieves segment " << uid);

        reply_tuple(cache_.retrieve(uid));
      },
      on(atom("kill")) >> [=]
      {
        segment_files_.clear();
        cache_.clear();
        quit();
        VAST_LOG_VERBOSE("segment manager @" << id() << " terminated");
      });
}

void segment_manager::store_segment(cppa::cow_tuple<segment> t)
{
  auto& s = cppa::get<0>(t);
  assert(segment_files_.find(s.id()) == segment_files_.end());
  auto filename = dir_ / path(to_string(s.id()));
  segment_files_.emplace(s.id(), filename);
  {
    file f(filename);
    f.open(file::write_only);
    io::file_output_stream out(f);
    io::binary_serializer sink(out);
    sink << s;
  }

  cache_.insert(s.id(), t);
  VAST_LOG_VERBOSE("segment manager @" << id() <<
                   " wrote segment to " << filename);
}

cppa::cow_tuple<segment> segment_manager::on_miss(uuid const& uid)
{
  assert(segment_files_.find(uid) != segment_files_.end());
  VAST_LOG_DEBUG("segment manager @" << id() <<
                 " cache miss, going to disk for segment " << uid);

  file f(dir_ / path(to_string(uid)));
  f.open(file::read_only);
  io::file_input_stream in(f);
  io::binary_deserializer source(in);
  segment s;
  source >> s;
  return {std::move(s)};
}

} // namespace vast
