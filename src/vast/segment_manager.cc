#include "vast/segment_manager.h"

#include <ze.h>
#include "vast/segment.h"
#include "vast/logger.h"

namespace vast {

segment_manager::segment_manager(size_t capacity, std::string const& dir)
  : cache_(capacity, [&](ze::uuid const& id) { return on_miss(id); })
  , dir_(dir)
{
  LOG(verbose, archive)
    << "spawning segment manager @" << id() << " with capacity " << capacity;

  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("load")) >> [=]
      {
        ze::traverse(
            dir_,
            [&](ze::path const& p) -> bool
            {
              LOG(verbose, archive)
                << "segment manager @" << id() << " found segment " << p;
              segment_files_.emplace(p.basename().string(), p);
              return true;
            });

        if (segment_files_.empty())
          LOG(verbose, archive)
            << "segment manager @" << id() << " did not find any segments";
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
        LOG(debug, archive)
          << "segment manager @" << id() << " retrieves all ids";

        std::vector<ze::uuid> ids;
        std::transform(segment_files_.begin(),
                       segment_files_.end(),
                       std::back_inserter(ids),
                       [](std::pair<ze::uuid, ze::path> const& p)
                       {
                         return p.first;
                       });

        reply(ids);
      },
      on(atom("get"), arg_match) >> [=](ze::uuid const& uuid)
      {
        LOG(debug, archive)
          << "segment manager @" << id() << " retrieves segment " << uuid;

        reply_tuple(cache_.retrieve(uuid));
      },
      on(atom("shutdown")) >> [=]
      {
        segment_files_.clear();
        cache_.clear();
        quit();
        LOG(verbose, archive) << "segment manager @" << id() << " terminated";
      });
}

void segment_manager::store_segment(cppa::cow_tuple<segment> t)
{
  auto& s = cppa::get<0>(t);
  assert(segment_files_.find(s.id()) == segment_files_.end());
  auto path = dir_ / ze::to_string(s.id());
  segment_files_.emplace(s.id(), path);
  {
    ze::file file(path);
    file.open(ze::file::write_only);
    ze::io::file_output_stream out(file);
    ze::io::binary_serializer sink(out);
    sink << s;
  }

  cache_.insert(s.id(), t);
  LOG(verbose, archive)
    << "segment manager @" << id() << " wrote segment to " << path;
}

cppa::cow_tuple<segment> segment_manager::on_miss(ze::uuid const& uuid)
{
  assert(segment_files_.find(uuid) != segment_files_.end());
  DBG(archive)
    << "segment manager @" << id()
    << " cache miss, going to disk for segment " << uuid;

  ze::file file(dir_ / ze::to_string(uuid));
  file.open(ze::file::read_only);
  ze::io::file_input_stream in(file);
  ze::io::binary_deserializer source(in);
  segment s;
  source >> s;
  return {std::move(s)};
}

} // namespace vast
