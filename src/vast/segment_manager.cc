#include "vast/segment_manager.h"

#include <ze.h>
#include "vast/segment.h"
#include "vast/logger.h"
#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"

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
        fs::each_file_entry(
            dir_,
            [&](fs::path const& p)
            {
              LOG(verbose, archive)
                << "segment manager @" << id() << " found segment " << p;
              segment_files_.emplace(p.filename().string(), p);
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
                       [](std::pair<ze::uuid, fs::path> const& p)
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
    fs::ofstream file(path, std::ios::binary | std::ios::out);
    ze::serialization::stream_oarchive oa(file);
    oa << s;
  }

  cache_.insert(s.id(), t);
  LOG(verbose, archive)
    << "segment manager @" << id() << " wrote segment to " << path;
}

cppa::cow_tuple<segment> segment_manager::on_miss(ze::uuid const& uuid)
{
  DBG(archive)
    << "segment manager @" << id()
    << " cache miss, going to disk for segment " << uuid;
  assert(segment_files_.find(uuid) != segment_files_.end());

  fs::ifstream file(dir_ / ze::to_string(uuid), std::ios::binary | std::ios::in);
  ze::serialization::stream_iarchive ia(file);
  segment s;
  ia >> s;

  return std::move(s);
}

} // namespace vast
