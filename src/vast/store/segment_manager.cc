#include <vast/store/segment_manager.h>

#include <vast/fs/fstream.h>
#include <vast/fs/operations.h>
#include <vast/store/segment.h>
#include <vast/util/logger.h>

namespace vast {
namespace store {

segment_manager::segment_manager(size_t capacity, std::string const& dir)
  : cache_(capacity, [&](ze::uuid const& id) { return on_miss(id); })
  , dir_(dir)
{
  LOG(debug, store) 
    << "spawning segment manager @" << id() << " with capacity " << capacity;

  if (! fs::exists(dir_))
  {
    LOG(info, store) << "creating new directory " << dir_;
    fs::mkdir(dir_);
  }
  else
  {
    LOG(info, store) << "scanning " << dir_;
    scan(dir_);
    if (segment_files_.empty())
      LOG(info, store) << "no segments found in " << dir_;
  }

  using namespace cppa;
  init_state = (
      on_arg_match >> [=](segment const& s)
      {
        auto t = tuple_cast<segment>(self->last_dequeued());
        assert(t.valid());
        store_segment(*t);
        reply(atom("segment"), atom("ack"), s.id());
      },
      on(atom("all ids")) >> [=]
      {
        std::vector<ze::uuid> ids;
        for (auto& f : segment_files_)
          ids.push_back(f.first);
        reply(atom("ids"), std::move(ids));
      },
      on(atom("retrieve"), arg_match) >> [=](ze::uuid const& id)
      {
        LOG(debug, store) 
          << "segment manager @" << self->id() << " retrieves segment " << id;
        self->last_sender() << cache_.retrieve(id);
      },
      on(atom("shutdown")) >> [=]
      {
        segment_files_.clear();
        cache_.clear();
        self->quit();
        LOG(verbose, store) << "segment manager @" << id() << " terminated";
      });
}

void segment_manager::scan(fs::path const& directory)
{
  fs::each_dir_entry(
      dir_,
      [&](fs::path const& p)
      {
        if (fs::is_directory(p))
          scan(p);
        else
        {
          LOG(verbose, store) << "found segment " << p;
          segment_files_.emplace(p.filename().string(), p);
        }
      });
}

void segment_manager::store_segment(cppa::cow_tuple<segment> t)
{
  auto& s = cppa::get<0>(t);

  // A segment should not have been recorded twice.
  assert(segment_files_.find(s.id()) == segment_files_.end());

  auto path = dir_ / s.id().to_string();
  segment_files_.emplace(s.id(), path);
  {
    fs::ofstream file(path, std::ios::binary | std::ios::out);
    ze::serialization::stream_oarchive oa(file);
    oa << s;
  }

  LOG(verbose, store) << "wrote segment to " << path;
  cache_.insert(s.id(), t);
}

cppa::cow_tuple<segment> segment_manager::on_miss(ze::uuid const& id)
{
  LOG(debug, store) << "cache miss, loading segment " << id;
  assert(segment_files_.find(id) != segment_files_.end());

  fs::ifstream file(dir_ / id.to_string(), std::ios::binary | std::ios::in);
  ze::serialization::stream_iarchive ia(file);
  segment s;
  ia >> s;

  return std::move(s);
}

} // namespace store
} // namespace vast
