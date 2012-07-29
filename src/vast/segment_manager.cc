#include "vast/segment_manager.h"

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

  LOG(verbose, archive)
    << "segment manager @" << id() << " scans directory " << dir_;

  assert(fs::exists(dir_));
  scan(dir_);
  if (segment_files_.empty())
    LOG(verbose, archive)
      << "segment manager @" << id() << " did not find any segments";

  using namespace cppa;
  init_state = (
      on_arg_match >> [=](segment const& s)
      {
        auto t = tuple_cast<segment>(last_dequeued());
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
      on(atom("retrieve"), arg_match) >> [=](ze::uuid const& uuid)
      {
        LOG(debug, archive)
          << "segment manager @" << id() << " retrieves segment " << uuid;

        last_sender() << cache_.retrieve(uuid);
      },
      on(atom("shutdown")) >> [=]
      {
        segment_files_.clear();
        cache_.clear();
        quit();
        LOG(verbose, archive) << "segment manager @" << id() << " terminated";
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
          LOG(verbose, archive)
            << "segment manager @" << id() << " found segment " << p;
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

  LOG(verbose, archive)
    << "segment manager @" << id() << " wrote segment to " << path;
  cache_.insert(s.id(), t);
}

cppa::cow_tuple<segment> segment_manager::on_miss(ze::uuid const& uuid)
{
  DBG(archive)
    << "segment manager @" << id()
    << " experienced cache miss for segment " << uuid;
  assert(segment_files_.find(uuid) != segment_files_.end());

  fs::ifstream file(dir_ / uuid.to_string(), std::ios::binary | std::ios::in);
  ze::serialization::stream_iarchive ia(file);
  segment s;
  ia >> s;

  return std::move(s);
}

} // namespace vast
