#include "vast/store/archive.h"

#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/store/emitter.h"
#include "vast/store/exception.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

archive::archive(std::string const& directory,
                 size_t max_events_per_chunk,
                 size_t max_segment_size,
                 size_t max_segments)
  : archive_root_(directory)
  , segment_manager_(max_segments, [&](ze::uuid const& id) { return load(id); })
{
  LOG(info, store) << "creating segment cache with capacity " << max_segments;

  segmentizer_ = cppa::spawn<segmentizer>(max_events_per_chunk,
                                          max_segment_size);

  if (! fs::exists(archive_root_))
  {
    LOG(info, store) << "creating new directory " << archive_root_;
    fs::mkdir(archive_root_);
  }
  else
  {
    LOG(info, store) << "scanning " << archive_root_;
    scan(archive_root_);
    if (segment_files_.empty())
      LOG(info, store) << "no segments found in " << archive_root_;
  }
  using namespace cppa;

  init_state = (
      on(atom("emitter"), atom("create"), arg_match) >> [=](actor_ptr sink)
      {
        create_emitter(sink);
      },
      on_arg_match >> [=](segment const& s)
      {
        auto path = archive_root_ / s.id().to_string();
        {
            fs::ofstream file(path, std::ios::binary | std::ios::out);
            ze::serialization::stream_oarchive oa(file);
            oa << s;
        }

        LOG(verbose, store) << "wrote segment to " << path;

        // FIXME: os cannot be moved.
        auto is = std::make_shared<isegment>(std::move(*os));
        {
            assert(segment_files_.find(is->id()) == segment_files_.end());
            segment_files_.emplace(is->id(), path);
        }

        segment_manager_->insert(is->id(), is);
      },
      on(atom("shutdown")) >> [=]()
      {
        segmentizer_ << self->last_dequeued();
        for (auto em : emitters)
          em << self->last_dequeued();
        self->quit();
      });
}

void archive::create_emitter(cppa::actor_ptr sink)
{
  // TODO: only select those segments IDs which contain relevant events.
  std::vector<ze::uuid> ids;
  {
    std::transform(
        segment_files_.begin(),
        segment_files_.end(),
        std::back_inserter(ids),
        [&](decltype(segment_files_)::value_type const& pair)
        {
          return pair.first;
        });
  }

  auto em = cppa::spawn<emitter>(segment_manager_, std::move(ids));
  cppa::send(em, atom("sink"), sink);
  emitters_.push_back(em);
}

void archive::scan(fs::path const& directory)
{
    fs::each_dir_entry(
        archive_root_,
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

std::shared_ptr<isegment> archive::load(ze::uuid const& id)
{
    LOG(debug, store) << "cache miss, loading segment " << id;

    // The inquired segment must have been recorded at startup or added
    // upon segment rotation by the writer.
    assert(segment_files_.find(id) != segment_files_.end());

    auto path = archive_root_ / id.to_string();
    fs::ifstream file(path, std::ios::binary | std::ios::in);
    ze::serialization::iarchive ia(file);
    std::shared_ptr<isegment> is;
    ia >> is;

    return is;
}

} // namespace store
} // namespace vast
