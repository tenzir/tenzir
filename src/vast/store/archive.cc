#include "vast/store/archive.h"

#include <ze/util/make_unique.h>
#include <ze/link.h>
#include "vast/comm/event_source.h"
#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

archive::archive(ze::io& io, comm::event_source& ingestor_source)
  : ze::component(io)
  , segmentizer_(*this)
  , writer_(*this)
{
    ze::link(ingestor_source, segmentizer_);
    ze::link(segmentizer_, writer_);
    writer_.receive([&](ze::intrusive_ptr<osegment>&& os)
                    {
                        on_rotate(os);
                    });
}

void archive::init(fs::path const& directory,
                   size_t max_events_per_chunk,
                   size_t max_segment_size,
                   size_t max_segments)
{
    archive_root_ = directory;

    LOG(info, store) << "creating segment cache with capacity " << max_segments;
    cache_ = std::make_shared<segment_cache>(
            max_segments,
            [&](ze::uuid id) { return load(id); });

    segmentizer_.init(max_events_per_chunk, max_segment_size);

    if (! fs::exists(archive_root_))
    {
        LOG(info, store) << "creating new directory " << archive_root_;
        fs::mkdir(archive_root_);
    }
    else
    {
        LOG(info, store) << "scanning " << archive_root_;
        scan(archive_root_);
        if (segments_.empty())
            LOG(info, store) << "no segments found in " << archive_root_;
    }
}

void archive::stop()
{
    segmentizer_.stop();
}

std::unique_ptr<emitter> archive::get()
{
    std::vector<ze::uuid> ids;
    // TODO: only select those segments IDs which contain relevant events.
    std::transform(
        segments_.begin(),
        segments_.end(),
        std::back_inserter(ids),
        [&](decltype(segments_)::value_type const& pair)
        {
            return pair.first;
        });

    return std::make_unique<emitter>(*this, cache_, std::move(ids));
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
                segments_.emplace(p.filename().string(), p);
            }
        });
}

void archive::on_rotate(ze::intrusive_ptr<osegment> os)
{
    auto path = archive_root_ / os->id().to_string();
    {
        fs::ofstream file(path, std::ios::binary | std::ios::out);
        ze::serialization::oarchive oa(file);
        oa << os;
    }
    LOG(debug, store) << "wrote segment to " << path;

    auto is = std::make_shared<isegment>(std::move(*os));
    assert(segments_.find(is->id()) == segments_.end());
    segments_.emplace(is->id(), path);

    cache_->insert(is->id(), is);
}

std::shared_ptr<isegment> archive::load(ze::uuid id)
{
    LOG(debug, store) << "cache miss, loading segment " << id;

    // The inquired segment must have been recorded at startup or added upon
    // segment rotation by the writer.
    assert(segments_.find(id) != segments_.end());

    auto path = archive_root_ / id.to_string();
    fs::ifstream file(path, std::ios::binary | std::ios::in);
    ze::serialization::iarchive ia(file);
    std::shared_ptr<isegment> is;
    ia >> is;

    return is;
}

} // namespace store
} // namespace vast
