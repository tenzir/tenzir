#include "vast/store/archive.h"

#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/store/emitter.h"
#include "vast/store/exception.h"
#include "vast/store/ingestor.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

archive::archive(ze::io& io, ingestor& ingest)
  : ze::component(io)
  , segmentizer_(*this)
  , writer_(*this)
{
    ingest.source.to(segmentizer_.frontend());
    segmentizer_.backend().to(writer_);
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
            [&](ze::uuid const& id) { return load(id); });

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

    for (auto& pair : emitters_)
        pair.second->pause();

    emitters_.clear();
}

emitter& archive::create_emitter()
{
    std::vector<ze::uuid> ids;
    // TODO: only select those segments IDs which contain relevant events.
    {
        std::lock_guard<std::mutex> lock(segment_mutex_);
        std::transform(
            segments_.begin(),
            segments_.end(),
            std::back_inserter(ids),
            [&](decltype(segments_)::value_type const& pair)
            {
                return pair.first;
            });
    }

    auto e = std::make_shared<emitter>(*this, cache_, std::move(ids));
    std::lock_guard<std::mutex> lock(emitter_mutex_);
    emitters_.emplace(e->id(), e);

    return *e;
}

emitter& archive::lookup_emitter(ze::uuid const& id)
{
    std::lock_guard<std::mutex> lock(emitter_mutex_);
    auto i = emitters_.find(id);
    if (i == emitters_.end())
        throw archive_exception("invalid emitter ID");

    return *i->second;
}

void archive::remove_emitter(ze::uuid const& id)
{
    std::lock_guard<std::mutex> lock(emitter_mutex_);
    auto i = emitters_.find(id);
    if (i == emitters_.end())
        throw archive_exception("invalid emitter ID");

    i->second->pause();
    emitters_.erase(i);
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
                std::lock_guard<std::mutex> lock(segment_mutex_);
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
    {
        std::lock_guard<std::mutex> lock(segment_mutex_);
        assert(segments_.find(is->id()) == segments_.end());
        segments_.emplace(is->id(), path);
    }

    cache_->insert(is->id(), is);
}

std::shared_ptr<isegment> archive::load(ze::uuid const& id)
{
    LOG(debug, store) << "cache miss, loading segment " << id;

    {
        std::lock_guard<std::mutex> lock(segment_mutex_);
        // The inquired segment must have been recorded at startup or added
        // upon segment rotation by the writer.
        assert(segments_.find(id) != segments_.end());
    }

    auto path = archive_root_ / id.to_string();
    fs::ifstream file(path, std::ios::binary | std::ios::in);
    ze::serialization::iarchive ia(file);
    std::shared_ptr<isegment> is;
    ia >> is;

    return is;
}

} // namespace store
} // namespace vast
