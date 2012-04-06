#include "vast/store/archiver.h"

#include <ze/event.h>
#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"
#include "vast/util/make_unique.h"

namespace vast {
namespace store {

archiver::archiver(ze::component<ze::event>& c)
  : ze::core_sink<ze::event>(c)
  , max_segment_size_(0u)
{
}

archiver::~archiver()
{
    if (segment_)
        flush();
}

void archiver::init(fs::path const& directory,
                    size_t max_events_per_chunk,
                    size_t max_segment_size)
{
    receive([&](ze::event_ptr&& event) { archive(std::move(event)); });

    archive_directory_ = directory;
    assert(fs::exists(archive_directory_));

    LOG(verbose, store)
        << "setting maximum segment size to " << max_segment_size << " bytes";

    max_segment_size_ = max_segment_size;
    max_events_per_chunk_ = max_segment_size;
    segment_ = std::make_unique<osegment>(max_events_per_chunk_);
}

void archiver::archive(ze::event_ptr&& event)
{
    std::lock_guard<std::mutex> lock(segment_mutex_);
    assert(segment_);

    segment_->put(*event);
    if (segment_->size() < max_segment_size_)
        return;

    flush();
}

std::unique_ptr<osegment> archiver::flush()
{
    assert(segment_);
    segment_->flush();

    auto path =  archive_directory_ / ze::uuid::random().to_string();
    LOG(debug, store) << "flushing segment to " << path;
    fs::ofstream file(path, std::ios::binary | std::ios::out);
    ze::serialization::oarchive oa(file);
    oa << *segment_;

    auto flushed = std::move(segment_);
    segment_ = std::make_unique<osegment>(max_events_per_chunk_);

    return flushed;
}

} // namespace store
} // namespace vast
