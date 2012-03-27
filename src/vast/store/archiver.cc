#include "vast/store/archiver.h"

#include <ze/event.h>
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
    if (segment_ && file_.good())
        segment_->flush(file_);
}

void archiver::init(fs::path const& directory,
                    size_t max_chunk_events,
                    size_t max_segment_size)
{
    receive([&](ze::event_ptr&& event) { archive(std::move(event)); });

    LOG(verbose, store) << "initializing archiver in " << directory;
    if (! fs::exists(directory))
        fs::mkdir(directory);

    file_.open(directory / "foo", std::ios::binary | std::ios::out);

    LOG(verbose, store)
        << "setting maximum segment size to " << max_segment_size << " bytes";

    max_segment_size_ = max_segment_size;
    segment_ = std::make_unique<osegment>(max_chunk_events);
}

void archiver::archive(ze::event_ptr&& event)
{
    segment_->put(*event);
    if (segment_->size() < max_segment_size_)
        return;

    LOG(debug, store) << "flushing segment of size " << segment_->size();
    segment_->flush(file_);
    segment_->clear();
}

} // namespace store
} // namespace vast
