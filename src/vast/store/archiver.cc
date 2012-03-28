#include "vast/store/archiver.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
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
    flush();
}

void archiver::init(fs::path const& directory,
                    size_t max_chunk_events,
                    size_t max_segment_size)
{
    receive([&](ze::event_ptr&& event) { archive(std::move(event)); });

    archive_directory_ = directory;
    LOG(verbose, store) << "initializing archiver in " << archive_directory_;
    if (! fs::exists(archive_directory_))
        fs::mkdir(archive_directory_);

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

    flush();
}

void archiver::flush()
{
    if (! segment_)
        return;

    std::string filename(
        boost::uuids::to_string(boost::uuids::random_generator()()));

    LOG(debug, store)
        << "flushing segment of size " << segment_->size()
        << " to " << filename;

    fs::ofstream file(archive_directory_ / filename,
                      std::ios::binary | std::ios::out);

    segment_->flush(file);
}

} // namespace store
} // namespace vast
