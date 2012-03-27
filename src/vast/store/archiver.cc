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
{
}

archiver::~archiver()
{
    if (segment_ && file_.good())
        segment_->flush(file_);
}

void archiver::init(fs::path const& directory)
{
    LOG(debug, store) << "initializing archiver in directory " << directory;

    receive([&](ze::event_ptr&& event) { archive(std::move(event)); });

    if (! fs::exists(directory))
        fs::mkdir(directory);

    file_.open(directory / "foo", std::ios::binary | std::ios::out);

    segment_ = std::make_unique<osegment>(10000);
}

void archiver::archive(ze::event_ptr&& event)
{
    segment_->put(*event);

    if (segment_->size() > max_segment_size_)
        segment_->flush(file_);
}

} // namespace store
} // namespace vast
