#include "vast/store/archiver.h"

#include <ze/event.h>
#include "vast/util/logger.h"
#include "vast/util/make_unique.h"

namespace vast {
namespace store {

archiver::archiver(ze::component<ze::event>& c)
  : ze::core_sink<ze::event>(c)
{
}

void archiver::init(fs::path const& directory)
{
    LOG(debug, store) << "initializing archiver in directory " << directory;

    receive([&](ze::event_ptr&& event) { archive(std::move(event)); });
    segment_ = std::make_unique<osegment>(1000);
}

void archiver::archive(ze::event_ptr&& event)
{
    // TODO: 
}

} // namespace store
} // namespace vast
