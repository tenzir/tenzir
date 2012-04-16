#include "vast/store/emitter.h"

#include <ze/event.h>
#include "vast/store/exception.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

emitter::emitter(ze::component& c,
                 std::shared_ptr<segment_cache> cache,
                 std::vector<ze::uuid> ids)
  : ze::publisher<>(c)
  , cache_(cache)
  , ids_(std::move(ids))
  , current_(ids_.begin())
{
}

emitter::~emitter()
{
    LOG(debug, store) << "removed emitter " << id();
}

void emitter::start()
{
    LOG(debug, store) << "starting emitter " << id();
    paused_ = false;
    io_.service().post(std::bind(&emitter::emit, shared_from_this()));
}

void emitter::pause()
{
    LOG(debug, store) << "pausing emitter " << id();
    paused_ = true;
}

void emitter::emit()
{
    if (paused_ || current_ == ids_.end())
        return;

    try
    {
        auto i = *current_++;
        std::shared_ptr<isegment> segment = cache_->retrieve(i);
        LOG(verbose, store) << "emitter " << id() << ": emitting segment " << i;
        segment->get([&](ze::event_ptr&& e) { send(e); });
    }
    catch (segment_exception const& e)
    {
        LOG(error, store) << e.what();
    }
    catch (ze::serialization::exception const& e)
    {
        LOG(error, store) << e.what();
    }

    io_.service().post(std::bind(&emitter::emit, shared_from_this()));
}

} // namespace store
} // namespace vast
