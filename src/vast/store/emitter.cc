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
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (state_ == finished)
        return;

    LOG(debug, store) << "starting emitter " << id();
    state_ = running;
    state_mutex_.unlock();

    io_.service().post(std::bind(&emitter::emit, shared_from_this()));
}

void emitter::pause()
{
    LOG(debug, store) << "pausing emitter " << id();
    std::lock_guard<std::mutex> lock(state_mutex_);
    state_ = paused;
}

void emitter::emit()
{
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        assert(state_ != finished);
        if (state_ == paused)
            return;
    }

    try
    {
        std::shared_ptr<isegment> segment = cache_->retrieve(*current_);
        auto remaining = segment->get_chunk([&](ze::event_ptr&& e) { send(e); });

        LOG(debug, store)
            << "emitter " << id()
            << ": emmitted chunk, " << remaining << " remaining";

        // Advance to the next segment after having processed all chunks.
        if (remaining == 0 && ++current_ == ids_.end())
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            state_ = finished;
            return;
        }
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
