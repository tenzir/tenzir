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

emitter::state emitter::start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == finished)
        return state_;

    LOG(debug, store) << "starting emitter " << id();
    state_ = running;

    io_.service().post(std::bind(&emitter::emit, shared_from_this()));
    return state_;
}

emitter::state emitter::pause()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == finished || state_ == paused)
        return state_;

    LOG(debug, store) << "pausing emitter " << id();
    state_ = paused;
    return state_;
}

void emitter::emit()
{
    std::lock_guard<std::mutex> lock(mutex_);
    assert(state_ != finished);
    if (state_ == paused)
        return;

    try
    {
        std::shared_ptr<isegment> segment = cache_->retrieve(*current_);
        auto remaining = segment->get_chunk([&](ze::event_ptr e) { send(e); });

        LOG(debug, store)
            << "emmitted chunk, " << remaining << " remaining (segment "
            << segment->id() << ")";

        // Advance to the next segment after having processed all chunks.
        if (remaining == 0 && ++current_ == ids_.end())
        {
            LOG(debug, store) << "emitter " << id() << ": finished";
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
