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
  , ze::actor<emitter>(c)
  , cache_(cache)
  , ids_(std::move(ids))
  , current_(ids_.begin())
{
    // FIXME: "empty" emitters should not be constructed in the first place.
    if (current_ == ids_.end())
        status(finished);
}

void emitter::act()
{
    try
    {
        assert(current_ != ids_.end());
        std::shared_ptr<isegment> segment = cache_->retrieve(*current_);
        auto remaining = segment->get_chunk([&](ze::event_ptr e) { send(e); });

        LOG(debug, store)
            << "emmitted chunk, " << remaining << " remaining (segment "
            << segment->id() << ")";

        // Advance to the next segment after having processed all chunks.
        if (remaining == 0 && ++current_ == ids_.end())
        {
            LOG(debug, store) << "emitter " << id() << ": finished";
            status(finished);
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

    enact();
}

} // namespace store
} // namespace vast
