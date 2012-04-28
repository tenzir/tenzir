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
  , segment_id_(ids_.begin())
{
    // FIXME: "empty" emitters should not be constructed in the first place.
    if (segment_id_ == ids_.end())
        status(finished);
}

void emitter::act()
{
    if (status() != running)
    {
        LOG(debug, store) << "emitter " << id() << status();
        return;
    }

    assert(segment_id_ != ids_.end());

    try
    {
        if (! segment_)
            segment_ = cache_->retrieve(*segment_id_);

        auto remaining = segment_->get_chunk([&](ze::event_ptr e) { send(e); });

        LOG(debug, store)
            << "emmitted chunk, "
            << remaining << " chunks remaining in segment " << segment_->id();

        if (remaining == 0)
        {
            if (++segment_id_ != ids_.end())
            {
                segment_ = cache_->retrieve(*segment_id_);
                LOG(debug, store)
                    << "emitter " << id() << " advanced to next segment "
                    << *segment_id_;

                enact();
            }
            else
            {
                status(finished);
                LOG(debug, store)
                    << "emitter " << id() << " processed all segments";
            }
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
}

} // namespace store
} // namespace vast
