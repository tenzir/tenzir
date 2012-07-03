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
}

void emitter::act()
{
    if (status() != running)
    {
        LOG(debug, store) << "emitter " << id() << status();
        return;
    }
    else if (segment_id_ == ids_.end())
    {
        LOG(debug, store) << "emitter " << id() << status();
        status(finished);
        return;
    }

    try
    {
        if (! segment_)
        {
            LOG(debug, store) << "emitter " << id()
                << " retrieves segment from cache";

            segment_ = cache_->retrieve(*segment_id_);
        }

        auto remaining = segment_->get_chunk([&](ze::event_ptr e) { send(e); });
//        LOG(debug, store)
//            << "emitted chunk, "
//            << remaining << " chunks remaining in segment " << segment_->id();

        if (remaining == 0)
        {
            if (++segment_id_ != ids_.end())
            {
                segment_.reset();
            }
            else
            {
                LOG(debug, store)
                    << "emitter " << id() << " processed all segments";

                status(finished);
                return;
            }
        }
    }
    catch (segment_exception const& e)
    {
        LOG(error, store) << e.what();
    }

    enact();
}

} // namespace store
} // namespace vast
