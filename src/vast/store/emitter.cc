#include "vast/store/emitter.h"

#include <ze/event.h>
#include "vast/store/exception.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

emitter::emitter(segment_manager& sm, std::vector<ze::uuid> ids)
  : segment_manager_(sm)
  , ids_(std::move(ids))
  , current_(ids_.begin())
{
  using namespace cppa;

  auto shutdown = on<atom("shutdown")> >> []()
  {
    LOG(verbose, store) << "shutting down emitter " << id();
    self->quit();
  }

  init_state = (
      on(atom("sink"), arg_match) [=](actor_ptr sink)
      {
        sink_ = sink;
        send(sink, atom("set"), atom("source"), self);
        become(running_);
      },
      shutdown
      );

  running_ = (
      on(atom("emit")) >> []()
      {
        emit_chunk();
      },
      shutdown
      );
}

void emitter::emit_chunk()
{
  using namespace cppa;
  if (current_ == ids_.end())
  {
    LOG(debug, store) << "emitter " << id() << "has already finished";
    send(self, atom("shutdown"));
    return;
  }

  try
  {
    if (! segment_)
    {
      LOG(debug, store) 
        << "emitter " << id() << " retrieves segment from cache";

      segment_ = segment_manager_->retrieve(*current_);
    }

    auto remaining = segment_->process_chunk(
        [=](ze::event e)
        {
          send(sink_, std::move(e));
        });

    if (remaining == 0)
    {
      if (++current_ != ids_.end())
      {
        segment_.reset();
      }
      else
      {
        LOG(debug, store) << "emitter " << id() << "has finished";
        send(self, atom("shutdown"));
        return;
      }
    }
  }
  catch (segment_exception const& e)
  {
    LOG(error, store) << e.what();
  }
}

} // namespace store
} // namespace vast
