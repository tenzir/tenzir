#include "vast/store/emitter.h"

#include <ze/event.h>
#include "vast/store/exception.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

emitter::emitter(cppa::actor_ptr segment_manager)
  : segment_manager_(segment_manager)
{
  using namespace cppa;

  auto shutdown = on(atom("shutdown")) >> [=]
  {
    LOG(verbose, store) << "shutting down emitter " << id();
    ids_.clear();
    self->quit();
  };

  init_state = (
      on(atom("set"), atom("sink"), arg_match) >> [=](actor_ptr sink)
      {
        sink_ = sink;
      },
      on(atom("ids"), arg_match) >> [=](std::vector<ze::uuid> const& ids)
      {
        std::copy(ids.begin(), ids.end(), std::back_inserter(ids_));
        become(running_);
      },
      shutdown
      );

  running_ = (
      on(atom("emit")) >> [=]()
      {
        emit_chunk();
      },
      shutdown
      );
}

void emitter::emit_chunk()
{
  using namespace cppa;

  if (ids_.empty())
  {
    LOG(debug, store) << "emitter " << id() << " has no more segment IDs";
    send(self, atom("shutdown"));
    return;
  }

  try
  {
    if (! segment_)
    {
      LOG(debug, store) 
        << "emitter " << id() << ": fetching segment " << ids_.front();

      send(segment_manager_, atom("retrieve"), ids_.front());
      ids_.pop_front();

      become(
          keep_behavior,
          on_arg_match >> [=](segment const& /* s */)
          {
            auto opt = tuple_cast<segment>(self->last_dequeued());
            assert(opt.valid());
            segment_tuple_ = *opt;
            segment_ = &get<0>(segment_tuple_);

            current_chunk_ = 0;
            last_chunk_ = segment_->size();
            assert(segment_->size() > 0);

            unbecome();
          },
          on(atom("shutdown")) >> [=]
          {
            send(self, atom("shutdown"));
            unbecome();
          });
    }

    LOG(debug, store) 
      << "emitter " << id() << ": sending chunk #" << current_chunk_;

    auto chunk = (*segment_)[current_chunk_++];
    send(sink_, chunk);

    if (current_chunk_ == last_chunk_)
    {
      segment_ = nullptr;
      if (ids_.empty())
      {
        LOG(debug, store) << "emitter " << id() << "has finished";
        send(self, atom("shutdown"));
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
