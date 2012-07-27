#include "vast/emitter.h"

#include <ze/event.h>
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/segment.h"

namespace vast {

emitter::emitter(cppa::actor_ptr segment_manager, cppa::actor_ptr sink)
  : segment_manager_(segment_manager)
  , sink_(sink)
{
  using namespace cppa;
  LOG(verbose, store) 
    << "spawning emitter @" << id() << " with sink @" << sink_->id();

  init_state = (
      on(atom("announce")) >> [=]
      {
        // TODO: The index should give the archive a list of segment IDs that
        // we hand to this emitter, that then will query the segment manager to
        // give us the corresponding segments.
        
        send(segment_manager_, atom("all ids"));
      },
      on(atom("ids"), arg_match) >> [=](std::vector<ze::uuid> const& ids)
      {
        std::copy(ids.begin(), ids.end(), std::back_inserter(ids_));
        send(sink_, atom("source"), self);
      },
      on(atom("emit")) >> [=]()
      {
        if (ids_.empty())
        {
          LOG(debug, store) << "emitter @" << id() << " has no segment IDs";
          send(sink, atom("finished"));
          return;
        }

        if (! segment_)
          retrieve_segment();
        else
          emit_chunk();
      },
      on(atom("shutdown")) >> [=]
      {
        ids_.clear();
        self->quit();
        LOG(verbose, store) << "emitter @" << id() << " terminated";
      });
}

void emitter::retrieve_segment()
{
  using namespace cppa;
  LOG(debug, store) 
    << "emitter @" << id() << " retrieves segment " << ids_.front();

  send(segment_manager_, atom("retrieve"), ids_.front());
  become(
      keep_behavior,
      on_arg_match >> [=](segment const& s)
      {
        ids_.pop_front();

        auto opt = tuple_cast<segment>(self->last_dequeued());
        assert(opt.valid());
        segment_tuple_ = *opt;
        segment_ = &get<0>(segment_tuple_);

        current_chunk_ = 0;
        last_chunk_ = segment_->size();
        assert(segment_->size() > 0);

        // FIXME: why does this fail? For now, we directly call emit_chunk().
        send(self, atom("emit"));
        emit_chunk();

        unbecome();
      },
      others() >> [=]
      {
        LOG(error, store) 
          << "invalid message";

        unbecome();
      },
      after(std::chrono::seconds(10)) >> [=]
      {
        LOG(error, store) 
          << "emitter @" << id() << " did not receive segment " << ids_.front();
        unbecome();
      });
}

void emitter::emit_chunk()
{
  using namespace cppa;
  LOG(debug, store) 
    << "emitter @" << id() << " sends chunk #" << current_chunk_;

  assert(segment_);
  sink_ << (*segment_)[current_chunk_++];

  if (current_chunk_ == last_chunk_)
  {
    LOG(debug, store) 
      << "emitter @" << id() 
      << " reached last chunk of segment" << segment_->id();

    segment_ = nullptr;
    if (ids_.empty())
    {
      LOG(debug, store) << "emitter @" << id() << " has finished";
      send(sink_, atom("finished"));
    }
  }
}

} // namespace vast
