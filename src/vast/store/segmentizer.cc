#include <vast/store/segmentizer.h>

#include <ze/event.h>
#include <vast/util/logger.h>

namespace vast {
namespace store {

segmentizer::segmentizer(cppa::actor_ptr segment_manager,
                         size_t max_events_per_chunk,
                         size_t max_segment_size)
  : writer_(segment_)
  , segment_manager_(segment_manager)
{
  LOG(verbose, store) << "spawning segmentizer @" << id();
  LOG(verbose, store)
    << "@" << id() << " has maximum segment size of " 
    << max_segment_size << " bytes";
  LOG(verbose, store)
    << "@" << id() << " uses at most " 
    << max_events_per_chunk << " events per chunk";

  using namespace cppa;
  init_state = (
      on_arg_match >> [=](ze::event const& e)
      {
        auto n = writer_ << e;
        if (n < max_events_per_chunk)
          return;

        if (segment_.bytes() < max_segment_size)
        {
          LOG(debug, store)
            << "@" << id() << " flushes chunk #" << segment_.size() + 1
            << " of segment " << segment_.id();

          writer_.flush_chunk();
          return;
        }

        LOG(debug, store)
          << "sending segment " << segment_.id() << " to archive";

        send(segment_manager_, std::move(segment_));
        segment_ = segment();
      },
      on(atom("shutdown")) >> [=]()
      {
        if (writer_.elements() == 0)
        {
          reply(atom("shutdown"), atom("ack"));
          terminate();
        }
        else
        {
          LOG(debug, store) << "sending last segment";
          send(segment_manager_, std::move(segment_));

          auto archive = self->last_sender();
          become(
              keep_behavior,
              on(atom("segment"), atom("ack"), segment_.id()) >>
                [=](ze::uuid const& id)
              {
                LOG(debug, store) << "segment manager acked segment " << id;
                send(archive, atom("shutdown"), atom("ack"));
                terminate();
              });
        }
      });
}

void segmentizer::terminate()
{
  cppa::self->quit();
  LOG(verbose, store) << "segmentizer @" << id() << " terminated";
}

} // namespace store
} // namespace vast
