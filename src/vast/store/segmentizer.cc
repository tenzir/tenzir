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
    << "segmentizer @" << id() << " has maximum segment size of "
    << max_segment_size << " bytes";
  LOG(verbose, store)
    << "segmentizer @" << id() << " uses at most "
    << max_events_per_chunk << " events per chunk";

  auto write_event = [=](ze::event const& e)
    {
      auto n = writer_ << e;
      if (n < max_events_per_chunk)
        return;

      if (segment_.bytes() < max_segment_size)
      {
        writer_.flush_chunk();
        return;
      }

      LOG(debug, store)
        << "segmentizer @" << id()
        << " sends segment " << segment_.id() << " to archive";

      send(segment_manager_, std::move(segment_));
      segment_ = segment();
    };

  using namespace cppa;
  init_state = (
      on_arg_match >> write_event,
      on_arg_match >> [=](std::vector<ze::event> const& v)
      {
        for (auto& e : v)
          write_event(e);
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
          LOG(debug, store)
            << "segmentizer @" << id()
            << " sends last segment " << segment_.id();

          send(segment_manager_, std::move(segment_));
          auto archive = self->last_sender();
          become(
              keep_behavior,
              on(atom("segment"), atom("ack"), arg_match) >>
                [=](ze::uuid const& id)
              {
                send(archive, atom("shutdown"), atom("ack"));
                terminate();
              },
              after(std::chrono::seconds(30)) >> [=]
              {
                LOG(debug, store)
                  << "segmentizer @" << id()
                  << " did not receive shutdown ack from segment manager @"
                  << segment_manager_->id();
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
