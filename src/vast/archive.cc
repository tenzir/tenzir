#include "vast/archive.h"

#include <ze/event.h>
#include "vast/emitter.h"
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/segmentizer.h"
#include "vast/segment_manager.h"

namespace vast {

archive::archive(std::string const& directory,
                 size_t max_events_per_chunk,
                 size_t max_segment_size,
                 size_t max_segments)
{
  LOG(verbose, store) << "spawning archive @" << id();
  using namespace cppa;
  segment_manager_ = spawn<segment_manager>(max_segments, directory);
  segmentizer_ = spawn<segmentizer>(segment_manager_,
                                    max_events_per_chunk,
                                    max_segment_size);
  init_state = (
      on(atom("emitter"), atom("create"), arg_match) >> [=](actor_ptr sink)
      {
        auto em = spawn<emitter>(segment_manager_, sink);
        send(em, atom("announce"));
        emitters_.push_back(em);
      },
      on_arg_match >> [=](ze::event const& e)
      {
        segmentizer_ << last_dequeued();
      },
      on_arg_match >> [=](std::vector<ze::event> const& v)
      {
        DBG(store) << "archive @" << id()
          << " forwards " << v.size() << " events to segmentizer @"
          << segmentizer_->id();

        segmentizer_ << last_dequeued();
      },
      on(atom("shutdown")) >> [=]()
      {
        segmentizer_ << last_dequeued();
        become(
            keep_behavior,
            on(atom("shutdown"), atom("ack")) >> [=]
            {
              send(segment_manager_, atom("shutdown"));
              for (auto em : emitters_)
                send(em, atom("shutdown"));

              quit();
              LOG(verbose, store) << "archive @" << id() << " terminated";
            },
            after(std::chrono::seconds(30)) >> [=]
            {
              LOG(error, store) << "archive @" << id()
                << " did not receive shutdown ack from segmentizer @"
                << segmentizer_->id();

              quit();
              LOG(verbose, store) << "archive @" << id() << " terminated";
            });
      });
}

} // namespace vast
