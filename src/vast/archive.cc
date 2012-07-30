#include "vast/archive.h"

#include <ze/event.h>
#include "vast/emitter.h"
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/segment_manager.h"
#include "vast/fs/operations.h"

namespace vast {

archive::archive(std::string const& directory, size_t max_segments)
{
  LOG(verbose, archive) << "spawning archive @" << id();
  if (! fs::exists(directory))
  {
    LOG(info, archive)
      << "archive @" << id() << " creates new directory " << directory;
    fs::mkdir(directory);
  }

  using namespace cppa;
  segment_manager_ = spawn<segment_manager>(max_segments, directory);
  init_state = (
      on(atom("emitter"), atom("create"), arg_match) >> [=](actor_ptr sink)
      {
        auto em = spawn<emitter>(segment_manager_, sink);
        emitters_.emplace(em, sink);
        monitor(sink);

        // TODO: The emitter should receive a list of IDs from the index once
        // we implemented the index. For now, "announce" just tells the segment
        // manager to go through all segments.
        send(em, atom("announce"));
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        DBG(archive)
            << "archive @" << id()
            << " detected termination of sink @" << last_sender()->id();

        for (auto i = emitters_.begin(); i != emitters_.end(); )
        {
          if (i->second == last_sender())
          {
            DBG(query)
              << "archive @" << id() << " removes emitter-to-sink mapping @"
              << i->first->id() << " -> @" << i->second->id();

            send(i->first, atom("shutdown"));
            i = emitters_.erase(i);
          }
          else
          {
            ++i;
          }
        }
      },
      on_arg_match >> [=](segment const& /* s */)
      {
        segment_manager_ << last_dequeued();
      },
      on(atom("shutdown")) >> [=]()
      {
        // TODO: wait for a signal from the ingestor that all segments have
        // been shipped.
        segment_manager_ << last_dequeued();

        for (auto& i : emitters_)
          i.first << last_dequeued();
        emitters_.clear();

        quit();
        LOG(verbose, archive) << "archive @" << id() << " terminated";
      });
}

} // namespace vast
