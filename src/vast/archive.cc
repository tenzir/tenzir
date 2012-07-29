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
        send(em, atom("announce"));
        emitters_.push_back(em);
      },
      on_arg_match >> [=](segment const& /* s */)
      {
        segment_manager_ << last_dequeued();
      },
      on(atom("shutdown")) >> [=]()
      {
        // TODO: wait for segments from the ingestor.

        segment_manager_ << last_dequeued();
        for (auto em : emitters_)
          em << last_dequeued();

        quit();
        LOG(verbose, archive) << "archive @" << id() << " terminated";
      });
}

} // namespace vast
