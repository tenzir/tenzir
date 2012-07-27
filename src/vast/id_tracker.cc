#include "vast/id_tracker.h"

#include "vast/fs/exception.h"
#include "vast/fs/operations.h"
#include "vast/logger.h"

namespace vast {

id_tracker::id_tracker(std::string const& id_file)
{
  LOG(verbose, ingest)
    << "spawning id tracker @" << id()
    << " with id file " << id_file;

  if (! fs::exists(id_file))
  {
    LOG(info, ingest)
      << "id tracker @" << id()
      << " did not find an id file, starting from 0";
  }
  else
  {
    std::ifstream ifs(id_file);
    if (! ifs)
      throw fs::file_exception("could not open id file", id_file);

    ifs >> id_;
    LOG(info, ingest)
      << "id tracker @" << id()
      << " found an id file with highest id " << id_;
  }

  file_.open(id_file);
  file_.seekp(0);

  using namespace cppa;
  chaining(false);
  init_state = (
    on(atom("request"), arg_match) >> [=](size_t n)
    {
      if (std::numeric_limits<uint64_t>::max() - id_ < n)
      {
        LOG(error, ingest)
          << "id tracker @" << id()
          << " has not enough ids available to hand out " << n << " ids";
        reply(atom("id"), atom("failure"));
        return;
      }

      DBG(ingest)
        << "id tracker @" << id() << " hands out ["
        << id_ << ',' << id_ + n << ')';

      file_ << id_ + n << std::endl;
      if (file_)
        reply(atom("id"), id_, id_ + n);
      else
        reply(atom("id"), atom("failure"));

      id_ += n;

      file_.seekp(0);
    },
    on(atom("shutdown")) >> [=]
    {
      file_ << id_ << std::endl;
      DBG(ingest)
        << "id tracker @" << id() << " saves last event id " << id_;

      if (! file_)
        LOG(error, ingest)
          << "id tracker @" << id() << " could not save current event id";

      quit();
      LOG(verbose, ingest) << "id tracker @" << id() << " terminated";
    });
}

} // namespace vast
