#include <vast/ingest/id_tracker.h>

#include <vast/fs/exception.h>
#include <vast/fs/operations.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

id_tracker::id_tracker(std::string const& id_file)
{
  LOG(verbose, ingest)
    << "spawning id_tracker @" << id()
    << " with id file " << id_file;

  if (! fs::exists(id_file))
  {
    LOG(info, ingest)
      << "id tracker @" << id()
      << " did not found an id file, starting from 1";

    std::ofstream ofs(id_file);
    if (! ofs)
      throw fs::file_exception("could not open id file", id_file);

    ofs << 1 << std::endl;
  }
  else
  {
    std::ifstream ifs(id_file);
    if (! ifs)
      throw fs::file_exception("could not open id file", id_file);

    ifs >> id_;
    if (id_ == 0)
    {
      LOG(warn, ingest)
        << "id tracker @" << id() << " discards invalid id file with id 0";
      id_ = 1;
    }
    else
    {
      LOG(info, ingest)
        << "id tracker @" << id()
        << " found an id file with highest id " << id_;
    }
  }

  file_.open(id_file);
  file_.seekp(0);

  using namespace cppa;
  chaining(false);
  init_state = (
    on(atom("request"), arg_match) >> [=](uint64_t n)
    {
      if (std::numeric_limits<uint64_t>::max() - id_ < n)
      {
        LOG(error, ingest)
          << "id tracker @" << id()
          << " has not enough ids available to hand out " << n << " ids";
        reply(atom("id"), atom("failure"));
        return;
      }

      auto lo = id_ + 1;
      auto hi = id_ + n;

      LOG(verbose, ingest)
        << "id tracker @" << id()
        << " hands out (" << lo << ',' << hi << ']';

      file_ << hi << std::endl;
      if (file_)
        reply(atom("id"), lo, hi);
      else
        reply(atom("id"), atom("failure"));

      file_.seekp(0);
    },
    on(atom("shutdown")) >> [=]
    {
      file_ << id_ << std::endl;
      if (! file_)
        LOG(error, ingest)
          << "id tracker @" << id()
          << " could not save current event id";

      quit();
      LOG(verbose, ingest) << "id tracker @" << id() << " terminated";
    });
}

} // namespace ingest
} // namespace vast
