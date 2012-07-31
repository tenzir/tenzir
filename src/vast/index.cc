#include "vast/index.h"

#include "vast/logger.h"
#include "vast/fs/operations.h"
#include "vast/fs/fstream.h"

namespace vast {

index::index(cppa::actor_ptr archive, std::string directory)
  : dir_(std::move(directory))
  , archive_(archive)
{
  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("load")) >> [=]
      {
        LOG(verbose, index) << "spawning index @" << id();
        if (! fs::exists(dir_))
        {
          LOG(info, index)
            << "index @" << id() << " creates new directory " << dir_;
          fs::mkdir(dir_);
        }

        assert(fs::exists(dir_));
        fs::each_file_entry(
            dir_,
            [&](fs::path const& p)
            {
              fs::ifstream file(p, std::ios::binary | std::ios::in);
              ze::serialization::stream_iarchive ia(file);
              segment::header hdr;
              ia >> hdr;

              build(hdr);
            });
      },
      on(atom("hit"), atom("all")) >> [=]
      {
        if (ids_.empty())
        {
          reply(atom("miss"));
          return;
        }

        std::vector<ze::uuid> ids;
        std::copy(ids_.begin(), ids_.end(), std::back_inserter(ids));
        reply(atom("hit"), std::move(ids));
      },
      on(atom("hit"), atom("name"), arg_match) >> [=](std::string const& str)
      {
        // TODO: Implement
      },
      on(atom("hit"), atom("time"), arg_match)
        >> [=](int64_t start, int64_t end)
      {
        // TODO: Implement
      },
      on(atom("build"), arg_match) >> [=](segment const& s)
      {
        process(s);
      },
      on(atom("shutdown")) >> [=]()
      {
        quit();
        LOG(verbose, index) << "index @" << id() << " terminated";
      });
}

void index::process(segment const& s)
{
  write(s);
  build(s.head());
}

void index::write(segment const& s)
{
  auto path = fs::path(dir_) / s.id().to_string();
  fs::ofstream file(path, std::ios::binary | std::ios::out);
  ze::serialization::stream_oarchive oa(file);
  oa << s.head();

  DBG(index)
    << "index @" << id() << " wrote segment header to " << path;
}

void index::build(segment::header const& hdr)
{
  LOG(verbose, index) << "index @" << id() 
    << " builds in-memory indexes for segment " << hdr.id;

  assert(ids_.count(hdr.id) == 0);
  ids_.insert(hdr.id);

// TODO: Remove as soon as newer GCC versions have adopted r181022.
#ifdef __clang__
  for (auto& event : hdr.event_names)
    event_names_.emplace(event, hdr.id);

  start_.emplace(hdr.start, hdr.id);
  end_.emplace(hdr.end, hdr.id);
#else
  for (auto& event : hdr.event_names)
    event_names_.insert({event, hdr.id});

  start_.insert({hdr.start, hdr.id});
  end_.insert({hdr.end, hdr.id});
#endif
}

} // namespace vast
