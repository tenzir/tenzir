#include "vast/ingest/reader.h"

#include <ze/event.h>
#include "vast/fs/fstream.h"
#include "vast/ingest/bro-1.5/conn.h"
#include "vast/util/logger.h"
#include "vast/util/parser/streamer.h"

namespace vast {
namespace ingest {

reader::reader(cppa::actor_ptr upstream)
  : upstream_(upstream)
{
  using namespace cppa;
  init_state = (
      on(atom("read"), arg_match) >> [=](std::string const& filename)
      {
        fs::ifstream ifs(filename);
        ifs.unsetf(std::ios::skipws);
        assert(ifs.good());

        size_t n = 0;;
        if (extract(ifs, n))
        {
          send(upstream_, atom("failure"));
        }
        else
        {
          LOG(verbose, ingest) << "reader ingested " << n << " events";
          send(upstream, atom("success"));
        }

        self->quit();
        LOG(verbose, ingest) << "reader " << id() << " terminated";
      });
}

bro_reader::bro_reader(cppa::actor_ptr upstream)
  : reader(upstream)
{
}

bool bro_reader::extract(std::ifstream& ifs, size_t& n)
{
  if (! ifs.good())
    return false;

  size_t batch_size = 1000;   // FIXME: make configurable.
  std::vector<ze::event> events;
  events.reserve(batch_size);

  util::parser::streamer<
      ingest::bro15::parser::connection
    , ingest::bro15::parser::skipper
    , ze::event
  > streamer(ifs);

  while (! streamer.done())
  {
    if (! ifs.good())
    {
      LOG(error, ingest) << "bad stream";
      return false;
    }

    ze::event event;
    event.id(ze::uuid::random());
    event.name("bro::connection");

    if (! streamer.extract(event))
    {
      LOG(error, ingest) << "reader stopping due to parse error";
      if (! events.empty())
        cppa::send(upstream_, std::move(events));
      return false;
    }

    ++n;
    events.push_back(std::move(event));

    if (events.size() % batch_size == 0)
    {
      // TODO: announce std::vector<ze::event>
      //cppa::send(upstream_, std::move(events));

      // FIXME: what's the easiest way to reinitialize a moved vector?
      events = decltype(events)();
      events.reserve(batch_size);
    }
  }

  return n;
}

} // namespace ingest
} // namespace vast
