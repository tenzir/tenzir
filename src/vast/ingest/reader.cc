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
  init_state = (
      on(atom("read"), arg_match) >> [=](std::string const& filename)
      {
        fs::ifstream ifs(filename);
        ifs.unsetf(std::ios::skipws);
        assert(ifs.good());

        auto n = extract(ifs, upstream_);

        if (n < 0)
        {
          send(upstream_, atom("failure"));
        }
        else
        {
          LOG(verbose, ingest) << "reader ingested " << events_ << " events";
          send(upstream, atom("success"));
        }

        self->quit();
      });
}

size_t bro_reader::extract(std::ifstream& ifs)
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
  > streamer;

  size_t n = 0;
  while (! streamer.done())
  {
    if (! ifs.good())
      break;

    ze::event event;
    event.id(ze::uuid::random());
    event.name("bro::connection");

    if (! streamer_.extract(event))
    {
      LOG(error, ingest) << "reader stopping due to parse error";

      // TODO: Send upstream what we got so far.
      //if (! events.empty())
      //  cppa::send(upstream_, std::move(events));

      break;
    }

    events.push_back(std::move(event));

    if (events.size() % batch_size == 0)
    {
      // TODO: announce std::vector<ze::event>
      //cppa::send(upstream_, std::move(events));

      n += events.size();

      // FIXME: what's the easiest way to reinitialize a moved vector?
      events = decltype(events)();
      events.reserve(batch_size);
    }
  }

  return n;
}

} // namespace ingest
} // namespace vast
