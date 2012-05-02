#include "vast/ingest/reader.h"

#include <ze/event.h>
#include "vast/util/logger.h"

namespace vast {
namespace ingest {

reader::reader(ze::component& c, fs::path const& filename)
  : ze::publisher<>(c)
  , ze::actor<reader>(c)
  , file_(filename)
{
    file_.unsetf(std::ios::skipws);
}

void reader::act()
{
    if (status() != running)
    {
        LOG(debug, ingest) << "reader " << id() << status();
        return;
    }

    bool success = true;
    do
    {
        ze::event_ptr event(new ze::event);
        success = parse(*event);
        ++events_;
        if (success)
            send(event);
    }
    while (success && events_ % batch_size_);

    if (success)
    {
        LOG(verbose, ingest) << "reader ingested " << events_ << " events";
        enact();
    }
    else if (! done())
    {
        status(stopped);
        LOG(error, ingest) << "reader stopping due to parse error";
    }
    else
    {
        status(finished);
        LOG(info, ingest) << "reader ingested file successfully";
    }
}

bro_reader::bro_reader(ze::component& c, fs::path const& filename)
  : reader(c, filename)
  , streamer_(file_)
{
}

bro_reader::~bro_reader()
{
}

bool bro_reader::parse(ze::event& event)
{
    if (! streamer_.extract(event))
        return false;

    event.id(ze::uuid::random());
    event.name("bro::connection");

    return true;
}

bool bro_reader::done()
{
    return streamer_.done();
}

} // namespace ingest
} // namespace vast
