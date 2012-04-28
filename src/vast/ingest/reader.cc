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
        success = parse();
        ++events_;
    }
    while (success && events_ % batch_size_);

    if (success)
    {
        LOG(verbose, ingest) << "reader ingested " << events_ << " events";
        enact();
    }
    else
    {
        status(stopped);
        LOG(error, ingest) << "reader stopping due to parse error";
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

bool bro_reader::parse()
{
    ingest::bro15::ast::conn c;
    if (! streamer_.extract(c))
        return false;

    ze::event_ptr e(new ze::event);
    e->id(ze::uuid::random());
    e->name("bro::connection");
    e->timestamp(c.timestamp);

    if (c.duration)
    {
        auto dur = ze::double_seconds(*c.duration);
        e->emplace_back(std::chrono::duration_cast<ze::duration>(dur));
    }
    else
        e->emplace_back(ze::nil);

    e->push_back(c.orig_h);
    e->push_back(c.resp_h);
    if (c.service)
        e->emplace_back(std::move(*c.service));
    else
        e->push_back(ze::nil);

    ze::port::port_type p;
    if (c.proto == "tcp")
        p = ze::port::tcp;
    else if (c.proto == "udp")
        p = ze::port::udp;
    else if (c.proto == "icmp")
        p = ze::port::icmp;
    else
        p = ze::port::unknown;

    e->emplace_back(ze::port(c.orig_p, p));
    e->emplace_back(ze::port(c.resp_p, p));
    e->push_back(c.flags);

    if (c.addl)
        e->emplace_back(std::move(*c.addl));
    else
        e->push_back(ze::nil);

    send(e);
    return true;
}

} // namespace ingest
} // namespace vast
