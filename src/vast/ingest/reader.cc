#include "vast/ingest/reader.h"

#include <ze/event.h>
#include "vast/fs/fstream.h"
#include "vast/ingest/bro-1.5/conn.h"
#include "vast/util/logger.h"
#include "vast/util/parser/streamer.h"

namespace vast {
namespace ingest {

reader::reader(ze::component& c, fs::path filename)
  : ze::publisher<>(c)
  , ze::actor<reader>(c)
  , filename_(std::move(filename))
{
}

void reader::act()
{
    fs::ifstream ifs(filename_);
    ifs.unsetf(std::ios::skipws);
    LOG(verbose, ingest) << "ingesting file " << filename_;
    if (parse(ifs))
    {
        LOG(verbose, ingest) << "succeeded ingesting " << filename_;
    }
    else
    {
        LOG(error, ingest) << "error occurred while ingesting " << filename_;
    }
}

bro_reader::bro_reader(ze::component& c, fs::path filename)
  : reader(c, std::move(filename))
{
}

bro_reader::~bro_reader()
{
}

bool bro_reader::parse(std::istream& in)
{
    typedef util::parser::streamer<
        ingest::bro15::parser::connection
      , ingest::bro15::parser::skipper
      , ingest::bro15::ast::conn
    > streamer;

    streamer s(
        [&](ingest::bro15::ast::conn const& c)
        {
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
        });

    return s.extract(in);
}

} // namespace ingest
} // namespace vast
