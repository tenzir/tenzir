#include <vast/ingest/reader.h>

#include <ze/event.h>
#include <vast/fs/fstream.h>
#include <vast/ingest/exception.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

reader::reader(cppa::actor_ptr upstream)
  : upstream_(upstream)
{
  LOG(verbose, core)
    << "spawning reader @" << id() << " with upstream @" << upstream_->id();

  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("read"), arg_match) >> [=](std::string const& filename)
      {
        LOG(verbose, core)
          << "reader @" << id() << " ingests file " << filename;

        fs::ifstream ifs(filename);
        ifs.unsetf(std::ios::skipws);
        assert(ifs.good());

        auto success = extract(ifs);
        send(upstream_,
             atom("read"),
             success ? atom("success") : atom("failure"));

        self->quit();
        LOG(verbose, ingest) << "reader @" << id() << " terminated";
      });
}

bro_reader::bro_reader(cppa::actor_ptr upstream)
  : reader(upstream)
{
}

bool bro_reader::extract(std::ifstream& ifs)
{
  if (! ifs.good())
    return false;

  size_t batch_size = 500;   // FIXME: make configurable.
  std::vector<ze::event> events;
  events.reserve(batch_size);

  std::string line;
  size_t n = 0;
  size_t lines = 0;
  while (std::getline(ifs, line))
  {
    try
    {
      ++lines;
      if (line.empty())
      {
        LOG(warn, ingest) << "encountered empty line in conn.log";
        continue;
      }

      field_splitter<std::string::const_iterator> fs(line.begin(), line.end());
      auto& i = fs.start();
      auto& j = fs.end();

      // A connection record.
      ze::event e("bro::connection");
      e.id(ze::uuid::random());

      // Timestamp
      if (*j != ' ')
        throw parse_exception("invalid conn.log timestamp (field 1)");
      e.emplace_back(ze::value::parse_time_point(i, j));

      // Duration
      if (! fs.advance())
        throw parse_exception("invalid conn.log duration (field 2)");
      e.emplace_back(*i == '?' ? ze::nil : ze::value::parse_duration(i, j));

      // Originator address
      if (! fs.advance())
        throw parse_exception("invalid conn.log originating address (field 3)");
      e.emplace_back(ze::value::parse_address(i, j));

      // Responder address
      if (! fs.advance())
        throw parse_exception("invalid conn.log responding address (field 4)");
      e.emplace_back(ze::value::parse_address(i, j));

      // Service
      if (! fs.advance())
        throw parse_exception("invalid conn.log service (field 5)");
      e.emplace_back(*i == '?' ? ze::nil : ze::value(i, j));

      // Ports and protocol
      if (! fs.advance())
        throw parse_exception("invalid conn.log originating port (field 6)");
      auto orig_p = ze::value::parse_port(i, j);
      if (! fs.advance())
        throw parse_exception("invalid conn.log responding port (field 7)");
      auto resp_p = ze::value::parse_port(i, j);
      if (! fs.advance())
        throw parse_exception("invalid conn.log proto (field 8)");
      auto proto = ze::value(i, j);
      auto str = proto.get<ze::string>().data();
      auto len = proto.get<ze::string>().size();
      auto p = ze::port::unknown;
      if (! std::strncmp(str, "tcp", len))
        p = ze::port::tcp;
      else if (! std::strncmp(str, "udp", len))
        p = ze::port::udp;
      else if (! std::strncmp(str, "icmp", len))
        p = ze::port::icmp;
      orig_p.get<ze::port>().type(p);
      resp_p.get<ze::port>().type(p);
      e.emplace_back(std::move(orig_p));
      e.emplace_back(std::move(resp_p));
      e.emplace_back(std::move(proto));

      // Originator and responder bytes
      if (! fs.advance())
        throw parse_exception("invalid conn.log originating bytes (field 9)");
      e.emplace_back(*i == '?' ? ze::nil : ze::value::parse_uint(i, j));
      if (! fs.advance())
        throw parse_exception("invalid conn.log responding bytes (field 10)");
      e.emplace_back(*i == '?' ? ze::nil : ze::value::parse_uint(i, j));

      // Connection state
      if (! fs.advance())
        throw parse_exception("invalid conn.log connection state (field 11)");
      e.emplace_back(ze::value(i, j));

      // Direction
      if (! fs.advance())
        throw parse_exception("invalid conn.log direction (field 12)");
      e.emplace_back(ze::value(i, j));

      // Additional information
      if (fs.advance())
        e.emplace_back(i, j);

      events.push_back(std::move(e));

      ++n;

      if (events.size() % batch_size == 0)
      {
        LOG(debug, ingest)
          << "reader @" << id()
          << " sends " << batch_size
          << " events to @" << upstream_->id()
          << " (cumulative total: " << n << ')';

        cppa::send(upstream_, std::move(events));
        events.clear();
        events.reserve(batch_size);
      }
    }
    catch (parse_exception const& e)
    {
      LOG(error, ingest)
        << "reader @" << id() << " encountered parse error at line " << lines
        << ": " << e.what();
    }
  }

  if (! events.empty())
  {
    LOG(debug, ingest)
      << "reader @" << id()
      << " sends last " << events.size()
      << " events to @" << upstream_->id();

    cppa::send(upstream_, std::move(events));
  }

  LOG(info, ingest) << "reader @" << id() << " ingested " << n << " events";

  return true;
}

} // namespace ingest
} // namespace vast
