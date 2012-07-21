#include <vast/ingest/reader.h>

#include <ze/event.h>
#include <vast/ingest/exception.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

reader::reader(cppa::actor_ptr upstream, std::string const& filename)
  : upstream_(upstream)
  , file_(filename)
{
  LOG(verbose, ingest)
    << "spawning reader @" << id()
    << " for file " << filename
    << " with upstream @" << upstream_->id();

  if (file_)
    file_.unsetf(std::ios::skipws);
  else
    LOG(error, ingest) << "reader @" << id() << " cannot read " << filename;

  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("extract"), arg_match) >> [=](size_t batch_size)
      {
        if (! file_)
        {
          LOG(error, ingest)
            << "reader @" << id() << " experienced an error with " << filename;

          reply(atom("reader"), atom("done"));
        }

        auto events = std::move(extract(file_, 500));
        if (! events.empty())
        {
          total_events_ += events.size();

          LOG(debug, ingest)
            << "reader @" << id()
            << " sends " << events.size()
            << " events to @" << upstream_->id()
            << " (cumulative events: " << total_events_ << ')';

          send(upstream_, std::move(events));
        }

        reply(atom("reader"), atom("ack"));
      },
      on(atom("shutdown")) >> [=]
      {
        self->quit();
        LOG(verbose, ingest) << "reader @" << id() << " terminated";
      });
}

line_reader::line_reader(cppa::actor_ptr upstream, std::string const& filename)
  : reader(std::move(upstream), filename)
{
}

std::vector<ze::event> line_reader::extract(std::ifstream& ifs, size_t batch_size)
{
  std::vector<ze::event> events;
  events.reserve(batch_size);

  size_t n = 0;
  size_t errors = 0;
  std::string line;
  std::getline(ifs, line);

  do
  {
    try
    {
      ++current_line_;
      events.emplace_back(parse(line));
      ++n;
    }
    catch (parse_exception const& e)
    {
      LOG(warn, ingest)
        << "reader @" << id() << " encountered parse error at line " << current_line_
        << ": " << e.what();

      if (++errors >= 20)
        break;
    }
  }
  while (std::getline(ifs, line) && events.size() % batch_size != 0);

  return events;
}

bro_15_conn_reader::bro_15_conn_reader(cppa::actor_ptr upstream,
                                       std::string const& filename)
  : line_reader(std::move(upstream), filename)
{
}

ze::event bro_15_conn_reader::parse(std::string const& line)
{
  if (line.empty())
    throw parse_exception("empty line in conn.log");

  // A connection record.
  ze::event e("bro::connection");
  e.id(ze::uuid::random());

  field_splitter<std::string::const_iterator> fs(line.begin(), line.end());
  auto& i = fs.start();
  auto& j = fs.end();

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

  return e;
}

} // namespace ingest
} // namespace vast
