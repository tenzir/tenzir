#include <caf/all.hpp>
#include <caf/detail/scope_guard.hpp>

#include "vast/config.h"
#include "vast/actor/sink/ascii.h"
#include "vast/actor/sink/bro.h"
#include "vast/actor/sink/json.h"
#include "vast/concept/parseable/vast/detail/to_schema.h"
#include "vast/concept/printable/vast/schema.h"
#include "vast/io/file_stream.h"
#include "vast/util/posix.h"

#ifdef VAST_HAVE_PCAP
#include "vast/actor/sink/pcap.h"
#endif

using namespace caf;
using namespace std::string_literals;

namespace vast {
namespace sink {

trial<caf::actor> spawn(message const& params)
{
  auto schema_file = ""s;
  auto output = ""s;
  auto r = params.extract_opts({
    {"schema,s", "alternate schema file", schema_file},
    {"write,w", "path to write events to", output},
    {"uds,u", "treat -w as UNIX domain socket to connect to"}
  });
  if (! r.error.empty())
    return error{std::move(r.error)};
  if (r.opts.count("write") == 0)
    return error{"no output specified (-w)"};
  // Setup a custom schema.
  schema sch;
  if (! schema_file.empty())
  {
    auto t = load_contents(schema_file);
    if (! t)
      return t.error();
    auto s = vast::detail::to_schema(*t);
    if (! s)
      return error{"failed to load schema: ", s.error()};
    sch = std::move(*s);
  }
  // Facilitate actor shutdown when returning with error.
  actor snk;
  auto guard = caf::detail::make_scope_guard(
    [&] { anon_send_exit(snk, exit::error); }
  );
  // The "pcap" and "bro" sink manually handle file output. All other
  // sources are file-based and we setup their input stream here.
  auto& format = params.get_as<std::string>(0);
  std::unique_ptr<io::output_stream> out;
  if (! (format == "pcap" || format == "bro"))
  {
    if (r.opts.count("uds") > 0)
    {
      auto uds = util::unix_domain_socket::connect(output);
      if (! uds)
        return error{"failed to connect to UNIX domain socket at ", output};
      auto remote_fd = uds.recv_fd(); // Blocks!
      out = std::make_unique<io::file_output_stream>(remote_fd);
    }
    else
    {
      out = std::make_unique<io::file_output_stream>(output);
    }
  }
  if (format == "pcap")
  {
#ifndef VAST_HAVE_PCAP
    return error{"not compiled with pcap support"};
#else
    auto flush = 10000u;
    r = r.remainder.extract_opts({
      {"flush,f", "flush to disk after this many packets", flush}
    });
    if (! r.error.empty())
      return error{std::move(r.error)};
    snk = caf::spawn<sink::pcap, priority_aware>(sch, output, flush);
#endif
  }
  else if (format == "bro")
  {
    snk = caf::spawn<sink::bro>(output);
  }
  else if (format == "ascii")
  {
    snk = caf::spawn<sink::ascii>(std::move(out));
  }
  else if (format == "json")
  {
    snk = caf::spawn<sink::json>(std::move(out));
  }
  else
  {
    return error{"invalid export format: ", format};
  }
  guard.disable();
  return snk;
}

} // namespace sink
} // namespace vast
