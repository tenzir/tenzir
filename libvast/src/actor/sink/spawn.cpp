#include <fstream>
#include <ostream>

#include <caf/all.hpp>
#include <caf/detail/scope_guard.hpp>

#include "vast/config.hpp"
#include "vast/actor/sink/ascii.hpp"
#include "vast/actor/sink/bro.hpp"
#include "vast/actor/sink/csv.hpp"
#include "vast/actor/sink/json.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/vast/schema.hpp"
#include "vast/util/fdostream.hpp"
#include "vast/util/posix.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/actor/sink/pcap.hpp"
#endif

using namespace std::string_literals;

namespace vast {
namespace sink {

trial<actor> spawn(message const& params) {
  auto schema_file = ""s;
  auto output = "-"s;
  auto r = params.extract_opts({
    {"schema,s", "alternate schema file", schema_file},
    {"write,w", "path to write events to", output},
    {"uds,u", "treat -w as UNIX domain socket to connect to"}
  });
  // Setup a custom schema.
  schema sch;
  if (!schema_file.empty()) {
    auto t = load_contents(schema_file);
    if (!t)
      return t.error();
    auto s = to<schema>(*t);
    if (!s)
      return error{"failed to load schema"};
    sch = std::move(*s);
  }
  // Facilitate actor shutdown when returning with error.
  actor snk;
  auto guard = make_scope_guard([&] { anon_send_exit(snk, exit::error); });
  // The "pcap" and "bro" sink manually handle file output. All other
  // sources are file-based and we setup their input stream here.
  auto& format = params.get_as<std::string>(0);
  std::unique_ptr<std::ostream> out;
  if (!(format == "pcap" || format == "bro")) {
    if (r.opts.count("uds") > 0) {
      if (output == "-")
        return error{"cannot use stdout as UNIX domain socket"};
      auto uds = util::unix_domain_socket::connect(output);
      if (!uds)
        return error{"failed to connect to UNIX domain socket at ", output};
      auto remote_fd = uds.recv_fd(); // Blocks!
      out = std::make_unique<util::fdostream>(remote_fd);
    } else if (output == "-") {
      out = std::make_unique<util::fdostream>(1); // stdout
    } else {
      out = std::make_unique<std::ofstream>(output);
    }
  }
  if (format == "pcap") {
#ifndef VAST_HAVE_PCAP
    return error{"not compiled with pcap support"};
#else
    auto flush = 10000u;
    r = r.remainder.extract_opts({
      {"flush,f", "flush to disk after this many packets", flush}
    });
    if (!r.error.empty())
      return error{std::move(r.error)};
    snk = caf::spawn<priority_aware>(pcap, sch, output, flush);
#endif
  } else if (format == "bro") {
    snk = caf::spawn(bro, output);
  } else if (format == "csv") {
    snk = caf::spawn(csv, std::move(out));
  } else if (format == "ascii") {
    snk = caf::spawn(ascii, std::move(out));
  } else if (format == "json") {
    r = r.remainder.extract_opts({
      {"flatten,f", "flatten records"}
    });
    snk = caf::spawn(sink::json, std::move(out), r.opts.count("flatten") > 0);
  // FIXME: currently the "vast export" command cannot take sink parameters,
  // which is why we add a hacky convenience sink called "flat-json". We should
  // have a command line format akin to "vast export json -f query ...",
  // which would allow passing both sink and exporter arguments.
  } else if (format == "flat-json") {
    snk = caf::spawn(sink::json, std::move(out), true);
  } else {
    return error{"invalid export format: ", format};
  }
  guard.disable();
  return snk;
}

} // namespace sink
} // namespace vast
