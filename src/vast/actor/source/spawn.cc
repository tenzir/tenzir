#include <caf/all.hpp>
#include <caf/detail/scope_guard.hpp>

#include "vast/config.h"
#include "vast/actor/source/bro.h"
#include "vast/actor/source/bgpdump.h"
#include "vast/actor/source/test.h"
#include "vast/concept/parseable/vast/detail/to_schema.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/schema.h"
#include "vast/io/file_stream.h"
#include "vast/util/posix.h"

#ifdef VAST_HAVE_PCAP
#include "vast/actor/source/pcap.h"
#endif

using namespace caf;
using namespace std::string_literals;

namespace vast {
namespace source {

trial<caf::actor> spawn(message const& params) {
  auto batch_size = uint64_t{100000};
  auto schema_file = ""s;
  auto input = "-"s;
  auto r = params.extract_opts({
    {"batch,b", "number of events to ingest at once", batch_size},
    {"schema,s", "alternate schema file", schema_file},
    {"read,r", "path to read events from", input},
    {"uds,u", "treat -r as UNIX domain socket to connect to"}
  });
  auto& format = params.get_as<std::string>(0);
  // The "pcap" and "test" sources manually verify the presence of
  // input. All other sources are file-based and we setup their input
  // stream here.
  std::unique_ptr<io::input_stream> in;
  if (!(format == "pcap" || format == "test")) {
    if (r.opts.count("uds") > 0) {
      if (input == "-")
        return error{"cannot use stdin as UNIX domain socket"};
      auto uds = util::unix_domain_socket::connect(input);
      if (!uds)
        return error{"failed to connect to UNIX domain socket at ", input};
      auto remote_fd = uds.recv_fd(); // Blocks!
      in = std::make_unique<io::file_input_stream>(remote_fd);
    } else {
      in = std::make_unique<io::file_input_stream>(input);
    }
  }
  // Facilitate shutdown when returning with error.
  actor src;
  auto guard = caf::detail::make_scope_guard(
    [&] { anon_send_exit(src, exit::error); }
  );
  // Spawn a source according to format.
  if (format == "pcap") {
#ifndef VAST_HAVE_PCAP
    return error{"not compiled with pcap support"};
#else
    auto flow_max = uint64_t{1} << 20;
    auto flow_age = 60u;
    auto flow_expiry = 10u;
    auto cutoff = std::numeric_limits<size_t>::max();
    auto pseudo_realtime = int64_t{0};
    r = r.remainder.extract_opts({// -i overrides -r
      {"interface,i", "the interface to read packets from", input},
      {"cutoff,c", "skip flow packets after this many bytes", cutoff},
      {"flow-max,m", "number of concurrent flows to track", flow_max},
      {"flow-age,a", "max flow lifetime before eviction", flow_age},
      {"flow-expiry,e", "flow table expiration interval", flow_expiry},
      {"pseudo-realtime,p", "factor c delaying trace packets by 1/c",
       pseudo_realtime}
    });
    if (!r.error.empty())
      return error{std::move(r.error)};
    if (input.empty())
      return error{"no input specified (-r or -i)"};
    src = caf::spawn<pcap, priority_aware + detached>(
      input, cutoff, flow_max, flow_age, flow_expiry, pseudo_realtime);
#endif
  } else if (format == "test") {
    auto id = event_id{0};
    auto events = uint64_t{100};
    r = r.remainder.extract_opts({
      {"id,i", "the base event ID", id},
      {"events,e", "number of events to generate", events}
    });
    if (!r.error.empty())
      return error{std::move(r.error)};
    src = caf::spawn<test, priority_aware>(id, events);
    // The test source doesn't consume any data, it only generates events.
    // Therefore we can use the input channel for the schema.
    schema_file = input;
  } else if (format == "bro") {
    src = caf::spawn<bro, priority_aware + detached>(std::move(in));
  } else if (format == "bgpdump") {
    src = caf::spawn<bgpdump, priority_aware + detached>(std::move(in));
  } else {
    return error{"invalid import format: ", format};
  }
  // Set a new schema if provided.
  if (!schema_file.empty()) {
    auto t = load_contents(schema_file);
    if (!t)
      return t.error();
    auto s = vast::detail::to_schema(*t);
    if (!s)
      return error{"failed to load schema: ", s.error()};
    anon_send(src, put_atom::value, *s);
  }
  // Set parameters.
  anon_send(src, batch_atom::value, batch_size);
  // Done.
  guard.disable();
  return src;
}

} // namespace source
} // namespace vast
