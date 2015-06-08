#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/config.h"
#include "vast/expression.h"
#include "vast/query_options.h"
#include "vast/actor/accountant.h"
#include "vast/actor/node.h"
#include "vast/actor/sink/ascii.h"
#include "vast/actor/sink/bro.h"
#include "vast/actor/sink/json.h"
#include "vast/actor/source/bro.h"
#include "vast/actor/source/bgpdump.h"
#include "vast/actor/source/test.h"
#include "vast/expr/normalize.h"
#include "vast/io/file_stream.h"
#include "vast/util/posix.h"

#ifdef VAST_HAVE_PCAP
#include "vast/actor/sink/pcap.h"
#include "vast/actor/source/pcap.h"
#endif

using namespace caf;
using namespace std::string_literals;

namespace vast {

message node::spawn_source(std::string const& label, message const& params)
{
  auto batch_size = uint64_t{100000};
  auto schema_file = ""s;
  auto input = ""s;
  auto r = params.extract_opts({
    {"batch,b", "number of events to ingest at once", batch_size},
    {"schema,s", "alternate schema file", schema_file},
    {"dump-schema,d", "print schema and exit"},
    {"read,r", "path to read events from", input},
    {"uds,u", "treat -r as UNIX domain socket to connect to"}
  });
  if (! r.error.empty())
    return make_message(error{std::move(r.error)});
  auto& format = params.get_as<std::string>(0);
  // The "pcap" and "test" sources manually verify the presence of
  // input. All other sources are file-based and we setup their input
  // stream here.
  std::unique_ptr<io::input_stream> in;
  if (! (format == "pcap" || format == "test"))
  {
    if (r.opts.count("read") == 0 || input.empty())
    {
      VAST_ERROR(this, "didn't specify valid input (-r)");
      return make_message(error{"no valid input specified (-r)"});
    }
    if (r.opts.count("uds") > 0)
    {
      auto uds = util::unix_domain_socket::connect(input);
      if (! uds)
      {
        auto err = "failed to connect to UNIX domain socket at ";
        VAST_ERROR(this, err << input);
        return make_message(error{err, input});
      }
      auto remote_fd = uds.recv_fd(); // Blocks!
      in = std::make_unique<io::file_input_stream>(remote_fd);
    }
    else
    {
      in = std::make_unique<io::file_input_stream>(input);
    }
  }
  auto dump_schema = r.opts.count("dump-schema") > 0;
  // Facilitate actor shutdown when returning with error.
  bool terminate = true;
  actor src;
  struct terminator
  {
    terminator(std::function<void()> f) : f(f) { }
    ~terminator() { f(); }
    std::function<void()> f;
  };
  terminator guard{[&] { if (terminate) send_exit(src, exit::error); }};
  // Spawn a source according to format.
  if (format == "pcap")
  {
#ifndef VAST_HAVE_PCAP
    return make_message(error{"not compiled with pcap support"});
#else
    auto flow_max = uint64_t{1} << 20;
    auto flow_age = 60u;
    auto flow_expiry = 10u;
    auto cutoff = std::numeric_limits<size_t>::max();
    auto pseudo_realtime = int64_t{0};
    r = r.remainder.extract_opts({ // -i overrides -r
      {"interface,i", "the interface to read packets from", input},
      {"cutoff,c", "skip flow packets after this many bytes", cutoff},
      {"flow-max,m", "number of concurrent flows to track", flow_max},
      {"flow-age,a", "max flow lifetime before eviction", flow_age},
      {"flow-expiry,e", "flow table expiration interval", flow_expiry},
      {"pseudo-realtime,p", "factor c delaying trace packets by 1/c",
                            pseudo_realtime}
    });
    if (! r.error.empty())
      return make_message(error{std::move(r.error)});
    if (input.empty())
    {
      VAST_ERROR(this, "didn't specify input (-r or -i)");
      return make_message(error{"no input specified (-r or -i)"});
    }
    src = spawn<source::pcap, priority_aware + detached>(
      input, cutoff, flow_max, flow_age, flow_expiry, pseudo_realtime);
#endif
  }
  else if (format == "test")
  {
    auto id = event_id{0};
    auto events = uint64_t{100};
    r = r.remainder.extract_opts({
      {"id,i", "the base event ID", id},
      {"events,n", "number of events to generate", events}
    });
    if (! r.error.empty())
      return make_message(error{std::move(r.error)});
    src = spawn<source::test, priority_aware>(id, events);
  }
  else if (format == "bro")
  {
    src = spawn<source::bro, priority_aware + detached>(
      std::move(in));
  }
  else if (format == "bgpdump")
  {
    src = spawn<source::bgpdump, priority_aware + detached>(
      std::move(in));
  }
  else
  {
    return make_message(error{"invalid import format: ", format});
  }
  attach_functor([=](uint32_t ec) { anon_send_exit(src, ec); });
  // Set a new schema if provided.
  if (! schema_file.empty())
  {
    auto t = load_and_parse<schema>(path{schema_file});
    if (! t)
      return make_message(error{"failed to load schema: ", t.error()});
    send(src, put_atom::value, *t);
  }
  // Dump the schema.
  if (dump_schema)
  {
    auto res = make_message(ok_atom::value);
    scoped_actor self;
    self->sync_send(src, get_atom::value, schema_atom::value).await(
      [&](schema const& sch) { res = make_message(to_string(sch)); }
    );
    return res;
  };
  // Set parameters.
  send(src, batch_atom::value, batch_size);
  send(src, put_atom::value, accountant_atom::value, accountant_);
  // Save it.
  terminate = false;
  return put({src, "source", label});
}

message node::spawn_sink(std::string const& label, message const& params)
{
  auto schema_file = ""s;
  auto output = ""s;
  auto r = params.extract_opts({
    {"schema,s", "alternate schema file", schema_file},
    {"write,w", "path to write events to", output},
    {"uds,u", "treat -w as UNIX domain socket to connect to"}
  });
  if (! r.error.empty())
    return make_message(error{std::move(r.error)});
  if (r.opts.count("write") == 0)
  {
    VAST_ERROR(this, "didn't specify output (-w)");
    return make_message(error{"no output specified (-w)"});
  }
  // Setup a custom schema.
  schema sch;
  if (! schema_file.empty())
  {
    auto t = load_and_parse<schema>(path{schema_file});
    if (! t)
    {
      VAST_ERROR(this, "failed to load schema", schema_file);
      return make_message(error{"failed to load schema: ", t.error()});
    }
    sch = std::move(*t);
  }
  // Facilitate actor shutdown when returning with error.
  actor snk;
  bool terminate = true;
  struct terminator
  {
    terminator(std::function<void()> f) : f(f) { }
    ~terminator() { f(); }
    std::function<void()> f;
  };
  terminator guard{[&] { if (terminate) send_exit(snk, exit::error); }};
  auto& format = params.get_as<std::string>(0);
  // The "pcap" and "bro" sink manually handle file output. All other
  // sources are file-based and we setup their input stream here.
  std::unique_ptr<io::output_stream> out;
  if (! (format == "pcap" || format == "bro"))
  {
    if (r.opts.count("uds") > 0)
    {
      auto uds = util::unix_domain_socket::connect(output);
      if (! uds)
      {
        auto err = "failed to connect to UNIX domain socket at ";
        VAST_ERROR(this, err << output);
        return make_message(error{err, output});
      }
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
    return make_message(error{"not compiled with pcap support"});
#else
    auto flush = 10000u;
    r = r.remainder.extract_opts({
      {"flush,f", "flush to disk after this many packets", flush}
    });
    if (! r.error.empty())
      return make_message(error{std::move(r.error)});
    snk = spawn<sink::pcap, priority_aware>(sch, output, flush);
#endif
  }
  else if (format == "bro")
  {
    snk = spawn<sink::bro>(output);
  }
  else if (format == "ascii")
  {
    snk = spawn<sink::ascii>(std::move(out));
  }
  else if (format == "json")
  {
    snk = spawn<sink::json>(std::move(out));
  }
  else
  {
    return make_message(error{"invalid export format: ", format});
  }
  attach_functor([=](uint32_t ec) { anon_send_exit(snk, ec); });
  terminate = false;
  return put({snk, "sink", label});
}


} // namespace vast
