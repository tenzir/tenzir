#include <csignal>

#include <chrono>
#include <fstream>
#include <sstream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/config.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/data.hpp"
#include "vast/expression.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/pcap.hpp"
#include "vast/format/test.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"

#include "vast/detail/posix.hpp"
#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/fdostream.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/consensus.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/profiler.hpp"
#include "vast/system/replicated_store.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/source.hpp"

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

namespace {

void stop(stateful_actor<node_state>* self) {
  self->send_exit(self, exit_reason::user_shutdown);
}

void peer(stateful_actor<node_state>* self, message& args) {
  auto rp = self->make_response_promise();
  if (args.empty()) {
    rp.deliver(make_error(ec::syntax_error, "no endpoint given"));
    return;
  }
  auto ep = to<endpoint>(args.get_as<std::string>(0));
  if (!ep) {
    rp.deliver(make_error(ec::parse_error, "invalid endpoint format"));
    return;
  }
  // Use localhost:42000 by default.
  if (ep->host.empty())
    ep->host = "127.0.0.1";
  if (ep->port == 0)
    ep->port = 42000;
  VAST_DEBUG(self, "connects to", ep->host << ':' << ep->port);
  auto& mm = self->system().middleman();
  auto peer = mm.remote_actor(ep->host.c_str(), ep->port);
  if (!peer) {
    VAST_ERROR(self, "failed to connect to peer:",
               self->system().render(peer.error()));
    rp.deliver(peer.error());
    return;
  }
  VAST_DEBUG(self, "sends peering request");
  auto t = actor_cast<actor>(self->state.tracker);
  rp.delegate(*peer, peer_atom::value, t, self->state.name);
}

void show(stateful_actor<node_state>* self, message& /* args */) {
  auto rp = self->make_response_promise();
  self->request(self->state.tracker, infinite, get_atom::value).then(
    [=](const component_map& components) mutable {
      json::object result;
      for (auto& peer : components) {
        json::array xs;
        for (auto& pair : peer.second)
          xs.push_back(pair.first + '#' + to_string(pair.second->id()));
        result.emplace(peer.first, std::move(xs));
      }
      rp.deliver(to_string(json{std::move(result)}));
    }
  );
}

expected<actor> spawn_metastore(stateful_actor<node_state>* self, message& xs) {
  auto server_id = raft::server_id{0};
  auto r = xs.extract_opts({
    {"id,i", "the static ID of the consensus module", server_id}
  });
  if (server_id == 0)
    return make_error(ec::unspecified, "invalid server ID: 0");
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  auto metastore_dir = self->state.dir / "meta";
  auto consensus = self->spawn(raft::consensus, metastore_dir, server_id);
  auto s = self->spawn(replicated_store<std::string, data>, consensus, 10000ms);
  return actor_cast<actor>(s);
}

expected<actor> spawn_archive(stateful_actor<node_state>* self, message& xs) {
  auto mss = size_t{128};
  auto segments = size_t{10};
  auto r = xs.extract_opts({
    {"segments,s", "number of cached segments", segments},
    {"max-segment-size,m", "maximum segment size in MB", mss}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  auto a = self->spawn(archive, self->state.dir / "archive", segments, mss);
  return actor_cast<actor>(a);
}

expected<actor> spawn_index(stateful_actor<node_state>* self, message& xs) {
  uint64_t max_events = 1 << 20;
  uint64_t passive = 10;
  auto r = xs.extract_opts({
    {"events,e", "maximum events per partition", max_events},
    {"passive,p", "maximum number of passive partitions", passive}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  return self->spawn(index, self->state.dir / "index", max_events, passive);
}

expected<actor> spawn_importer(stateful_actor<node_state>* self, message& xs) {
  auto ids = size_t{128};
  auto r = xs.extract_opts({
    {"ids,n", "number of initial IDs to request", ids},
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  return self->spawn(importer, self->state.dir / "importer", ids);
}

expected<actor> spawn_exporter(stateful_actor<node_state>* self, message& xs) {
  std::string expr_str;
  auto r = xs.extract_opts({
    {"expression,e", "the query expression", expr_str},
    {"continuous,c", "marks a query as continuous"},
    {"historical,h", "marks a query as historical"},
    {"unified,u", "marks a query as unified"},
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  // Parse expression.
  auto expr = to<expression>(expr_str);
  if (!expr)
    return expr.error();
  *expr = normalize(*expr);
  VAST_DEBUG(self, "normalized query expression to:", *expr);
  // Parse query options.
  auto query_opts = no_query_options;
  if (r.opts.count("continuous") > 0)
    query_opts = query_opts + continuous;
  if (r.opts.count("historical") > 0)
    query_opts = query_opts + historical;
  if (r.opts.count("unified") > 0)
    query_opts = unified;
  if (query_opts == no_query_options)
    return make_error(ec::syntax_error, "got query w/o options (-h, -c, -u)");
  return self->spawn(exporter, std::move(*expr), query_opts);
}


expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, bool is_uds) {
  if (is_uds) {
    if (input == "-")
      return make_error(ec::filesystem_error,
                        "cannot use stdin as UNIX domain socket");
    auto uds = detail::unix_domain_socket::connect(input);
    if (!uds)
      return make_error(ec::filesystem_error,
                        "failed to connect to UNIX domain socket at", input);
    auto remote_fd = uds.recv_fd(); // Blocks!
    auto sb = std::make_unique<detail::fdinbuf>(remote_fd);
    return std::make_unique<std::istream>(sb.release());
  }
  if (input == "-") {
    auto sb = std::make_unique<detail::fdinbuf>(0); // stdin
    return std::make_unique<std::istream>(sb.release());
  }
  auto fb = std::make_unique<std::filebuf>();
  fb->open(input, std::ios_base::binary | std::ios_base::in);
  return std::make_unique<std::istream>(fb.release());
}

expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, bool is_uds) {
  if (is_uds) {
      return make_error(ec::filesystem_error,
                        "cannot use stdout as UNIX domain socket");
    auto uds = detail::unix_domain_socket::connect(output);
    if (!uds)
      return make_error(ec::filesystem_error,
                        "failed to connect to UNIX domain socket at", output);
    auto remote_fd = uds.recv_fd(); // Blocks!
    return std::make_unique<detail::fdostream>(remote_fd);
  }
  if (output == "-")
    return std::make_unique<detail::fdostream>(1); // stdout
  return std::make_unique<std::ofstream>(output);
}

expected<actor> spawn_source(stateful_actor<node_state>* self, message& xs) {
  if (xs.empty())
    return make_error(ec::syntax_error, "missing format");
  auto& format = xs.get_as<std::string>(0);
  auto source_args = xs.drop(1);
  // Parse common parameters first.
  auto input = "-"s;
  std::string schema_file;
  auto r = source_args.extract_opts({
    {"read,r", "path to input where to read events from", input},
    {"schema,s", "path to alternate schema", schema_file},
    {"uds,u", "treat -r as listening UNIX domain socket"}
  });
  actor src;
  // Parse source-specific parameters, if any.
  if (format == "pcap") {
#ifndef VAST_HAVE_PCAP
    return make_error(ec::unspecified, "not compiled with pcap support");
#else
    auto flow_max = uint64_t{1} << 20;
    auto flow_age = 60u;
    auto flow_expiry = 10u;
    auto cutoff = std::numeric_limits<size_t>::max();
    auto pseudo_realtime = int64_t{0};
    r = r.remainder.extract_opts({
      {"cutoff,c", "skip flow packets after this many bytes", cutoff},
      {"flow-max,m", "number of concurrent flows to track", flow_max},
      {"flow-age,a", "max flow lifetime before eviction", flow_age},
      {"flow-expiry,e", "flow table expiration interval", flow_expiry},
      {"pseudo-realtime,p", "factor c delaying trace packets by 1/c",
       pseudo_realtime}
    });
    if (!r.error.empty())
      return make_error(ec::syntax_error, r.error);
    format::pcap::reader reader{input, cutoff, flow_max, flow_age, flow_expiry,
                                pseudo_realtime};
    src = self->spawn(source<format::pcap::reader>, std::move(reader));
#endif
  } else if (format == "bro" || format == "bgpdump") {
    auto in = make_input_stream(input, r.opts.count("uds") > 0);
    if (!in)
      return in.error();
    if (format == "bro") {
      format::bro::reader reader{std::move(*in)};
      src = self->spawn(source<format::bro::reader>, std::move(reader));
    } else /* if (format == "bgpdump") */ {
      format::bgpdump::reader reader{std::move(*in)};
      src = self->spawn(source<format::bgpdump::reader>, std::move(reader));
    }
  } else if (format == "test") {
    auto seed = size_t{0};
    auto id = event_id{0};
    auto n = uint64_t{100};
    r = r.remainder.extract_opts({
      {"seed,s", "the PRNG seed", seed},
      {"events,n", "number of events to generate", n},
      {"id,i", "the base event ID", id}
    });
    if (!r.error.empty())
      return make_error(ec::syntax_error, r.error);
    format::test::reader reader{seed, n, id};
    src = self->spawn(source<format::test::reader>, std::move(reader));
    // Since the test source doesn't consume any data and only generates
    // events out of thin air, we use the input channel to specify the schema.
    schema_file = input;
  } else {
    return make_error(ec::syntax_error, "invalid format:", format);
  }
  // Supply an alternate schema, if requested.
  if (!schema_file.empty()) {
    auto str = load_contents(schema_file);
    if (!str)
      return str.error();
    auto sch = to<schema>(*str);
    if (!sch)
      return sch.error();
    // Send anonymously, since we can't process the reply here.
    self->anon_send(src, put_atom::value, std::move(*sch));
  }
  return src;
}

expected<actor> spawn_sink(stateful_actor<node_state>* self, message& xs) {
  if (xs.empty())
    return make_error(ec::syntax_error, "missing format");
  auto& format = xs.get_as<std::string>(0);
  auto sink_args = xs.drop(1);
  // Parse common parameters first.
  auto output = "-"s;
  auto schema_file = ""s;
  auto r = sink_args.extract_opts({
    {"write,w", "path to write events to", output},
    //{"schema,s", "alternate schema file", schema_file},
    {"uds,u", "treat -w as UNIX domain socket to connect to"}
  });
  actor snk;
  // Parse sink-specific parameters, if any.
  if (format == "pcap") {
#ifndef VAST_HAVE_PCAP
    return make_error(ec::unspecified, "not compiled with pcap support");
#else
    auto flush = 10000u;
    r = r.remainder.extract_opts({
      {"flush,f", "flush to disk after this many packets", flush}
    });
    if (!r.error.empty())
      return make_error(ec::syntax_error, r.error);
    format::pcap::writer writer{output, flush};
    snk = self->spawn(sink<format::pcap::writer>, std::move(writer));
#endif
  } else if (format == "bro") {
    format::bro::writer writer{output};
    snk = self->spawn(sink<format::bro::writer>, std::move(writer));
  } else {
    auto out = make_output_stream(output, r.opts.count("uds") > 0);
    if (!out)
      return out.error();
    if (format == "csv") {
      format::csv::writer writer{std::move(*out)};
      snk = self->spawn(sink<format::csv::writer>, std::move(writer));
    } else if (format == "ascii") {
      format::ascii::writer writer{std::move(*out)};
      snk = self->spawn(sink<format::ascii::writer>, std::move(writer));
    } else if (format == "json") {
      format::json::writer writer{std::move(*out)};
      snk = self->spawn(sink<format::json::writer>, std::move(writer));
    } else {
      return make_error(ec::syntax_error, "invalid format:", format);
    }
  }
  return snk;
}

#ifdef VAST_HAVE_GPERFTOOLS
expected<actor> spawn_profiler(stateful_actor<node_state>* self, message& xs) {
  auto resolution = 1u;
  auto r = xs.extract_opts({
    {"cpu,c", "start the CPU profiler"},
    {"heap,h", "start the heap profiler"},
    {"resolution,r", "seconds between measurements", resolution}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  auto secs = std::chrono::seconds(resolution);
  auto prof = self->spawn(profiler, self->state.dir, secs);
  if (r.opts.count("cpu") > 0)
    self->send(prof, start_atom::value, cpu_atom::value);
  if (r.opts.count("heap") > 0)
    self->send(prof, start_atom::value, heap_atom::value);
  return prof;
}
#else
expected<actor> spawn_profiler(stateful_actor<node_state>*, message&) {
  return make_error(ec::unspecified, "not compiled with gperftools");
}
#endif

void spawn(stateful_actor<node_state>* self, message& args) {
  auto rp = self->make_response_promise();
  if (args.empty()) {
    rp.deliver(make_error(ec::syntax_error, "missing arguments"));
    return;
  }
  using factory_function = std::function<expected<actor>(message&)>;
  auto bind = [=](auto f) { return [=](message& xs) { return f(self, xs); }; };
  static auto factory = std::unordered_map<std::string, factory_function>{
    {"metastore", bind(spawn_metastore)},
    {"archive", bind(spawn_archive)},
    {"index", bind(spawn_index)},
    {"importer", bind(spawn_importer)},
    {"exporter", bind(spawn_exporter)},
    {"source", bind(spawn_source)},
    {"sink", bind(spawn_sink)},
    {"profiler", bind(spawn_profiler)}
  };
  // Split arguments into two halves at the command.
  factory_function fun;
  const std::string* cmd = nullptr;
  size_t i;
  for (i = 0; i < args.size(); ++i) {
    auto j = factory.find(args.get_as<std::string>(i));
    if (j != factory.end()) {
      cmd = &j->first;
      fun = j->second;
      break;
    }
  }
  if (!fun) {
    rp.deliver(make_error(ec::unspecified, "invalid spawn component"));
    return;
  }
  // Parse spawn args.
  auto spawn_args = args.take(i);
  std::string node;
  std::string label;
  auto r = spawn_args.extract_opts({
    {"node,n", "the node where to spawn the component", node},
    {"label,l", "a unique label for the component", label}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    rp.deliver(make_error(ec::syntax_error, "invalid syntax", invalid));
  }
  if (!r.error.empty())
    rp.deliver(make_error(ec::syntax_error, std::move(r.error)));
  // Dispatch command.
  auto cmd_args = args.take_right(args.size() - i - 1);
  auto a = fun(cmd_args);
  if (!a)
    rp.deliver(a.error());
  else
    rp.delegate(self->state.tracker, put_atom::value, *cmd, *a);
}

void kill(stateful_actor<node_state>* self, message& /* args */) {
  auto rp = self->make_response_promise();
  rp.deliver(make_error(ec::unspecified, "not yet implemented"));
}

void send(stateful_actor<node_state>* self, message& /* args */) {
  auto rp = self->make_response_promise();
  rp.deliver(make_error(ec::unspecified, "not yet implemented"));
}

} // namespace <anonymous>

caf::behavior node(stateful_actor<node_state>* self, std::string name,
                   path dir) {
  self->state.dir = std::move(dir);
  self->state.name = std::move(name);
  // Bring up the topology tracker.
  self->state.tracker = self->spawn<linked>(tracker, self->state.name);
  // Bring up the accountant and put it in the actor registry. All
  // accounting-aware actors look for the accountant in the registry.
  auto accounting_log = self->state.dir / "log" / "current" / "accounting.log";
  auto acc = self->spawn<linked>(accountant, accounting_log);
  auto ptr = actor_cast<strong_actor_ptr>(acc);
  self->system().registry().put(accountant_atom::value, ptr);
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      self->send_exit(self->state.tracker, msg.reason);
      self->quit(msg.reason);
    }
  );
  return {
    [=](const std::string& cmd, message& args) {
      VAST_DEBUG(self, "got command", cmd, deep_to_string(args));
      if (cmd == "stop") {
        stop(self);
      } else if (cmd == "peer") {
        peer(self, args);
      } else if (cmd == "show") {
        show(self, args);
      } else if (cmd == "spawn") {
        spawn(self, args);
      } else if (cmd == "kill") {
        kill(self, args);
      } else if (cmd == "send") {
        send(self, args);
      } else {
        auto e = make_error(ec::unspecified, "invalid command", cmd);
        VAST_INFO(self, self->system().render(e));
        self->make_response_promise().deliver(std::move(e));
      }
    },
    [=](peer_atom, actor& tracker, std::string& peer_name) {
      self->delegate(self->state.tracker, peer_atom::value,
                     std::move(tracker), std::move(peer_name));
    },
    [=](signal_atom, int signal) {
      VAST_INFO(self, "got signal", ::strsignal(signal));
    }
  };
}

} // namespace system
} // namespace vast
