#include <csignal>

#include <chrono>
#include <sstream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/config.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/data.hpp"
#include "vast/expression.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/consensus.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/profiler.hpp"
#include "vast/system/replicated_store.hpp"

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

namespace {

//behavior send_run(event_based_actor* self,
//                  stateful_actor<node::state>* node) {
//  return on("send", val<std::string>, "run") >> [=](std::string const& arg,
//                                                    std::string const&) {
//    auto rp = self->make_response_promise();
//    self->send(node->state.store, get_atom::value,
//               make_actor_key(arg, node->state.desc));
//    self->become(
//      [=](actor const& a, std::string const&) {
//        self->send(a, run_atom::value);
//        rp.deliver(make_message(ok_atom::value));
//        self->quit();
//      },
//      [=](none) {
//        rp.deliver(make_message(error{"no such actor: ", arg}));
//        self->quit(exit::error);
//      }
//    );
//  };
//}
//
//behavior send_flush(event_based_actor* self,
//                    stateful_actor<node::state>* node) {
//  return on("send", val<std::string>, "flush") >> [=](std::string const& arg,
//                                                      std::string const&) {
//    auto rp = self->make_response_promise();
//    self->send(node->state.store, get_atom::value,
//               make_actor_key(arg, node->state.desc));
//    self->become(
//      [=](actor const& a, std::string const& type) {
//        if (!(type == "index" || type == "archive")) {
//          rp.deliver(make_message(error{type, " does not support flushing"}));
//          self->quit(exit::error);
//          return;
//        }
//        self->send(a, flush_atom::value);
//        self->become(
//          [=](actor const& task) {
//            self->monitor(task);
//            self->become(
//              [=](down_msg const& msg) {
//                VAST_ASSERT(msg.source == task);
//                rp.deliver(make_message(ok_atom::value));
//                self->quit(exit::done);
//              }
//            );
//          },
//          [=](ok_atom) {
//            rp.deliver(self->current_message());
//            self->quit(exit::done);
//          },
//          [=](error const&) {
//            rp.deliver(self->current_message());
//            self->quit(exit::error);
//          },
//          others >> [=] {
//            rp.deliver(make_message(error{"unexpected response to FLUSH"}));
//            self->quit(exit::error);
//          },
//          after(time::seconds(10)) >> [=] {
//            rp.deliver(make_message(error{"FLUSH timed out"}));
//            self->quit(exit::error);
//          }
//        );
//      },
//      [=](none) {
//        rp.deliver(make_message(error{"no such actor: ", arg}));
//        self->quit(exit::error);
//      }
//    );
//  };
//}
//
//behavior quit_actor(event_based_actor* self,
//                    stateful_actor<node::state>* node) {
//  return on("quit", arg_match) >> [=](std::string const& arg) {
//    auto rp = self->make_response_promise();
//    self->send(node->state.store, get_atom::value,
//               make_actor_key(arg, node->state.desc));
//    self->become(
//      [=](actor const& a, std::string const&) {
//        self->send_exit(a, exit::stop);
//        rp.deliver(make_message(ok_atom::value));
//        self->quit();
//      },
//      [=](none) {
//        rp.deliver(make_message(error{"no such actor: ", arg}));
//        self->quit(exit::error);
//      }
//    );
//  };
//}
//

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
    //{"source", bind(spawn_source)},
    //{"sink", bind(spawn_sink)},
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
