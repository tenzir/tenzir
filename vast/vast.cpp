#include "vast/config.hpp"

#ifdef VAST_LINUX
#include <unistd.h> // daemon(3)
#endif

#include <csignal>
#include <cstdlib>
#include <cstring> // strsignal(3)

#include <algorithm>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include <caf/io/all.hpp>

#include "vast/aliases.hpp"
#include "vast/announce.hpp"
#include "vast/banner.hpp"
#include "vast/caf.hpp"
#include "vast/filesystem.hpp"
#include "vast/key.hpp"
#include "vast/logger.hpp"
#include "vast/uuid.hpp"
#include "vast/actor/accountant.hpp"
#include "vast/actor/atoms.hpp"
#include "vast/actor/node.hpp"
#include "vast/actor/signal_monitor.hpp"
#include "vast/actor/sink/spawn.hpp"
#include "vast/actor/source/spawn.hpp"
#include "vast/concept/parseable/parse.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/time.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/util/flat_set.hpp"
#include "vast/util/string.hpp"
#include "vast/util/system.hpp"
#include "vast/util/terminal.hpp"

using namespace vast;
using namespace std::string_literals;

// Temporary helper function until we have a better way to reconfigure a NODE
// at runtime.
std::pair<message, message> parse_core_args(message const& input) {
  auto result = make_message("spawn", "core");
  std::string id_batch_size;
  std::string archive_comp;
  std::string archive_segments;
  std::string archive_size;
  std::string index_events;
  std::string index_active;
  std::string index_passive;
  auto r = input.extract_opts({
    {"identifier-batch-size", "initial identifier batch size", id_batch_size},
    {"archive-compression", "archive compression algorithm", archive_comp},
    {"archive-segments", "archive in-memory segments", archive_segments},
    {"archive-size", "archive segment size", archive_size},
    {"index-events", "maximum number of events per partition", index_events},
    {"index-active", "number of active partitions", index_active},
    {"index-passive", "number of passive partitions", index_passive},
    // FIXME: Because extract_opts unfortunately *always* defines -h, we have
    // to "haul it through" if it was set. :-/
    {"historical,h", "marks a query as historical"},
  });
  if (r.opts.count("identifier-batch-size") > 0)
    result = result + make_message("--identifier-batch-size=", id_batch_size);
  if (r.opts.count("archive-compression") > 0)
    result = result + make_message("--archive-compression=" + archive_comp);
  if (r.opts.count("archive-segments") > 0)
    result = result + make_message("--archive-segments=" + archive_segments);
  if (r.opts.count("archive-size") > 0)
    result = result + make_message("--archive-size=" + archive_size);
  if (r.opts.count("index-events") > 0)
    result = result + make_message("--index-events=" + index_events);
  if (r.opts.count("index-active") > 0)
    result = result + make_message("--index-active=" + index_active);
  if (r.opts.count("index-passive") > 0)
    result = result + make_message("--index-passive=" + index_passive);
  // FIXME: see not above.
  if (r.opts.count("historical") > 0)
    r.remainder = r.remainder + make_message("-h" + index_passive);
  return {result, r.remainder};
}

int run_start(actor const& node, std::string const& name,
              endpoint const& node_endpoint) {
  VAST_ASSERT(!node->is_remote());
  scoped_actor self;
  auto sig_mon = self->spawn<linked>(signal_monitor::make, self);
  try {
    // Publish the node.
    auto host =
      node_endpoint.host.empty() ? nullptr : node_endpoint.host.c_str();
    auto bound_port = caf::io::publish(node, node_endpoint.port, host);
    VAST_VERBOSE("listening on", (host ? host : "") << ':' << bound_port,
                 "with name \"" << name << '"');
    self->monitor(node);
    auto stop = false;
    self->do_receive(
      [&](down_msg const& msg) {
        VAST_ASSERT(msg.source == node);
        VAST_DEBUG("received DOWN from node");
        stop = true;
      },
      [&](signal_atom, int signal) {
        VAST_DEBUG("got " << ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM)
          self->send_exit(node, exit::stop);
        else
          self->send(node, signal_atom::value, signal);
      },
      others >> [&] {
        VAST_WARN("received unexpected message:",
                   to_string(self->current_message()));
      }
    ).until([&] { return stop; });
    auto er = node->exit_reason();
    if (er == exit::error || !(er == exit::done || er == exit::stop))
      return 1;
  } catch (caf::network_error const& e) {
    VAST_ERROR(e.what());
    self->send_exit(node, exit::stop);
    return 1;
  } catch (...) {
    VAST_ERROR("terminating due to uncaught exception");
    return 1;
  }
  self->send_exit(sig_mon, exit::stop);
  self->await_all_other_actors_done();
  return 0;
}

int run_import(actor const& node, message args) {
  scoped_actor self;
  auto sig_mon = self->spawn<linked>(signal_monitor::make, self);
  // 1. Spawn a SOURCE.
  auto src = source::spawn(std::move(args));
  if (!src) {
    VAST_ERROR("failed to spawn source:", src.error());
    return 1;
  }
  auto source_guard = make_scope_guard(
    [=] { anon_send_exit(*src, exit::kill); }
  );
  // 2. Hook the SOURCE up with the ACCOUNTANT.
  self->sync_send(node, store_atom::value, list_atom::value,
                  key::str("actors")).await(
    [&](std::map<std::string, message>& m) {
      for (auto& p : m)
        p.second.apply({
          [&](accountant::type const& a) {
            self->send(*src, a);
          }
        });
    }
  );
  // 3. Find all IMPORTERs to load-balance across them.
  std::vector<actor> importers;
  self->sync_send(node, store_atom::value, list_atom::value,
                  key::str("actors")).await(
    [&](std::map<std::string, message>& m) {
      for (auto& p : m)
        p.second.apply({
          [&](actor const& a, std::string const& type) {
            VAST_ASSERT(a != invalid_actor);
            if (type == "importer")
              importers.push_back(a);
          }
        });
    }
  );
  if (importers.empty()) {
    VAST_ERROR("no importers found");
    return 1;
  }
  // 4. Connect SOURCE and IMPORTERs.
  for (auto& imp : importers) {
    VAST_ASSERT(imp != invalid_actor);
    VAST_DEBUG("connecting source with importer", imp);
    self->send(*src, put_atom::value, sink_atom::value, imp);
  }
  // 5. Run the SOURCE.
  source_guard.disable();
  self->send(*src, run_atom::value);
  self->monitor(*src);
  auto stop = false;
  self->do_receive(
    [&](down_msg const& msg) {
      VAST_ASSERT(msg.source == *src);
      VAST_DEBUG("received DOWN from source");
      stop = true;
    },
    [&](signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(*src, exit::stop);
      else
        VAST_VERBOSE("ignoring signal", ::strsignal(signal));
    },
    others >> [&] {
      VAST_WARN("received unexpected message:",
                 to_string(self->current_message()));
    }
  ).until([&] { return stop; });
  if (!node->is_remote()) {
    self->monitor(node);
    self->send_exit(node, exit::stop);
    self->receive([](down_msg const&) { });
  }
  self->send_exit(sig_mon, exit::stop);
  self->await_all_other_actors_done();
  return 0;
}

int run_export(actor const& node, message sink_args, message export_args) {
  scoped_actor self;
  auto sig_mon = self->spawn<linked>(signal_monitor::make, self);
  // 1. Spawn a SINK.
  auto snk = sink::spawn(std::move(sink_args));
  if (!snk) {
    VAST_ERROR("failed to spawn sink:", snk.error());
    return 1;
  }
  auto sink_guard = make_scope_guard(
    [snk = *snk] { anon_send_exit(snk, exit::kill); }
  );
  // 2. Hook the SINK up with the ACCOUNTANT.
  self->sync_send(node, store_atom::value, list_atom::value,
                  key::str("actors")).await(
    [&](std::map<std::string, message>& m) {
      for (auto& p : m)
        p.second.apply({
          [&](accountant::type const& a) {
            self->send(*snk, a);
          }
        });
    }
  );
  // 4. For each NODE, spawn an (auto-connected) EXPORTER.
  std::vector<actor> nodes;
  self->sync_send(node, store_atom::value, list_atom::value,
                  key::str("nodes")).await(
    [&](std::map<std::string, message> const& m) {
      for (auto& p : m)
        nodes.push_back(p.second.get_as<actor>(0));
    }
  );
  VAST_ASSERT(!nodes.empty());
  for (auto n : nodes) {
    message_builder mb;
    auto label = "exporter-" + to_string(uuid::random()).substr(0, 7);
    mb.append("spawn");
    mb.append("-l");
    mb.append(label);
    mb.append("exporter");
    mb.append("-a");
    self->send(n, mb.to_message() + export_args);
    VAST_DEBUG("created", label, "at node" << node);
  }
  // 4. Wait until the remote NODE returns the EXPORTERs so that we can
  // monitor them and connect them with our local SINK.
  auto early_finishers = 0u;
  util::flat_set<actor> exporters;
  auto failed = false;
  self->do_receive(
    [&](actor const& exporter) {
      exporters.insert(exporter);
      self->monitor(exporter);
      self->send(exporter, put_atom::value, sink_atom::value, *snk);
      VAST_DEBUG("running exporter");
      self->send(exporter, run_atom::value);
      self->send(exporter, stop_atom::value); // enter draining mode
    },
    [&](down_msg const& msg) {
      ++early_finishers;
      exporters.erase(actor_cast<actor>(msg.source));
    },
    [&](error const& e) {
      failed = true;
      VAST_ERROR("failed to spawn exporter on node"
                 << self->current_sender() << ':', e);
    },
    others >> [&] {
      failed = true;
      VAST_ERROR("got unexpected message from node"
                 << self->current_sender() << ':',
                 to_string(self->current_message()));
    }
  ).until([&] {
    return early_finishers + exporters.size() == nodes.size() || failed;
  });
  if (failed) {
    for (auto exporter : exporters)
      self->send_exit(exporter, exit::error);
    return 1;
  }
  // 5. Wait for all EXPORTERs to terminate. Thereafter we can shutdown the
  // SINK and finish.
  if (!exporters.empty()) {
    self->do_receive(
      [&](down_msg const& msg) {
        exporters.erase(actor_cast<actor>(msg.source));
        VAST_DEBUG("got DOWN from exporter" << msg.source << ',',
                   "remaining:", exporters.size());
      },
      others >> [&] {
        failed = true;
        VAST_ERROR("got unexpected message from node"
                     << self->current_sender() << ':',
                   to_string(self->current_message()));
      }
    ).until([&] { return exporters.size() == 0 || failed; });
    if (failed)
      return 1;
  }
  sink_guard.disable();
  self->monitor(*snk);
  self->send_exit(*snk, exit::done);
  auto stop = false;
  self->do_receive(
    [&](down_msg const& msg) {
      VAST_ASSERT(msg.source == *snk);
      VAST_DEBUG("received DOWN from source");
      stop = true;
    },
    [&](signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(*snk, exit::stop);
      else
        VAST_VERBOSE("ignoring signal", ::strsignal(signal));
    },
    others >> [&] {
      VAST_WARN("received unexpected message:",
                 to_string(self->current_message()));
    }
  ).until([&] { return stop; });
  if (!node->is_remote()) {
    self->monitor(node);
    self->send_exit(node, exit::stop);
    self->receive([](down_msg const&) { });
  }
  self->send_exit(sig_mon, exit::stop);
  self->await_all_other_actors_done();
  return 0;
}

int run_remote(actor const& node, message args) {
  auto failed = false;
  scoped_actor self;
  self->sync_send(node, std::move(args)).await(
    [&](ok_atom) {
      // Standard reply for success.
    },
    [&](actor const&) {
      // "vast spawn" returns an actor.
    },
    [&](std::string const& str) {
      // Status messages or query results.
      std::cout << str << std::endl;
    },
    [&](error const& e) {
      VAST_ERROR(e);
      failed = true;
    },
    others >> [&] {
      auto msg = to_string(self->current_message());
      VAST_ERROR("got unexpected reply:", msg);
      failed = true;
    }
  );
  return failed ? 1 : 0;
}

int main(int argc, char* argv[]) {
  auto start_time = time::snapshot();
  if (!detail::adjust_resource_consumption())
    return 1;
  // Locate command in command line.
  std::vector<std::string> commands = {
    "connect",
    "disconnect",
    "export",
    "import",
    "quit",
    "peer",
    "send",
    "show",
    "spawn",
    "start",
    "stop"
  };
  std::vector<std::string> command_line(argv + 1, argv + argc);
  auto cmd = std::find_first_of(command_line.begin(), command_line.end(),
                                commands.begin(), commands.end());
  // Parse options.
  auto log_level = 3;
  auto dir = "vast"s;
  auto node_endpoint_str = ""s;
  auto node_endpoint = endpoint{"", 42000};
  auto messages = std::numeric_limits<size_t>::max();
  auto profile_file = std::string{};
  auto threads = std::thread::hardware_concurrency();
  auto conf = message_builder(command_line.begin(), cmd).extract_opts({
    {"no-colors,C", "disable colors on console"},
    {"dir,d", "directory for logs and state", dir},
    {"endpoint,e", "node endpoint", node_endpoint_str},
    {"log-level,l", "verbosity of console and/or log file", log_level},
    {"messages,m", "CAF scheduler message throughput", messages},
    {"node,n", "apply command to a locally spawned node"},
    {"profile,p", "CAF scheduler profiling", profile_file},
    {"threads,t", "CAF scheduler threads", threads},
    {"version,v", "print version and exit"},
  });
  auto syntax = "vast [options] <command> [arguments]";
  if (conf.opts.count("help") > 0) {
    // TODO: Improve help.
    std::cout << banner() << "\n\n"
              << syntax << "\n\n"
              << conf.helptext << std::flush;
    return 0;
  }
  if (cmd == command_line.end()) {
    std::cerr << "invalid command: " << syntax << std::endl;
    return 1;
  }
  if (!conf.remainder.empty()) {
    auto invalid_cmd = conf.remainder.get_as<std::string>(0);
    std::cerr << "illegal command line element: " << invalid_cmd << std::endl;
    return 1;
  }
  if (! conf.error.empty()) {
    std::cerr << conf.error << std::endl;
    return 1;
  }
  // Sanitize user input.
  if (log_level < 0 || log_level > VAST_LOG_LEVEL) {
    std::cerr << "log level not in [0," << VAST_LOG_LEVEL << ']' << std::endl;
    return 1;
  }
  auto verbosity = static_cast<logger::level>(log_level);
  if (dir == ".") // Don't clobber current directory.
    dir = "vast";
  auto abs_dir = path{dir}.complete();
  if (conf.opts.count("endpoint") > 0) {
    if (!make_parser<endpoint>()(node_endpoint_str, node_endpoint)) {
      std::cerr << "invalid endpoint: " << node_endpoint_str << std::endl;
      return 1;
    }
  }
  if (threads == 0) {
    std::cerr << "CAF scheduler threads cannot be 0" << std::endl;
    return 1;
  }
  if (messages == 0) {
    std::cerr << "CAF scheduler throughput cannot be 0" << std::endl;
    return 1;
  }
  // Parse options for start already here, because we need them to determine
  // how we spawn the logger.
  auto name = util::split_to_str(util::hostname(), ".")[0];
  decltype(conf) start;
  if (*cmd == "start") {
    start = message_builder(cmd + 1, command_line.end()).extract_opts({
      {"bare,b", "spawn empty node without any actors"},
      {"foreground,f", "run daemon in foreground"},
      {"name,n", "the name of this node", name},
    });
    if (!start.error.empty()) {
      std::cerr << start.error << std::endl;
      return 1;
    }
    if (!start.remainder.empty()) {
      auto arg = start.remainder.get_as<std::string>(0);
      std::cerr << "invalid stray argument: " << arg << std::endl;
      return 1;
    }
    // No need to show anything on standard input when launching a NODE.
    util::terminal::disable_echo();
  }
  // Daemonize.
  if (*cmd == "start" && start.opts.count("foreground") == 0) {
    // On Mac OS, daemon(3) is deprecated since 10.5.
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED
    if (::daemon(0, 0) != 0) {
      return 1;
    }
    VAST_DIAGNOSTIC_POP
  } else {
    auto console = [no_colors = conf.opts.count("no-colors") > 0](auto v) {
      return no_colors ? logger::console(v) : logger::console_colorized(v);
    };
    if (!console(verbosity)) {
      std::cerr << "failed to initialize logger console backend" << std::endl;
      return 1;
    }
  }
  auto logger_guard = make_scope_guard([] { logger::destruct(); });
  auto timer_guard = make_scope_guard([start_time] {
    VAST_DEBUG("completed execution in", time::snapshot() - start_time);
  });
  auto local_node = *cmd == "start" || conf.opts.count("node") > 0;
  if (local_node) {
    // Start logger file backend.
    auto log = abs_dir / node::log_path() / "vast.log";
    if (!logger::file(verbosity, log.str())) {
      std::cerr << "failed to initialize logger file backend" << std::endl;
      return 1;
    }
    // Create symlink to current log directory.
    auto link = log.chop(-2) / "current";
    VAST_DEBUG(link.str());
    if (exists(link))
      rm(link);
    create_symlink(log.trim(-2).chop(-1), link);
  }
  // Replace/adjust CAF scheduler.
  VAST_ASSERT(threads > 0);
  VAST_ASSERT(messages > 0);
  if (conf.opts.count("profile")) {
    set_scheduler(
      new scheduler::profiled_coordinator<>{
        profile_file, std::chrono::milliseconds{1000}, threads, messages});
  } else if (conf.opts.count("threads") || conf.opts.count("messages")) {
    set_scheduler<>(threads, messages);
  }
  VAST_VERBOSE(banner() << "\n\n");
  VAST_VERBOSE("set scheduler threads to", threads);
  VAST_VERBOSE("set scheduler maximum throughput to",
               (messages == std::numeric_limits<size_t>::max()
                ? "unlimited" : std::to_string(messages)));
  // Enable direct connections.
  VAST_VERBOSE("enabling direct connection optimization");
  auto cfg = whereis(atom("ConfigServ"));
  anon_send(cfg, put_atom::value, "global.enable-automatic-connections",
                 make_message(true));
  // Announce VAST types to CAF.
  announce_types();
  auto caf_guard = make_scope_guard([] { caf::shutdown(); });
  // Get a NODE actor.
  actor node;
  if (local_node) {
    VAST_VERBOSE("spawning local node:", name);
    scoped_actor self;
    node = self->spawn(vast::node::make, name, abs_dir);
    if (start.opts.count("bare") == 0) {
      // TODO: once we have a better API for adjusting parameters of running
      // actors, we don't need to haul through the command line arguments in
      // this crude way.
      message_builder mb{cmd + 1, command_line.end()};
      auto failed = false;
      self->sync_send(node, parse_core_args(mb.to_message()).first).await(
        [](ok_atom) {},
        [&](error const& e) {
          failed = true;
          VAST_ERROR("failed to spawn node:", e);
        }
      );
      if (failed) {
        // Bring down NODE safely on error.
        if (node->exit_reason() == exit_reason::not_exited) {
          self->monitor(node);
          self->send_exit(node, exit::error);
          self->receive([&](down_msg const&) {});
        }
        return 1;
      }
    }
  } else {
    try {
      auto host = node_endpoint.host;
      if (host.empty())
        host = "127.0.0.1";
      VAST_VERBOSE("connecting to", host << ':' << node_endpoint.port);
      node = caf::io::remote_actor(host, node_endpoint.port);
    } catch (caf::network_error const& e) {
      VAST_ERROR(e.what());
      return 1;
    }
  }
  // Process commands.
  if (*cmd == "start") {
    return run_start(node, name, node_endpoint);
  } else if (*cmd == "import") {
    message_builder mb;
    auto i = cmd + 1;
    while (i != command_line.end())
      mb.append(*i++);
    return run_import(node, parse_core_args(mb.to_message()).second);
  } else if (*cmd == "export") {
    if (cmd + 1 == command_line.end()) {
      VAST_ERROR("missing sink format");
      return 1;
    } else if (cmd + 2 == command_line.end()) {
      VAST_ERROR("missing query arguments");
      return 1;
    } else {
      auto i = cmd + 2;
      message_builder mb;
      mb.append(*i++);
      while (i != command_line.end())
        mb.append(*i++);
      return run_export(node, make_message(*(cmd + 1)),
                        parse_core_args(mb.to_message()).second);
    }
  } else {
    auto args = std::vector<std::string>(cmd + 1, command_line.end());
    auto cmd_line = *cmd + util::join(args, " ");
    message_builder mb;
    mb.append(*cmd);
    for (auto& a : args)
      mb.append(std::move(a));
    VAST_DEBUG("sending command:", to_string(mb.to_message()));
    return run_remote(node, mb.to_message());
  }
}
