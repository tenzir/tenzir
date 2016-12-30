#include "vast/config.hpp"

#ifdef VAST_LINUX
#include <unistd.h> // daemon(3)
#endif

#include <csignal>
#include <cstdlib>
#include <cstring> // strsignal(3)

#include <chrono>
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/banner.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/node.hpp"
#include "vast/system/signal_monitor.hpp"

#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/detail/terminal.hpp"

using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace caf;
using namespace vast;

namespace {

// TODO
//int run_import(actor const& node, message args) {
//  scoped_actor self;
//  auto sig_mon = self->spawn<linked>(signal_monitor::make, self);
//  // 1. Spawn a SOURCE.
//  auto src = source::spawn(std::move(args));
//  if (!src) {
//    VAST_ERROR("failed to spawn source:", src.error());
//    return 1;
//  }
//  auto source_guard = make_scope_guard(
//    [=] { anon_send_exit(*src, exit::kill); }
//  );
//  // 2. Hook the SOURCE up with the ACCOUNTANT.
//  self->sync_send(node, store_atom::value, list_atom::value,
//                  key::str("actors")).await(
//    [&](std::map<std::string, message>& m) {
//      for (auto& p : m)
//        p.second.apply({
//          [&](accountant::type const& a) {
//            self->send(*src, a);
//          }
//        });
//    }
//  );
//  // 3. Find all IMPORTERs to load-balance across them.
//  std::vector<actor> importers;
//  self->sync_send(node, store_atom::value, list_atom::value,
//                  key::str("actors")).await(
//    [&](std::map<std::string, message>& m) {
//      for (auto& p : m)
//        p.second.apply({
//          [&](actor const& a, std::string const& type) {
//            VAST_ASSERT(a != invalid_actor);
//            if (type == "importer")
//              importers.push_back(a);
//          }
//        });
//    }
//  );
//  if (importers.empty()) {
//    VAST_ERROR("no importers found");
//    return 1;
//  }
//  // 4. Connect SOURCE and IMPORTERs.
//  for (auto& imp : importers) {
//    VAST_ASSERT(imp != invalid_actor);
//    VAST_DEBUG("connecting source with importer", imp);
//    self->send(*src, put_atom::value, sink_atom::value, imp);
//  }
//  // 5. Run the SOURCE.
//  source_guard.disable();
//  self->send(*src, run_atom::value);
//  self->monitor(*src);
//  auto stop = false;
//  self->do_receive(
//    [&](down_msg const& msg) {
//      VAST_ASSERT(msg.source == *src);
//      VAST_DEBUG("received DOWN from source");
//      stop = true;
//    },
//    [&](signal_atom, int signal) {
//      VAST_DEBUG("got " << ::strsignal(signal));
//      if (signal == SIGINT || signal == SIGTERM)
//        self->send_exit(*src, exit::stop);
//      else
//        VAST_INFO("ignoring signal", ::strsignal(signal));
//    },
//    others >> [&] {
//      VAST_WARN("received unexpected message:",
//                 to_string(self->current_message()));
//    }
//  ).until([&] { return stop; });
//  if (!node->is_remote()) {
//    self->monitor(node);
//    self->send_exit(node, exit::stop);
//    self->receive([](down_msg const&) { });
//  }
//  self->send_exit(sig_mon, exit::stop);
//  self->await_all_other_actors_done();
//  return 0;
//}

//int run_export(actor const& node, message sink_args, message export_args) {
//  scoped_actor self;
//  auto sig_mon = self->spawn<linked>(signal_monitor::make, self);
//  // 1. Spawn a SINK.
//  auto snk = sink::spawn(std::move(sink_args));
//  if (!snk) {
//    VAST_ERROR("failed to spawn sink:", snk.error());
//    return 1;
//  }
//  auto sink_guard = make_scope_guard(
//    [snk = *snk] { anon_send_exit(snk, exit::kill); }
//  );
//  // 2. Hook the SINK up with the ACCOUNTANT.
//  self->sync_send(node, store_atom::value, list_atom::value,
//                  key::str("actors")).await(
//    [&](std::map<std::string, message>& m) {
//      for (auto& p : m)
//        p.second.apply({
//          [&](accountant::type const& a) {
//            self->send(*snk, a);
//          }
//        });
//    }
//  );
//  // 4. For each node, spawn an (auto-connected) EXPORTER.
//  std::vector<actor> nodes;
//  self->sync_send(node, store_atom::value, list_atom::value,
//                  key::str("nodes")).await(
//    [&](std::map<std::string, message> const& m) {
//      for (auto& p : m)
//        nodes.push_back(p.second.get_as<actor>(0));
//    }
//  );
//  VAST_ASSERT(!nodes.empty());
//  for (auto n : nodes) {
//    message_builder mb;
//    auto label = "exporter-" + to_string(uuid::random()).substr(0, 7);
//    mb.append("spawn");
//    mb.append("-l");
//    mb.append(label);
//    mb.append("exporter");
//    mb.append("-a");
//    self->send(n, mb.to_message() + export_args);
//    VAST_DEBUG("created", label, "at node" << node);
//  }
//  // 4. Wait until the remote node returns the EXPORTERs so that we can
//  // monitor them and connect them with our local SINK.
//  auto early_finishers = 0u;
//  util::flat_set<actor> exporters;
//  auto failed = false;
//  self->do_receive(
//    [&](actor const& exporter) {
//      exporters.insert(exporter);
//      self->monitor(exporter);
//      self->send(exporter, put_atom::value, sink_atom::value, *snk);
//      VAST_DEBUG("running exporter");
//      self->send(exporter, run_atom::value);
//      self->send(exporter, stop_atom::value); // enter draining mode
//    },
//    [&](down_msg const& msg) {
//      ++early_finishers;
//      exporters.erase(actor_cast<actor>(msg.source));
//    },
//    [&](error const& e) {
//      failed = true;
//      VAST_ERROR("failed to spawn exporter on node"
//                 << self->current_sender() << ':', e);
//    },
//    others >> [&] {
//      failed = true;
//      VAST_ERROR("got unexpected message from node"
//                 << self->current_sender() << ':',
//                 to_string(self->current_message()));
//    }
//  ).until([&] {
//    return early_finishers + exporters.size() == nodes.size() || failed;
//  });
//  if (failed) {
//    for (auto exporter : exporters)
//      self->send_exit(exporter, exit::error);
//    return 1;
//  }
//  // 5. Wait for all EXPORTERs to terminate. Thereafter we can shutdown the
//  // SINK and finish.
//  if (!exporters.empty()) {
//    self->do_receive(
//      [&](down_msg const& msg) {
//        exporters.erase(actor_cast<actor>(msg.source));
//        VAST_DEBUG("got DOWN from exporter" << msg.source << ',',
//                   "remaining:", exporters.size());
//      },
//      others >> [&] {
//        failed = true;
//        VAST_ERROR("got unexpected message from node"
//                     << self->current_sender() << ':',
//                   to_string(self->current_message()));
//      }
//    ).until([&] { return exporters.size() == 0 || failed; });
//    if (failed)
//      return 1;
//  }
//  sink_guard.disable();
//  self->monitor(*snk);
//  self->send_exit(*snk, exit::done);
//  auto stop = false;
//  self->do_receive(
//    [&](down_msg const& msg) {
//      VAST_ASSERT(msg.source == *snk);
//      VAST_DEBUG("received DOWN from source");
//      stop = true;
//    },
//    [&](signal_atom, int signal) {
//      VAST_DEBUG("got " << ::strsignal(signal));
//      if (signal == SIGINT || signal == SIGTERM)
//        self->send_exit(*snk, exit::stop);
//      else
//        VAST_INFO("ignoring signal", ::strsignal(signal));
//    },
//    others >> [&] {
//      VAST_WARN("received unexpected message:",
//                 to_string(self->current_message()));
//    }
//  ).until([&] { return stop; });
//  if (!node->is_remote()) {
//    self->monitor(node);
//    self->send_exit(node, exit::stop);
//    self->receive([](down_msg const&) { });
//  }
//  self->send_exit(sig_mon, exit::stop);
//  self->await_all_other_actors_done();
//  return 0;
//}

int run_start(scoped_actor& self, actor& node) {
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms, self);
  self->link_to(sig_mon);
  auto rc = 0;
  auto stop = false;
  self->do_receive(
    [&](const down_msg& msg) {
      VAST_ASSERT(msg.source == node);
      VAST_DEBUG("received DOWN from node");
      stop = true;
      if (msg.reason != exit_reason::user_shutdown)
        rc = 1;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(node, exit_reason::user_shutdown);
      else
        self->send(node, system::signal_atom::value, signal);
    }
  ).until([&] { return stop; });
  self->send_exit(sig_mon, exit_reason::user_shutdown);
  return rc;
}

int run_remote(scoped_actor& self, actor& node, std::string cmd, message args) {
  auto result = true;
  self->send(node, std::move(cmd), std::move(args));
  self->receive(
    [&](const down_msg& msg) {
      if (msg.reason != exit_reason::user_shutdown)
        result = false;
    },
    [&](ok_atom) {
      // Standard reply for success.
    },
    [&](actor&) {
      // "vast spawn" returns an actor.
    },
    [&](const std::string& str) {
      // Status messages or query results.
      std::cout << str << std::endl;
    },
    [&](const error& e) {
      VAST_ERROR(self->system().render(e));
      result = false;
    }
  );
  return result ? 0 : 1;
}

} // namespace <anonymous>

int main(int argc, char* argv[]) {
  if (!vast::detail::adjust_resource_consumption())
    return 1;
  // Locate command in command line.
  std::vector<std::string> commands = {
    "export",
    "import",
    "kill",
    "peer",
    "send",
    "show",
    "spawn",
    "start",
    "stop"
  };
  std::vector<std::string> cmd_line(argv + 1, argv + argc);
  // Move CAF-specific options into separate option sequence.
  auto is_not_caf_opt = [](auto& x) {
    return !vast::detail::starts_with(x, "--caf#");
  };
  auto caf_opt = std::stable_partition(cmd_line.begin(), cmd_line.end(),
                                       is_not_caf_opt);
  std::vector<std::string> caf_opts;
  std::move(caf_opt, cmd_line.end(), std::back_inserter(caf_opts));
  cmd_line.erase(caf_opt, cmd_line.end());
  // Locate VAST command.
  auto cmd = std::find_first_of(cmd_line.begin(), cmd_line.end(),
                                commands.begin(), commands.end());
  // Parse top-level options that apply only up to the command.
  auto dir = "vast"s;
  auto node_endpoint_str = ""s;
  auto node_endpoint = endpoint{"", 42000};
  auto conf = message_builder(cmd_line.begin(), cmd).extract_opts({
    {"dir,d", "directory for persistent state", dir},
    {"endpoint,e", "node endpoint", node_endpoint_str},
    {"node,n", "apply command to a locally spawned node"},
    {"version,v", "print version and exit"},
  });
  auto syntax = "vast [options] <command> [arguments]";
  if (conf.opts.count("help") > 0) {
    std::cout << banner() << "\n\n"
              << syntax << "\n\n"
              << conf.helptext << std::flush;
    return 0;
  }
  if (cmd == cmd_line.end()) {
    std::cerr << "invalid command: " << syntax << std::endl;
    return 1;
  }
  if (!conf.remainder.empty()) {
    auto invalid_cmd = conf.remainder.get_as<std::string>(0);
    std::cerr << "illegal command line element: " << invalid_cmd << std::endl;
    return 1;
  }
  if (!conf.error.empty()) {
    std::cerr << conf.error << std::endl;
    return 1;
  }
  // Don't clobber current directory.
  if (dir == ".")
    dir = "vast";
  auto abs_dir = path{dir}.complete();
  if (conf.opts.count("endpoint") > 0) {
    if (!parsers::endpoint(node_endpoint_str, node_endpoint)) {
      std::cerr << "invalid endpoint: " << node_endpoint_str << std::endl;
      return 1;
    }
  }
  if (conf.opts.count("version") > 0) {
    std::cout << VAST_VERSION << std::endl;
    return 0;
  }
  // Start assembling the actor system configuration.
  system::configuration sys_cfg{caf_opts};
  sys_cfg.middleman_enable_automatic_connections = true;
  // Parse options for the "start" command already here, because they determine
  // how to configure the logger.
  auto name = vast::detail::split_to_str(vast::detail::hostname(), ".")[0];
  decltype(conf) start;
  if (*cmd == "start") {
    start = message_builder(cmd + 1, cmd_line.end()).extract_opts({
      {"bare,b", "spawn empty node without any components"},
      {"foreground,f", "run in foreground (do not daemonize)"},
      {"name,n", "the name of this node", name},
    });
    if (!start.error.empty()) {
      std::cerr << start.error << std::endl;
      return 1;
    }
    if (!start.remainder.empty()) {
      auto arg = start.remainder.get_as<std::string>(0);
      std::cerr << "stray argument: " << arg << std::endl;
      return 1;
    }
    // No need to echo characters typed when launching a node.
    vast::detail::terminal::disable_echo();
  }
  // Daemonize or configure console logger.
  if (*cmd == "start" && start.opts.count("foreground") == 0) {
    // Go into daemon mode if we're not supposed to run in the foreground.
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED // macOS deprecated daemon(3) since 10.5.
    if (::daemon(0, 0) != 0) {
      std::cerr << "failed to daemonize process" << std::endl;
      return 1;
    }
    VAST_DIAGNOSTIC_POP
  } else {
    // Enable the console in foreground only.
    sys_cfg.logger_console = atom("COLORED");
    // Only override if not previously specified via --caf#logger.filter.
    if (sys_cfg.logger_filter.empty())
      sys_cfg.logger_filter = "vast";
    // TODO: teach CAF how to apply the filter to the console only.
  }
  // We spawn a node
  auto spawn_node = *cmd == "start" || conf.opts.count("node") > 0;
  // Setup log file.
  if (!spawn_node) {
    sys_cfg.logger_filename.clear();
  } else {
    auto secs = duration_cast<seconds>(system_clock::now().time_since_epoch());
    auto pid = vast::detail::process_id();
    auto current = std::to_string(secs.count()) + '_' + std::to_string(pid);
    auto log_path = abs_dir / "log" / current / "vast.log";
    sys_cfg.logger_filename = log_path.str();
    if (!exists(log_path.parent())) {
      auto result = mkdir(log_path.parent());
      if (!result) {
        std::cerr << "failed to create log directory: "
                  << log_path.parent() << std::endl;
        return 1;
      }
    }
    // Create symlink to current log directory.
    auto link = log_path.chop(-2) / "current";
    if (exists(link))
      rm(link);
    create_symlink(log_path.trim(-2).chop(-1), link);
  }
  // Start or connect to a node.
  actor_system sys{sys_cfg};
  VAST_INFO(banner() << '\n');
  scoped_actor self{sys};
  actor node;
  if (spawn_node) {
    VAST_INFO("spawning local node:", name);
    node = self->spawn(system::node, name, abs_dir);
    if (start.opts.count("bare") == 0) {
      // If we're not in bare mode, we spawn all core actors.
      // TODO
    }
  } else {
    auto host = node_endpoint.host;
    if (node_endpoint.host.empty())
      node_endpoint.host = "127.0.0.1";
    VAST_INFO("connecting to", node_endpoint.host << ':' << node_endpoint.port);
    auto& mm = sys.middleman();
    auto a = mm.remote_actor(node_endpoint.host, node_endpoint.port);
    if (!a) {
      VAST_ERROR("failed to connect:", sys.render(a.error()));
      return 1;
    }
    node = std::move(*a);
  }
  self->monitor(node);
  // Now that we have a node handle, process the commands. If the command
  // pertains to a local node, we dispatch in this process.
  if (*cmd == "start") {
    // Publish the node.
    auto host =
      node_endpoint.host.empty() ? nullptr : node_endpoint.host.c_str();
    auto& mm = self->system().middleman();
    auto bound_port = mm.publish(node, node_endpoint.port, host);
    if (!bound_port) {
      VAST_ERROR(self->system().render(bound_port.error()));
      self->send_exit(node, exit_reason::user_shutdown);
      return 1;
    }
    VAST_INFO("listening on", (host ? host : "") << ':' << *bound_port);
    return run_start(self, node);
  // TODO
  //} else if (*cmd == "import") {
  //  message_builder mb;
  //  auto i = cmd + 1;
  //  while (i != cmd_line.end())
  //    mb.append(*i++);
  //  return run_import(node, parse_core_args(mb.to_message()).second);
  //} else if (*cmd == "export") {
  //  if (cmd + 1 == cmd_line.end()) {
  //    VAST_ERROR("missing sink format");
  //    return 1;
  //  } else if (cmd + 2 == cmd_line.end()) {
  //    VAST_ERROR("missing query arguments");
  //    return 1;
  //  } else {
  //    auto i = cmd + 2;
  //    message_builder mb;
  //    mb.append(*i++);
  //    while (i != cmd_line.end())
  //      mb.append(*i++);
  //    return run_export(node, make_message(*(cmd + 1)),
  //                      parse_core_args(mb.to_message()).second);
  //  }
  }
  // Send command to remote node.
  auto args = message_builder{cmd + 1, cmd_line.end()}.to_message();
  VAST_DEBUG("sending command to remote node:", *cmd, to_string(args));
  return run_remote(self, node, *cmd, args);
}
