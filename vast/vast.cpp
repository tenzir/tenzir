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
#include "vast/system/archive.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/node.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn.hpp"

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

int run_import(scoped_actor& self, actor& node, message args) {
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms, self);
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
  auto rc = 1;
  auto stop = false;
  // Spawn a source.
  auto opts = system::options{args, {}, {}};
  auto src = system::spawn_source(actor_cast<local_actor*>(self), opts);
  if (!src) {
    VAST_ERROR("failed to spawn source:", self->system().render(src.error()));
    return rc;
  }
  // Connect source to importers.
  self->request(node, infinite, get_atom::value).receive(
    [&](const std::string& name, system::registry& reg) {
      auto er = reg[name].equal_range("importer");
      if (er.first == er.second) {
        VAST_ERROR("no importers available at node", name);
        stop = true;
      } else {
        VAST_DEBUG("connecting source to importers");
        for (auto i = er.first; i != er.second; ++i)
          self->send(*src, system::sink_atom::value, i->second.actor);
      }
    },
    [&](const error& e) {
      VAST_ERROR(self->system().render(e));
      stop = true;
    }
  );
  if (stop)
    return rc;
  // Start the source.
  rc = 0;
  self->send(*src, system::run_atom::value);
  self->monitor(*src);
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG("received DOWN from node");
        self->send_exit(*src, exit_reason::user_shutdown);
        rc = 1;
      } else if (msg.source == *src) {
        VAST_DEBUG("received DOWN from source");
      }
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(*src, exit_reason::user_shutdown);
    }
  ).until([&] { return stop; });
  return rc;
}

int run_export(scoped_actor& self, actor& node, message args) {
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms, self);
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
  // Spawn a sink.
  auto opts = system::options{args, {}, {}};
  VAST_DEBUG("spawning sink with parameters:", deep_to_string(opts.params));
  auto snk = system::spawn_sink(actor_cast<local_actor*>(self), opts);
  if (!snk) {
    VAST_ERROR("failed to spawn sink:", self->system().render(snk.error()));
    return 1;
  }
  // Spawn exporter at the node.
  actor exp;
  args = make_message("exporter") + opts.params;
  VAST_DEBUG("spawning exporter with parameters:", to_string(args));
  self->request(node, infinite, "spawn", args).receive(
    [&](const actor& a) { 
      exp = a;
    },
    [&](const error& e) {
      VAST_ERROR("failed to spawn exporter:", self->system().render(e));
    }
  );
  if (!exp) {
    self->send_exit(*snk, exit_reason::user_shutdown);
    return 1;
  }
  // Start the exporter.
  self->send(exp, system::sink_atom::value, *snk);
  self->send(exp, system::run_atom::value);
  self->monitor(*snk);
  self->monitor(exp);
  auto rc = 0;
  auto stop = false;
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG("received DOWN from node");
        self->send_exit(*snk, exit_reason::user_shutdown);
        self->send_exit(exp, exit_reason::user_shutdown);
        rc = 1;
      } else if (msg.source == exp) {
        VAST_DEBUG("received DOWN from exporter");
        self->send_exit(*snk, exit_reason::user_shutdown);
      } else if (msg.source == *snk) {
        VAST_DEBUG("received DOWN from sink");
        self->send_exit(exp, exit_reason::user_shutdown);
        rc = 1;
      } else {
        VAST_ASSERT(!"received DOWN from inexplicable actor");
      }
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM) {
        self->send_exit(exp, exit_reason::user_shutdown);
        self->send_exit(*snk, exit_reason::user_shutdown);
      }
    }
  ).until([&] { return stop; });
  return rc;
}

int run_start(scoped_actor& self, actor& node) {
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms, self);
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
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
  auto id = system::raft::server_id{0};
  auto name = vast::detail::split_to_str(vast::detail::hostname(), ".")[0];
  auto conf = message_builder(cmd_line.begin(), cmd).extract_opts({
    {"dir,d", "directory for persistent state", dir},
    {"endpoint,e", "node endpoint", node_endpoint_str},
    {"id,i", "the consensus module ID of this node", id},
    {"local,l", "apply command to a locally spawned node"},
    {"name,n", "the name of this node", name},
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
  decltype(conf) start;
  if (*cmd == "start") {
    start = message_builder(cmd + 1, cmd_line.end()).extract_opts({
      {"bare,b", "spawn empty node without any components"},
      {"foreground,f", "run in foreground (do not daemonize)"},
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
  // We spawn a node either for the "start" command or when -n is given.
  auto spawn_node = *cmd == "start" || conf.opts.count("local") > 0;
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
    node = self->spawn(system::node, name, abs_dir, id);
    if (start.opts.count("bare") == 0) {
      // If we're not in bare mode, we spawn all core actors.
      auto spawn_component = [&](auto&&... xs) {
        return [&] {
          auto result = error{};
          auto args = make_message(std::move(xs)...);
          self->request(node, infinite, "spawn", std::move(args)).receive(
            [](const actor&) { /* nop */ },
            [&](error& e) { result = std::move(e); }
          );
          return result;
        };
      };
      auto err = error::eval(
        spawn_component("metastore"),
        spawn_component("archive"),
        spawn_component("index"),
        spawn_component("importer")
      );
      if (err) {
        VAST_ERROR(self->system().render(err));
        self->send_exit(node, exit_reason::user_shutdown);
        return 1;
      }
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
  } else if (*cmd == "import" || *cmd == "export") {
    auto args = message_builder{cmd + 1, cmd_line.end()}.to_message();
    auto run = *cmd == "import" ? run_import : run_export;
    auto rc = run(self, node, args);
    if (spawn_node)
      self->send_exit(node, exit_reason::user_shutdown);
    return rc;
  }
  // Send command to remote node.
  auto args = message_builder{cmd + 1, cmd_line.end()}.to_message();
  VAST_DEBUG("sending command to remote node:", *cmd, to_string(args));
  return run_remote(self, node, *cmd, args);
}
