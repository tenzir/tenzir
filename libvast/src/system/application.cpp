/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/config.hpp"

#include <iostream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/banner.hpp"

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/data.hpp"

#include "vast/system/application.hpp"

#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"


//#ifdef VAST_LINUX
//#include <unistd.h> // daemon(3)
//#endif
//
//#include <csignal>
//#include <cstdlib>
//#include <cstring> // strsignal(3)
//
//#include <chrono>
//
//#include "vast/error.hpp"
//#include "vast/filesystem.hpp"
//#include "vast/logger.hpp"
//#include "vast/concept/parseable/vast/endpoint.hpp"
//#include "vast/concept/printable/stream.hpp"
//#include "vast/concept/printable/std/chrono.hpp"
//#include "vast/concept/printable/vast/filesystem.hpp"
//
//#include "vast/system/atoms.hpp"
//#include "vast/system/archive.hpp"
//#include "vast/system/configuration.hpp"
//#include "vast/system/node.hpp"
//#include "vast/system/signal_monitor.hpp"
//#include "vast/system/spawn.hpp"

//using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace caf;

namespace vast::system {

namespace {

/*

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
    [&](const std::string& id, system::registry& reg) {
      auto er = reg.components[id].equal_range("importer");
      if (er.first == er.second) {
        VAST_ERROR("no importers available at node", id);
        stop = true;
      } else {
        VAST_DEBUG("connecting source to importers");
        for (auto i = er.first; i != er.second; ++i)
          self->send(*src, system::sink_atom::value, i->second.actor);
      }
    },
    [&](const error& e) {
      VAST_IGNORE_UNUSED(e);
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
      VAST_IGNORE_UNUSED(e);
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
      VAST_IGNORE_UNUSED(e);
      VAST_ERROR(self->system().render(e));
      result = false;
    }
  );
  return result ? 0 : 1;
}

*/


} // namespace <anonymous>


application::application(const configuration& cfg) : config_{cfg} {
  // TODO: this function has side effects...should we put it elsewhere where
  // it's explicit to the user? Or perhaps make whatever this function does
  // simply a configuration option and use it later?
  detail::adjust_resource_consumption();
  // Define program layout.
  //program_
  //  .opt("dir,d", "directory for persistent state", "vast")
  //  .opt("endpoint,e", "node endpoint", ":42000")
  //  .opt("id,i", "the unique ID of this node",
  //       detail::split_to_str(detail::hostname(), ".")[0])
  //  .opt("node,n", "spawn a local node instead of connecting to one")
  //  .opt("version,v", "print version and exit");
  //program_
  //  .cmd("start", "start a node")
  //    .opt("bare,b", "spawn empty node without any components")
  //    .opt("foreground,f", "run in foreground (do not daemonize)");
  //program_.cmd("stop", "stop a node");
  //program_.cmd("show");
  //program_.cmd("export");
  //program_.cmd("import");
  //program_.cmd("spawn");
  //program_.cmd("send");
  //program_.cmd("kill");
  //program_.cmd("peer");
}

int application::run(caf::actor_system& sys) {
//  // TODO: replace this manual parsing by a proper interface in the program
//  // layout.
//  auto pred = [](auto& x, auto& cmd) { return x == cmd.first; };
//  auto cmd = std::find_first_of(config_.command_line.begin(),
//                                config_.command_line.end(),
//                                program_.commands.begin(),
//                                program_.commands.end(),
//                                pred);
//  // Parse top-level options that apply only up to the command.
//  auto builder = message_builder(config_.command_line.begin(), cmd);
//  auto conf = builder.extract_opts(make_cli_args(*program_));
//  auto syntax = "vast [options] <command> [arguments]";
//  if (conf.opts.count("help") > 0) {
//    std::cout << banner() << "\n\n"
//              << syntax << "\n\n"
//              << conf.helptext << std::flush;
//    return 0;
//  }
//  if (!conf.remainder.empty()) {
//    auto invalid_cmd = conf.remainder.get_as<std::string>(0);
//    std::cerr << "illegal command line element: " << invalid_cmd << std::endl;
//    return 1;
//  }
//  if (!conf.error.empty()) {
//    std::cerr << conf.error << std::endl;
//    return 1;
//  }
//  if (cmd == config_.command_line.end()) {
//    std::cerr << "invalid command: " << syntax << std::endl;
//    return 1;
//  }
//  std::cout << program_.options["dir"].value << std::endl;
//  std::cout << *program_.commands[*cmd]->get("bare") << std::endl;
//  std::cout << program_.commands[*cmd]->options["foreground"].value << std::endl;
//  return 0;
//  // Don't clobber current directory.
//  if (dir == ".")
//    dir = "vast";
//  auto abs_dir = path{dir}.complete();
//  if (conf.opts.count("endpoint") > 0) {
//    if (!parsers::endpoint(node_endpoint_str, node_endpoint)) {
//      std::cerr << "invalid endpoint: " << node_endpoint_str << std::endl;
//      return 1;
//    }
//  }
//  if (conf.opts.count("version") > 0) {
//    std::cout << VAST_VERSION << std::endl;
//    return 0;
//  }
//  // Parse options for the "start" command already here, because they determine
//  // how to configure the logger.
//  decltype(conf) start;
//  if (*cmd == "start") {
//    start = message_builder(cmd + 1, config_.command_line.end()).extract_opts({
//      {"bare,b", "spawn empty node without any components"},
//      {"foreground,f", "run in foreground (do not daemonize)"},
//    });
//    if (!start.error.empty()) {
//      std::cerr << start.error << std::endl;
//      return 1;
//    }
//    if (!start.remainder.empty()) {
//      auto arg = start.remainder.get_as<std::string>(0);
//      std::cerr << "stray argument: " << arg << std::endl;
//      return 1;
//    }
//    // No need to echo characters typed when launching a node.
//    detail::terminal::disable_echo();
//  }
//  // Daemonize or configure console logger.
//  if (*cmd == "start" && start.opts.count("foreground") == 0) {
//    // Go into daemon mode if we're not supposed to run in the foreground.
//    VAST_DIAGNOSTIC_PUSH
//    VAST_DIAGNOSTIC_IGNORE_DEPRECATED // macOS deprecated daemon(3) since 10.5.
//    if (::daemon(0, 0) != 0) {
//      std::cerr << "failed to daemonize process" << std::endl;
//      return 1;
//    }
//    VAST_DIAGNOSTIC_POP
//  } else {
//    // Only enable color when we're the console in foreground only.
//    sys_cfg.logger_console = atom("COLORED");
//  }
//  // We spawn a node either for the "start" command or when -n is given.
//  auto spawn_node = *cmd == "start" || conf.opts.count("node") > 0;
//  // Setup log file.
//  if (!spawn_node) {
//    sys_cfg.logger_file_name.clear();
//  } else {
//    auto secs = duration_cast<seconds>(system_clock::now().time_since_epoch());
//    auto pid = detail::process_id();
//    auto current = std::to_string(secs.count()) + '_' + std::to_string(pid);
//    auto log_path = abs_dir / "log" / current / "vast.log";
//    sys_cfg.logger_file_name = log_path.str();
//    if (!exists(log_path.parent())) {
//      auto result = mkdir(log_path.parent());
//      if (!result) {
//        std::cerr << "failed to create log directory: "
//                  << log_path.parent() << std::endl;
//        return 1;
//      }
//    }
//    // Create symlink to current log directory.
//    auto link = log_path.chop(-2) / "current";
//    if (exists(link))
//      rm(link);
//    create_symlink(log_path.trim(-2).chop(-1), link);
//  }
//  // Start or connect to a node.
//  auto use_encryption = !sys_cfg.openssl_certificate.empty()
//                        || !sys_cfg.openssl_key.empty()
//                        || !sys_cfg.openssl_passphrase.empty()
//                        || !sys_cfg.openssl_capath.empty()
//                        || !sys_cfg.openssl_cafile.empty();
//  VAST_INFO(banner() << '\n');
//  scoped_actor self{sys};
//  actor node;
//  if (spawn_node) {
//    VAST_INFO("spawning local node:", id);
//    node = self->spawn(system::node, id, abs_dir);
//    if (start.opts.count("bare") == 0) {
//      // If we're not in bare mode, we spawn all core actors.
//      auto spawn_component = [&](auto&&... xs) {
//        return [&] {
//          auto result = error{};
//          auto args = make_message(std::move(xs)...);
//          self->request(node, infinite, "spawn", std::move(args)).receive(
//            [](const actor&) { /* nop */ },
//            [&](error& e) { result = std::move(e); }
//          );
//          return result;
//        };
//      };
//      auto err = error::eval(
//        spawn_component("metastore"),
//        spawn_component("archive"),
//        spawn_component("index"),
//        spawn_component("importer")
//      );
//      if (err) {
//        VAST_ERROR(self->system().render(err));
//        self->send_exit(node, exit_reason::user_shutdown);
//        return 1;
//      }
//    }
//  } else {
//    auto host = node_endpoint.host;
//    if (node_endpoint.host.empty())
//      node_endpoint.host = "127.0.0.1";
//    VAST_INFO("connecting to", node_endpoint.host << ':' << node_endpoint.port);
//    auto remote_actor = [&]() -> expected<actor> {
//      if (use_encryption)
//#ifdef VAST_USE_OPENSSL
//        return openssl::remote_actor(sys, node_endpoint.host,
//                                     node_endpoint.port);
//#else
//        return make_error(ec::unspecified, "not compiled with OpenSSL support");
//#endif
//      auto& mm = sys.middleman();
//      return mm.remote_actor(node_endpoint.host, node_endpoint.port);
//    };
//    auto a = remote_actor();
//    if (!a) {
//      VAST_ERROR("failed to connect:", sys.render(a.error()));
//      return 1;
//    }
//    node = std::move(*a);
//  }
//  self->monitor(node);
//  // Now that we have a node handle, process the commands. If the command
//  // pertains to a local node, we dispatch in this process.
//  if (*cmd == "start") {
//    // Publish the node.
//    auto host =
//      node_endpoint.host.empty() ? nullptr : node_endpoint.host.c_str();
//    auto publish = [&]() -> expected<uint16_t> {
//      if (use_encryption)
//#ifdef VAST_USE_OPENSSL
//        return openssl::publish(node, node_endpoint.port, host);
//#else
//        return make_error(ec::unspecified, "not compiled with OpenSSL support");
//#endif
//      auto& mm = self->system().middleman();
//      return mm.publish(node, node_endpoint.port, host);
//    };
//    auto bound_port = publish();
//    if (!bound_port) {
//      VAST_ERROR(self->system().render(bound_port.error()));
//      self->send_exit(node, exit_reason::user_shutdown);
//      return 1;
//    }
//    VAST_INFO("listening on", (host ? host : "") << ':' << *bound_port);
//    return run_start(self, node);
//  } else if (*cmd == "import" || *cmd == "export") {
//    auto args = message_builder{cmd + 1, config_.command_line.end()}.to_message();
//    auto run = *cmd == "import" ? run_import : run_export;
//    auto rc = run(self, node, args);
//    if (spawn_node)
//      self->send_exit(node, exit_reason::user_shutdown);
//    return rc;
//  }
//  // Send command to remote node.
//  auto args = message_builder{cmd + 1, config_.command_line.end()}.to_message();
//  VAST_DEBUG("sending command to remote node:", *cmd, to_string(args));
//  return run_remote(self, node, *cmd, args);
}

} // namespace vast::system
