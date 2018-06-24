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
#include "vast/logger.hpp"

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

using std::string;

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace caf;

namespace vast::system {

application::root_command::root_command() {
  add_opt<string>("dir,d", "directory for persistent state");
  add_opt<string>("endpoint,e", "node endpoint");
  add_opt<string>("id,i", "the unique ID of this node");
  add_opt<bool>("node,n", "spawn a node instead of connecting to one");
  add_opt<bool>("version,v", "print version and exit");
}

command::proceed_result
application::root_command::proceed(caf::actor_system& sys,
                                   const caf::config_value_map& options,
                                   argument_iterator begin,
                                   argument_iterator end) {
  VAST_UNUSED(begin, end);
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  CAF_IGNORE_UNUSED(sys);
  CAF_IGNORE_UNUSED(options);
  if (get_or(options, "version", false)) {
    std::cout << VAST_VERSION << std::endl;
    return stop_successful;
  }
  std::cout << banner() << "\n\n";
  return command::proceed_ok;
}

application::application() {
  // TODO: this function has side effects...should we put it elsewhere where
  // it's explicit to the user? Or perhaps make whatever this function does
  // simply a configuration option and use it later?
  detail::adjust_resource_consumption();
}

int application::run(caf::actor_system& sys, command::argument_iterator begin,
                     command::argument_iterator end) {
  VAST_TRACE(VAST_ARG("args", begin, end));
  return root_.run(sys, begin, end);
}

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
//    return start_command(self, node);
//  } else if (*cmd == "import" || *cmd == "export") {
//    auto args = message_builder{cmd + 1, config_.command_line.end()}.to_message();
//    auto run = *cmd == "import" ? import_command : export_command;
//    auto rc = run(self, node, args);
//    if (spawn_node)
//      self->send_exit(node, exit_reason::user_shutdown);
//    return rc;
//  }
//  // Send command to remote node.
//  auto args = message_builder{cmd + 1, config_.command_line.end()}.to_message();
//  VAST_DEBUG("sending command to remote node:", *cmd, to_string(args));
//  return remote_command(self, node, *cmd, args);
//}

} // namespace vast::system
