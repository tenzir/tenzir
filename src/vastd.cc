#include "vast/config.h"

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

#include <caf/io/all.hpp>

#include "vast/announce.h"
#include "vast/banner.h"
#include "vast/caf.h"
#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/optional.h"
#include "vast/actor/node.h"
#include "vast/actor/signal_monitor.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/detail/adjust_resource_consumption.h"
#include "vast/util/endpoint.h"
#include "vast/util/string.h"
#include "vast/util/system.h"

using namespace vast;
using namespace std::string_literals;

int main(int argc, char* argv[]) {
  if (!detail::adjust_resource_consumption())
    return 1;
  // Defaults.
  auto dir = "vast"s;
  auto endpoint = std::string{};
  auto host = "127.0.0.1"s;
  auto log_level = 3;
  auto messages = std::numeric_limits<size_t>::max();
  auto name = util::split_to_str(util::hostname(), ".")[0];
  auto port = uint16_t{42000};
  auto profile_file = std::string{};
  auto threads = std::thread::hardware_concurrency();
  // Parse and validate command line.
  auto r = message_builder(argv + 1, argv + argc).extract_opts({
    {"bare,b", "spawn empty node without any actors"},
    {"directory,d", "path to persistent state directory", dir},
    {"endpoint,e", "the node endpoint", endpoint},
    {"foreground,f", "run daemon in foreground"},
    {"log-level,l", "verbosity of console and/or log file", log_level},
    {"messages,m", "maximum messages per CAF scheduler invocation", messages},
    {"name,n", "the name of this node", name},
    {"profile,p", "enable CAF profiler", profile_file},
    {"threads,t", "number of worker threads in CAF scheduler", threads},
    {"version,v", "print version and exit"}
  });
  if (! r.error.empty()) {
    std::cerr << r.error << std::endl;
    return 1;
  }
  if (r.opts.count("version") > 0) {
    std::cout << VAST_VERSION << std::endl;
    return 0;
  }
  if (r.opts.count("help") > 0) {
    std::cout << banner() << "\n\n" << r.helptext;
    return 0;
  }
  if (r.opts.count("endpoint") > 0
      && !util::parse_endpoint(endpoint, host, port)) {
    std::cout << "invalid endpoint: " << endpoint << std::endl;
    return 1;
  }
  if (!r.remainder.empty()) {
    auto arg = r.remainder.get_as<std::string>(0);
    std::cerr << "invalid stray argument: " << arg << std::endl;
    return 1;
  }
  // Initialize logger.
  auto verbosity = static_cast<logger::level>(log_level);
  auto log_dir = path{dir} / node::log_path();
  if (!logger::file(verbosity, (log_dir / "vast.log").str())) {
    std::cerr << "failed to initialize logger file backend" << std::endl;
    return 1;
  }
  if (r.opts.count("foreground")) {
    auto colorized = true;
    if (!logger::console(verbosity, colorized)) {
      std::cerr << "failed to initialize logger console backend" << std::endl;
      return 1;
    }
  } else {
    VAST_DEBUG("deamonizing process (PID", util::process_id() << ")");
    // On Mac OS, daemon(3) is deprecated since 10.5.
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED
    if (::daemon(0, 0) != 0) {
      VAST_ERROR("failed to daemonize process");
      return 1;
    }
    VAST_DIAGNOSTIC_POP
  }
  // Replace/adjust scheduler.
  if (r.opts.count("profile"))
    set_scheduler(
      new scheduler::profiled_coordinator<>{
        profile_file, std::chrono::milliseconds{1000}, threads, messages});
  else if (r.opts.count("threads") || r.opts.count("messages"))
    set_scheduler<>(threads, messages);
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
  // Initialize node actor.
  auto guard = make_scope_guard(
    [] { caf::shutdown(); logger::destruct(); }
  );
  announce_types();
  auto node = spawn<vast::node>(name, dir);
  scoped_actor self;
  if (r.opts.count("bare") == 0)
    self->sync_send(node, "spawn", "core").await([](ok_atom) {});
  try {
    // Publish the node.
    auto bound_port = caf::io::publish(node, port, host.c_str());
    VAST_VERBOSE("listening on", host << ':' << bound_port,
                 "with name \"" << name << '"');
    /// Install signal handlers and block until either a signal arrives or the
    /// node terminates.
    auto sig_mon = self->spawn<signal_monitor>(self);
    self->monitor(node);
    auto stop = false;
    self->do_receive(
      [&](down_msg const& msg) {
        VAST_DEBUG("received DOWN from " << msg.source);
        stop = true;
      },
      [&](signal_atom, int signal) {
        VAST_DEBUG("got " << ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM)
          stop = true;
        else
          self->send(node, signal_atom::value, signal);
      },
      others() >> [&] {
        VAST_WARN("received unexpected message:",
                   to_string(self->current_message()));
      }
    ).until([&] { return stop == true; });
    if (node->exit_reason() == exit_reason::not_exited)
      self->send_exit(node, exit::stop);
    self->send_exit(sig_mon, exit::stop);
    self->await_all_other_actors_done();
    auto er = node->exit_reason();
    if (er == exit::error)
      return 1;
    else if (er == exit::kill)
      return -1;
    else if (!(er == exit::done || er == exit::stop))
      return 2;
  } catch (caf::network_error const& e) {
    VAST_ERROR(e.what());
    self->send_exit(node, exit::stop);
    return 1;
  } catch (...) {
    VAST_ERROR("terminating due to uncaught exception");
    return 1;
  }
  return 0;
}
