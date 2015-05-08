#include <cassert>
#include <csignal>
#include <cstdlib>

#include <algorithm>
#include <regex>
#include <sstream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <caf/scheduler/profiled_coordinator.hpp>

#include "vast/announce.h"
#include "vast/banner.h"
#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/optional.h"
#include "vast/actor/node.h"
#include "vast/actor/signal_monitor.h"
#include "vast/util/endpoint.h"
#include "vast/util/string.h"
#include "vast/util/system.h"

using namespace vast;
using namespace std::string_literals;

int launch(std::string host, uint16_t port, int log_level,
           std::vector<std::string> const& args)
{
  // Default options
  auto dir = "vast"s;
  auto messages = std::numeric_limits<size_t>::max();
  auto name = util::hostname();
  auto profile_file = std::string{};
  auto threads = std::thread::hardware_concurrency();
  // Parse arguments.
  auto r = caf::message_builder(args.begin(), args.end()).extract_opts({
    {"core,c", "spawn core components"},
    {"directory,d", "path to persistent state directory", dir},
    {"foreground,f", "run daemon in foreground"},
    {"messages,m", "maximum messages per CAF scheduler invocation", messages},
    {"name,n", "the name of this node", name},
    {"profile,p", "enable CAF profiler", profile_file},
    {"threads,w", "number of worker threads in CAF scheduler", threads},
  });
  if (! r.remainder.empty())
  {
    auto arg = r.remainder.get_as<std::string>(0);
    std::cerr << "invalid argument: " << arg << std::endl;
    return 1;
  }
  // Initialize logger.
  auto verbosity = static_cast<logger::level>(log_level);
  auto log_dir = path{dir} / node::log_path();
  if (! logger::file(verbosity, (log_dir / "vast.log").str()))
  {
    std::cerr << "failed to initialize logger file backend" << std::endl;
    return 1;
  }
  if (r.opts.count("foreground"))
  {
    auto colorized = true;
    if (! logger::console(verbosity, colorized))
    {
      std::cerr << "failed to initialize logger console backend" << std::endl;
      return 1;
    }
  }
  else
  {
    VAST_DEBUG("deamonizing process (PID", util::process_id() << ")");
    if (::daemon(0, 0) != 0)
    {
      VAST_ERROR("failed to daemonize process");
      return 1;
    }
  }
  // Replace/adjust scheduler.
  if (r.opts.count("profile"))
    caf::set_scheduler(
      new caf::scheduler::profiled_coordinator<>{
        profile_file, std::chrono::milliseconds{1000}, threads, messages});
  else if (r.opts.count("threads") || r.opts.count("messages"))
    caf::set_scheduler<>(threads, messages);
  VAST_VERBOSE(banner() << "\n\n");
  VAST_VERBOSE("set scheduler threads to", threads);
  VAST_VERBOSE("set scheduler maximum throughput to",
               (messages == std::numeric_limits<size_t>::max()
                ? "unlimited" : std::to_string(messages)));
  // Initialize NODE.
  caf::actor n = caf::spawn<node>(name, dir);
  auto exit_code = 0;
  caf::scoped_actor self;
  try
  {
    // Publish NODE and catch termination signals.
    auto bound_port = caf::io::publish(n, port, host.c_str());
    VAST_VERBOSE("listening on", host << ':' << bound_port,
                 "with name \"" << name << '"');
    auto sig_mon = self->spawn<signal_monitor>(self);
    self->monitor(n);
    // Create core ecosystem.
    if (r.opts.count("core") > 0)
    {
      auto instruct = [&](auto&&... xs)
      {
        self->sync_send(n, std::forward<decltype(xs)>(xs)...).await(
          [](ok_atom) {}
        );
      };
      // FIXME: Perform these operations asynchronously.
      instruct("spawn", "identifier");
      instruct("spawn", "archive");
      instruct("spawn", "index");
      instruct("spawn", "importer");
      instruct("connect", "importer", "identifier");
      instruct("connect", "importer", "archive");
      instruct("connect", "importer", "index");
    }
    // Block until shutdown.
    auto stop = false;
    self->do_receive(
      [&](caf::down_msg const& msg)
      {
        VAST_DEBUG("received DOWN from " << msg.source);
        stop = true;
      },
      [&](signal_atom, int signal)
      {
        if (signal == SIGINT || signal == SIGTERM)
          stop = true;
        else
          self->send(n, signal_atom::value, signal);
      },
      caf::others() >> [&]
      {
        VAST_WARN("received unexpected message:",
                   caf::to_string(self->current_message()));
      }).until([&] { return stop == true; });
    if (n->exit_reason() == caf::exit_reason::not_exited)
      self->send_exit(n, exit::stop);
    self->send_exit(sig_mon, exit::stop);
    self->await_all_other_actors_done();
    auto er = n->exit_reason();
    if (er == exit::error)
      exit_code = 1;
    else if (er == exit::kill)
      exit_code = 2;
    else if (! (er == exit::done || er == exit::stop))
      exit_code = 255;
  }
  catch (caf::network_error const& e)
  {
    VAST_ERROR(e.what());
    self->send_exit(n, exit::stop);
    exit_code = 1;
  }
  logger::destruct();
  return exit_code;
}

int execute(std::string const& host, uint16_t port, int log_level,
            std::string const& cmd, std::vector<std::string> const& args)
{
  auto colorized = true;
  auto verbosity = static_cast<logger::level>(log_level);
  if (! logger::console(verbosity, colorized))
  {
    std::cerr << "failed to initialize logger console backend" << std::endl;
    return 1;
  }
  auto exit_code = 0;
  try
  {
    VAST_VERBOSE("connecting to", host << ':' << port);
    auto node = caf::io::remote_actor(host.c_str(), port);
    caf::message_builder mb;
    mb.append(cmd);
    for (auto& a : args)
      mb.append(a);
    caf::scoped_actor self;
    self->sync_send(node, mb.to_message()).await(
      [&](ok_atom)
      {
        VAST_VERBOSE("successfully executed command:",
                     cmd, util::join(args, " "));
      },
      [&](std::string const& str)
      {
        VAST_VERBOSE("successfully executed command:",
                     cmd, util::join(args, " "));
        std::cout << str << std::endl;
      },
      [&](error const& e)
      {
        VAST_ERROR("failed to executed command:",
                   cmd, util::join(args, " "));
        VAST_ERROR(e);
        exit_code = 1;
      },
      caf::others() >> [&]
      {
        VAST_ERROR("got unexpected message:",
                   caf::to_string(self->current_message()));
      }
    );
    self->await_all_other_actors_done();
  }
  catch (caf::network_error const& e)
  {
    VAST_ERROR("failed to connect to", host << ':' << port);
    exit_code = 1;
  }
  logger::destruct();
  return exit_code;
}

int main(int argc, char *argv[])
{
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
  // Parse and validate command line.
  auto log_level = 3;
  auto endpoint = std::string{};
  auto host = "127.0.0.1"s;
  auto port = uint16_t{42000};
  auto r = caf::message_builder(command_line.begin(), cmd).extract_opts({
    {"node,n", "the node endpoint", endpoint},
    {"log-level,l", "verbosity of console and/or log file", log_level},
    {"version,v", "print version and exit"}
  });
  if (r.opts.count("version") > 0)
  {
    std::cout << VAST_VERSION << std::endl;
    return 0;
  }
  if (r.opts.count("help") > 0)
  {
    std::cout << banner() << "\n\n";
    std::cout << "TODO: display usage" << std::endl; // TODO
    return 0;
  }
  if (r.opts.count("node") > 0 && ! util::parse_endpoint(endpoint, host, port))
  {
    std::cout << "invalid endpoint: " << endpoint << std::endl;
    return 1;
  }
  if (! r.remainder.empty())
  {
    auto invalid_cmd = r.remainder.get_as<std::string>(0);
    std::cerr << "invalid command: " << invalid_cmd << std::endl;
    return 1;
  }
  if (cmd == command_line.end())
  {
    std::cerr << "missing command" << std::endl;
    return 1;
  }
  // Go!
  announce_types();
  auto exit_code = 0;
  auto args = std::vector<std::string>(cmd + 1, command_line.end());
  if (*cmd == "start")
    exit_code = launch(host, port, log_level, args);
  else
    exit_code = execute(host, port, log_level, *cmd, args);
  caf::shutdown();
  return exit_code;
}
