#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/announce.h"
#include "vast/banner.h"
#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/actor/node.h"
#include "vast/util/endpoint.h"
#include "vast/util/string.h"

using namespace vast;
using namespace std::string_literals;

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
    {"endpoint,e", "the node endpoint", endpoint},
    {"log-level,l", "verbosity of console and/or log file", log_level},
    {"version,v", "print version and exit"}
  });
  if (! r.error.empty())
  {
    std::cerr << r.error << std::endl;
    return 1;
  }
  if (r.opts.count("version") > 0)
  {
    std::cout << VAST_VERSION << std::endl;
    return 0;
  }
  if (r.opts.count("help") > 0)
  {
    std::cout << banner() << "\n\n" << r.helptext;
    return 0;
  }
  if (r.opts.count("endpoint") > 0
      && ! util::parse_endpoint(endpoint, host, port))
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
  // Initialize logger.
  auto colorized = true;
  auto verbosity = static_cast<logger::level>(log_level);
  if (! logger::console(verbosity, colorized))
  {
    std::cerr << "failed to initialize logger console backend" << std::endl;
    return 1;
  }
  // Go!
  announce_types();
  auto exit_code = 0;
  auto args = std::vector<std::string>(cmd + 1, command_line.end());
  try
  {
    VAST_VERBOSE("connecting to", host << ':' << port);
    auto node = caf::io::remote_actor(host.c_str(), port);
    // Assemble message from command line.
    caf::message_builder mb;
    mb.append(*cmd);
    for (auto& a : args)
      mb.append(a);
    caf::scoped_actor self;
    self->sync_send(node, mb.to_message()).await(
      [&](ok_atom)
      {
        VAST_VERBOSE("successfully executed command:",
                     *cmd, util::join(args, " "));
      },
      [&](std::string const& str)
      {
        VAST_VERBOSE("successfully executed command:",
                     *cmd, util::join(args, " "));
        std::cout << str << std::endl;
      },
      [&](error const& e)
      {
        VAST_ERROR("failed to executed command:",
                   *cmd, util::join(args, " "));
        VAST_ERROR(e);
        exit_code = 1;
      },
      caf::others() >> [&]
      {
        auto msg = to_string(self->current_message());
        VAST_ERROR("got unexpected message:", msg);
      }
    );
    self->await_all_other_actors_done();
  }
  catch (caf::network_error const& e)
  {
    VAST_ERROR("failed to connect to", host << ':' << port);
    exit_code = 1;
  }
  caf::shutdown();
  logger::destruct();
  return exit_code;
}
