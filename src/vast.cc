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
#include "vast/actor/atoms.h"
#include "vast/actor/exit.h"
#include "vast/actor/source/spawn.h"
#include "vast/concept/printable/vast/error.h"
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
  if (! logger::file(logger::quiet))
  {
    std::cerr << "failed to reset logger file backend" << std::endl;
    return 1;
  }
  // Establish connection to remote node.
  auto guard = caf::detail::make_scope_guard(
    [] { caf::shutdown(); logger::destruct(); }
  );
  announce_types();
  caf::actor node;
  try
  {
    VAST_VERBOSE("connecting to", host << ':' << port);
    node = caf::io::remote_actor(host.c_str(), port);
  }
  catch (caf::network_error const& e)
  {
    VAST_ERROR("failed to connect to", host << ':' << port);
    return 1;
  }
  // Assemble message from command line.
  auto args = std::vector<std::string>(cmd + 1, command_line.end());
  caf::message_builder mb;
  mb.append(*cmd);
  for (auto& a : args)
    mb.append(a);
  caf::scoped_actor self;
  if (*cmd == "import")
  {
    // 1. Spawn a source.
    auto src = source::spawn(mb.to_message().drop(1));
    if (! src)
    {
      VAST_ERROR("failed to spawn source:", src.error());
      return 1;
    }
    auto source_guard = caf::detail::make_scope_guard(
      [=] { anon_send_exit(*src, exit::kill); }
    );
    // 2. Find the next-best importer.
    caf::actor importer;
    self->sync_send(node, store_atom::value).await(
      [&](caf::actor const& store) {
        self->sync_send(store, list_atom::value, "actors").await(
          [&](std::map<std::string, caf::message>& m) {
            for (auto& p : m)
              // TODO: as opposed to taking the first importer available, it
              // could make sense to take the one which faces the least load.
              if (p.second.get_as<std::string>(1) == "importer")
              {
                importer = p.second.get_as<caf::actor>(0);
                return;
              }
          },
          caf::others >> [&] {
            VAST_ERROR("got unexpected message:",
                       caf::to_string(self->current_message()));
          }
        );
      }
    );
    if (! importer)
    {
      VAST_ERROR("could not obtain importer from node");
      return 1;
    }
    // 3. Connect source and importer.
    VAST_DEBUG("connecting source with remote importer");
    auto msg = make_message(put_atom::value, sink_atom::value, importer);
    self->send(*src, std::move(msg));
    // 4. Run the source.
    VAST_DEBUG("running source");
    self->send(*src, run_atom::value);
    source_guard.disable();
  }
  else
  {
    auto cmd_line = *cmd + util::join(args, " ");
    auto exit_code = 0;
    self->sync_send(node, mb.to_message()).await(
      [&](ok_atom)
      {
        VAST_VERBOSE("successfully executed command:", cmd_line);
      },
      [&](std::string const& str)
      {
        VAST_VERBOSE("successfully executed command:", cmd_line);
        std::cout << str << std::endl;
      },
      [&](error const& e)
      {
        VAST_ERROR("failed to execute command:", cmd_line);
        VAST_ERROR(e);
        exit_code = 1;
      },
      caf::others >> [&]
      {
        auto msg = to_string(self->current_message());
        VAST_ERROR("got unexpected message:", msg);
        exit_code = 1;
      }
    );
    if (exit_code != 0)
      return exit_code;
  }
  self->await_all_other_actors_done();
  return 0;
}
