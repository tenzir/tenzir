#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/aliases.h"
#include "vast/announce.h"
#include "vast/banner.h"
#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/uuid.h"
#include "vast/actor/accountant.h"
#include "vast/actor/atoms.h"
#include "vast/actor/exit.h"
#include "vast/actor/sink/spawn.h"
#include "vast/actor/source/spawn.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/uuid.h"
#include "vast/detail/adjust_resource_consumption.h"
#include "vast/util/endpoint.h"
#include "vast/util/string.h"

using namespace vast;
using namespace std::string_literals;

int main(int argc, char *argv[])
{
  if (! detail::adjust_resource_consumption())
    return 1;
  caf::set_scheduler<>(2, -1);
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
  auto dir = "."s;
  auto endpoint = ""s;
  auto host = "127.0.0.1"s;
  auto port = uint16_t{42000};
  auto r = caf::message_builder(command_line.begin(), cmd).extract_opts({
    {"dir,d", "directory for logs and client state", dir},
    {"endpoint,e", "node endpoint", endpoint},
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
  // Process commands.
  caf::scoped_actor self;
  auto accounting_log = path(dir) / "accounting.log";
  if (*cmd == "import")
  {
    // 1. Spawn a SOURCE.
    caf::message_builder mb;
    auto i = cmd + 1;
    while (i != command_line.end())
      mb.append(*i++);
    auto src = source::spawn(mb.to_message());
    if (! src)
    {
      VAST_ERROR("failed to spawn source:", src.error());
      return 1;
    }
    auto source_guard = caf::detail::make_scope_guard(
      [=] { anon_send_exit(*src, exit::kill); }
    );
    auto acc = self->spawn<accountant<uint64_t>>(accounting_log);
    acc->link_to(*src);
    self->send(*src, put_atom::value, accountant_atom::value, acc);
    // 2. Find all next-best IMPORTERs.
    std::vector<caf::actor> importers;
    self->sync_send(node, store_atom::value).await(
      [&](caf::actor const& store) {
        self->sync_send(store, list_atom::value, "actors/").await(
          [&](std::map<std::string, caf::message>& m) {
            for (auto& p : m)
              if (p.second.get_as<std::string>(1) == "importer") {
                auto imp = p.second.get_as<caf::actor>(0);
                if (imp != caf::invalid_actor)
                  importers.push_back(imp);
              }
          }
        );
      }
    );
    if (importers.empty())
    {
      VAST_ERROR("no importers found");
      return 1;
    }
    // 3. Connect SOURCE and IMPORTERs.
    for (auto& imp : importers)
    {
      //VAST_ASSERT(imp != caf::invalid_actor);
      VAST_DEBUG("connecting source with importer", imp);
      self->send(*src, put_atom::value, sink_atom::value, imp);
    }
    // 4. Run the SOURCE.
    VAST_DEBUG("running source");
    self->send(*src, run_atom::value);
    source_guard.disable();
  }
  else if (*cmd == "export")
  {
    if (cmd + 1 == command_line.end())
    {
      VAST_ERROR("missing sink format");
      return 1;
    }
    else if (cmd + 2 == command_line.end())
    {
      VAST_ERROR("missing query arguments");
      return 1;
    }
    // 1. Spawn a SINK.
    auto snk = sink::spawn(caf::make_message(*(cmd + 1)));
    if (! snk)
    {
      VAST_ERROR("failed to spawn sink:", snk.error());
      return 1;
    }
    auto acc = self->spawn<accountant<uint64_t>>(accounting_log);
    acc->link_to(*snk);
    self->send(*snk, put_atom::value, accountant_atom::value, acc);
    auto sink_guard = caf::detail::make_scope_guard(
      [snk=*snk] { anon_send_exit(snk, exit::kill); }
    );
    // 2. Spawn an EXPORTER.
    caf::message_builder mb;
    auto label = "exporter-" + to_string(uuid::random()).substr(0, 7);
    mb.append("spawn");
    mb.append("-l");
    mb.append(label);
    mb.append("exporter");
    auto i = cmd + 2;
    mb.append(*i++);
    while (i != command_line.end())
      mb.append(*i++);
    caf::actor exporter;
    self->sync_send(node, mb.to_message()).await(
      [&](caf::actor const& actor) {
        exporter = actor;
      },
      [&](error const& e) {
        VAST_ERROR("failed to spawn exporter:", e);
      },
      caf::others >> [&] {
        VAST_ERROR("got unexpected message:",
                   caf::to_string(self->current_message()));
      }
    );
    if (! exporter)
      return 1;
    // 3. Connect EXPORTER with ARCHIVEs and INDEXes.
    std::vector<caf::actor> archives;
    std::vector<caf::actor> indexes;
    VAST_DEBUG("retrieving topology to connect exporter with archive/index");
    self->sync_send(node, store_atom::value).await(
      [&](caf::actor const& store) {
        self->sync_send(store, list_atom::value, "actors").await(
          [&](std::map<std::string, caf::message>& m) {
            for (auto& p : m)
              if (p.second.get_as<std::string>(1) == "archive")
                archives.push_back(p.second.get_as<caf::actor>(0));
              else if (p.second.get_as<std::string>(1) == "index")
                indexes.push_back(p.second.get_as<caf::actor>(0));
          }
        );
      }
    );
    if (archives.empty())
    {
      VAST_ERROR("no archives found");
      return 1;
    }
    if (indexes.empty())
    {
      VAST_ERROR("no indexes found");
      return 1;
    }
    for (auto& archive : archives)
      self->send(exporter, put_atom::value, archive_atom::value, archive);
    for (auto& index : indexes)
      self->send(exporter, put_atom::value, index_atom::value, index);
    // 4. Connect EXPORTER with SINK.
    self->send(exporter, put_atom::value, sink_atom::value, *snk);
    // 5. Run the EXPORTER.
    VAST_DEBUG("running exporter");
    self->send(exporter, stop_atom::value);
    self->send(exporter, run_atom::value);
    sink_guard.disable();
  }
  else
  {
    auto args = std::vector<std::string>(cmd + 1, command_line.end());
    caf::message_builder mb;
    mb.append(*cmd);
    for (auto& a : args)
      mb.append(std::move(a));
    auto cmd_line = *cmd + util::join(args, " ");
    auto exit_code = 0;
    self->sync_send(node, mb.to_message()).await(
      [&](ok_atom)
      {
        VAST_VERBOSE("successfully executed command:", cmd_line);
      },
      [&](caf::actor const&)
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
        VAST_ERROR("got unexpected reply:", msg);
        exit_code = 1;
      }
    );
    if (exit_code != 0)
      return exit_code;
  }
  self->await_all_other_actors_done();
  return 0;
}
