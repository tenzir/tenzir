#include <algorithm>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include <caf/io/all.hpp>

#include "vast/aliases.h"
#include "vast/announce.h"
#include "vast/banner.h"
#include "vast/caf.h"
#include "vast/filesystem.h"
#include "vast/key.h"
#include "vast/logger.h"
#include "vast/uuid.h"
#include "vast/actor/accountant.h"
#include "vast/actor/atoms.h"
#include "vast/actor/node.h"
#include "vast/actor/sink/spawn.h"
#include "vast/actor/source/spawn.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/time.h"
#include "vast/concept/printable/vast/uuid.h"
#include "vast/detail/adjust_resource_consumption.h"
#include "vast/util/endpoint.h"
#include "vast/util/flat_set.h"
#include "vast/util/string.h"
#include "vast/util/system.h"

using namespace vast;
using namespace std::string_literals;

int main(int argc, char* argv[]) {
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
    "stop"
  };
  std::vector<std::string> command_line(argv + 1, argv + argc);
  auto cmd = std::find_first_of(command_line.begin(), command_line.end(),
                                commands.begin(), commands.end());
  // Parse options.
  auto log_level = 3;
  auto dir = "."s;
  auto endpoint = ""s;
  auto host = "127.0.0.1"s;
  auto port = uint16_t{42000};
  auto messages = std::numeric_limits<size_t>::max();
  auto profile_file = std::string{};
  auto threads = std::thread::hardware_concurrency();
  // Options for "spawn core".
  auto core = make_message("spawn", "core");
  std::string id_batch_size;
  std::string archive_comp;
  std::string archive_segments;
  std::string archive_size;
  std::string index_events;
  std::string index_active;
  std::string index_passive;
  auto r = message_builder(command_line.begin(), cmd).extract_opts({
    {"no-colors,C", "disable colors on console"},
    {"dir,d", "directory for logs and client state", dir},
    {"endpoint,e", "node endpoint", endpoint},
    {"log-level,l", "verbosity of console and/or log file", log_level},
    {"messages,m", "CAF scheduler message throughput", messages},
    {"profile,p", "CAF scheduler profiling", profile_file},
    {"threads,t", "CAF scheduler threads", threads},
    {"version,v", "print version and exit"},
    // FIXME: We need a better way to manage program options in the future.
    // option handling is lifted from node.cc. And there, we also stitched it
    // together from the individual actor options. It's too easy to diverge.
    {"identifier-batch-size", "initial identifier btach size", id_batch_size},
    {"archive-compression", "archive compression algorithm", archive_comp},
    {"archive-segments", "archive in-memory segments", archive_segments},
    {"archive-size", "archive segment size", archive_size},
    {"index-events", "maximum number of events per partition", index_events},
    {"index-active", "number of active partitions", index_active},
    {"index-passive", "number of passive partitions", index_passive}
  });
  if (! r.error.empty()) {
    std::cerr << r.error << std::endl;
    return 1;
  }
  if (r.opts.count("identifier-batch-size") > 0)
    core = core + make_message("--identifier-batch-size=", id_batch_size);
  if (r.opts.count("archive-compression") > 0)
    core = core + make_message("--archive-compression=" + archive_comp);
  if (r.opts.count("archive-segments") > 0)
    core = core + make_message("--archive-segments=" + archive_segments);
  if (r.opts.count("archive-size") > 0)
    core = core + make_message("--archive-size=" + archive_size);
  if (r.opts.count("index-events") > 0)
    core = core + make_message("--index-events=" + index_events);
  if (r.opts.count("index-active") > 0)
    core = core + make_message("--index-active=" + index_active);
  if (r.opts.count("index-passive") > 0)
    core = core + make_message("--index-passive=" + index_passive);
  if (r.opts.count("version") > 0) {
    std::cout << VAST_VERSION << std::endl;
    return 0;
  }
  if (r.opts.count("help") > 0) {
    std::cout << banner() << "\n\n" << r.helptext;
    return 0;
  }
  if (r.opts.count("endpoint") > 0) {
    if (!util::parse_endpoint(endpoint, host, port)) {
      std::cerr << "invalid endpoint: " << endpoint << std::endl;
      return 1;
    }
  } else if (dir == ".") {
    dir = "vast"; // Don't clobber current directory.
  }
  if (!r.remainder.empty()) {
    auto invalid_cmd = r.remainder.get_as<std::string>(0);
    std::cerr << "invalid command: " << invalid_cmd << std::endl;
    return 1;
  }
  if (threads == 0) {
    std::cerr << "CAF scheduler threads cannot be 0" << std::endl;
    return 1;
  }
  if (messages == 0) {
    std::cerr << "CAF scheduler throughput cannot be 0" << std::endl;
    return 1;
  }
  if (cmd == command_line.end()) {
    std::cerr << "missing command" << std::endl;
    return 1;
  }
  // Initialize logger.
  auto console = [no_colors = r.opts.count("no-colors") > 0](auto v) {
    return no_colors ? logger::console(v) : logger::console_colorized(v);
  };
  auto verbosity = static_cast<logger::level>(log_level);
  if (!console(verbosity)) {
    std::cerr << "failed to initialize logger console backend" << std::endl;
    return 1;
  }
  if (r.opts.count("endpoint") > 0)
    verbosity = logger::quiet;
  auto log_file = dir / node::log_path() / "vast.log";
  if (!logger::file(verbosity, log_file.str())) {
    std::cerr << "failed to initialize logger file backend" << std::endl;
    return 1;
  }
  auto guard = make_scope_guard([] {
    caf::shutdown();
    logger::destruct();
  });
  // Replace/adjust scheduler.
  VAST_ASSERT(threads > 0);
  VAST_ASSERT(messages > 0);
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
  announce_types();
  actor node;
  scoped_actor self;
  auto failed = false;
  auto node_name = util::split_to_str(util::hostname(), ".")[0];
  if (r.opts.count("endpoint") == 0) {
    // Spawn local NODE.
    node = self->spawn<vast::node>(node_name, dir);
    self->sync_send(node, std::move(core)).await(
      [](ok_atom) {},
      [&](error const& e) {
        failed = true;
        VAST_ERROR("failed to spawn core:", e);
      }
    );
    if (failed) {
      if (node->exit_reason() == exit_reason::not_exited) {
        self->monitor(node);
        self->send_exit(node, exit::error);
        self->receive([&](down_msg const&) {});
      }
      return 1;
    }
  } else {
    // Establish connection to remote node.
    try {
      VAST_VERBOSE("connecting to", host << ':' << port);
      node = caf::io::remote_actor(host.c_str(), port);
    } catch (caf::network_error const& e) {
      VAST_ERROR("failed to connect to", host << ':' << port);
      return 1;
    }
  }
  // Prepare accountant.
  vast::accountant::actor_type accountant;
  if (node->is_remote()) {
    VAST_DEBUG("spawning local accountant");
    accountant = self->spawn(accountant::actor, path(dir) / "accounting.log");
  } else {
    VAST_DEBUG("using node accountant");
    self->sync_send(node, store_atom::value, get_atom::value,
                    key::str("actors", node_name, "accountant")).await(
      [&](accountant::actor_type const& acc) {
        accountant = acc;
      }
    );
  }
  // Process commands.
  auto start = time::snapshot();
  if (*cmd == "import") {
    // 1. Spawn a SOURCE.
    message_builder mb;
    auto i = cmd + 1;
    while (i != command_line.end())
      mb.append(*i++);
    auto src = source::spawn(mb.to_message());
    if (!src) {
      VAST_ERROR("failed to spawn source:", src.error());
      return 1;
    }
    auto source_guard = make_scope_guard(
      [=] { anon_send_exit(*src, exit::kill); }
    );
    self->send(*src, accountant);
    // 2. Find all IMPORTERs to load-balance across them.
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
    // 3. Connect SOURCE and IMPORTERs.
    for (auto& imp : importers) {
      VAST_ASSERT(imp != invalid_actor);
      VAST_DEBUG("connecting source with importer", imp);
      self->send(*src, put_atom::value, sink_atom::value, imp);
    }
    // 4. Run the SOURCE.
    source_guard.disable();
    self->send(*src, run_atom::value);
    self->monitor(*src);
    self->receive([](down_msg const&) {});
  } else if (*cmd == "export") {
    if (cmd + 1 == command_line.end()) {
      VAST_ERROR("missing sink format");
      return 1;
    } else if (cmd + 2 == command_line.end()) {
      VAST_ERROR("missing query arguments");
      return 1;
    }
    // 1. Spawn a SINK.
    auto snk = sink::spawn(make_message(*(cmd + 1)));
    if (!snk) {
      VAST_ERROR("failed to spawn sink:", snk.error());
      return 1;
    }
    auto sink_guard = make_scope_guard(
      [snk = *snk] { anon_send_exit(snk, exit::kill); }
    );
    self->send(*snk, accountant);
    // 2. For each node, spawn an (auto-connected) EXPORTER and connect it to
    // the sink.
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
      // Spawn remote exporter.
      message_builder mb;
      auto label = "exporter-" + to_string(uuid::random()).substr(0, 7);
      mb.append("spawn");
      mb.append("-l");
      mb.append(label);
      mb.append("exporter");
      mb.append("-a");
      auto i = cmd + 2;
      mb.append(*i++);
      while (i != command_line.end())
        mb.append(*i++);
      self->send(n, mb.to_message());
      VAST_DEBUG("created", label, "at node" << node);
    }
    // 3. Wait until the remote NODE returns the EXPORTERs so that we can
    // monitor them and connect them with our local SINK.
    auto early_finishers = 0u;
    util::flat_set<actor> exporters;
    self->do_receive(
      [&](actor const& exporter) {
        exporters.insert(exporter);
        self->monitor(exporter);
        self->send(exporter, put_atom::value, sink_atom::value, *snk);
        VAST_DEBUG("running exporter");
        self->send(exporter, stop_atom::value);
        self->send(exporter, run_atom::value);
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
    // 4. Wait for all EXPORTERs to terminate. Thereafter we can shutdown the
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
    }
    if (failed)
      return 1;
    sink_guard.disable();
    self->monitor(*snk);
    self->send_exit(*snk, exit::done);
    self->receive([](down_msg const&) {});
  } else {
    // Only "import" and "export" are local commands, the remote node executes
    // all other ones.
    auto args = std::vector<std::string>(cmd + 1, command_line.end());
    message_builder mb;
    mb.append(*cmd);
    for (auto& a : args)
      mb.append(std::move(a));
    auto cmd_line = *cmd + util::join(args, " ");
    auto exit_code = 0;
    VAST_DEBUG("sending command:", to_string(mb.to_message()));
    self->sync_send(node, mb.to_message()).await(
      [&](ok_atom) {
        VAST_VERBOSE("successfully executed command:", cmd_line);
      },
      [&](actor const&) {
        VAST_VERBOSE("successfully executed command:", cmd_line);
      },
      [&](std::string const& str) {
        VAST_VERBOSE("successfully executed command:", cmd_line);
        std::cout << str << std::endl;
      },
      [&](error const& e) {
        VAST_ERROR("failed to execute command:", cmd_line);
        VAST_ERROR(e);
        exit_code = 1;
      },
      others >> [&] {
        auto msg = to_string(self->current_message());
        VAST_ERROR("got unexpected reply:", msg);
        exit_code = 1;
      }
    );
    if (exit_code != 0)
      return exit_code;
  }
  if (node->is_remote()) {
    self->monitor(accountant);
    self->send_exit(accountant, exit::done);
    self->receive([](down_msg const&) {});
  } else {
    self->send_exit(node, exit::done);
    self->await_all_other_actors_done();
  }
  auto stop = time::snapshot();
  VAST_INFO("completed execution in", stop - start);
  return 0;
}
