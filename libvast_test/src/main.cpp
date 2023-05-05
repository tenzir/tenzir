//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/data.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/system/configuration.hpp"
#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

#include <caf/message_builder.hpp>
#include <caf/test/unit_test.hpp>

#include <iostream>
#include <set>
#include <string>

namespace artifacts::logs::zeek {

const char* conn = VAST_TEST_PATH "artifacts/logs/zeek/conn.log";
const char* dns = VAST_TEST_PATH "artifacts/logs/zeek/dns.log";
const char* ftp = VAST_TEST_PATH "artifacts/logs/zeek/ftp.log";
const char* http = VAST_TEST_PATH "artifacts/logs/zeek/http.log";
const char* small_conn = VAST_TEST_PATH "artifacts/logs/zeek/small_conn.log";
const char* smtp = VAST_TEST_PATH "artifacts/logs/zeek/smtp.log";
const char* ssl = VAST_TEST_PATH "artifacts/logs/zeek/ssl.log";

} // namespace artifacts::logs::zeek

namespace artifacts::logs::suricata {

const char* alert = VAST_TEST_PATH "artifacts/logs/suricata/alert.json";
const char* dns = VAST_TEST_PATH "artifacts/logs/suricata/dns.json";
const char* fileinfo = VAST_TEST_PATH "artifacts/logs/suricata/fileinfo.json";
const char* flow = VAST_TEST_PATH "artifacts/logs/suricata/flow.json";
const char* http = VAST_TEST_PATH "artifacts/logs/suricata/http.json";
const char* netflow = VAST_TEST_PATH "artifacts/logs/suricata/netflow.json";
const char* stats = VAST_TEST_PATH "artifacts/logs/suricata//stats.json";

} // namespace artifacts::logs::suricata

namespace artifacts::logs::syslog {

const char* syslog_msgs
  = VAST_TEST_PATH "artifacts/logs/syslog/syslog-test.txt";

} // namespace artifacts::logs::syslog

namespace artifacts::schemas {

const char* base = VAST_TEST_PATH "artifacts/schemas/base.schema";
const char* suricata = VAST_TEST_PATH "artifacts/schemas/suricata.schema";

} // namespace artifacts::schemas

namespace artifacts::traces {

const char* nmap_vsn = VAST_TEST_PATH "artifacts/traces/nmap_vsn.pcap";
const char* workshop_2011_browse
  = VAST_TEST_PATH "artifacts/traces/workshop_2011_browse.pcap";

} // namespace artifacts::traces

namespace caf::test {

int main(int, char**);

} // namespace caf::test

namespace vast::test {

extern std::set<std::string> config;

} // namespace vast::test

namespace {

// Retrieves arguments after the '--' delimiter.
std::vector<std::string> get_test_args(int argc, const char* const* argv) {
  // Parse everything after after '--'.
  constexpr std::string_view delimiter = "--";
  auto start = argv + 1;
  auto end = argv + argc;
  auto args_start = std::find(start, end, delimiter);
  if (args_start == end)
    return {};
  return {args_start + 1, end};
}

} // namespace

int main(int argc, char** argv) {
  std::string vast_loglevel = "quiet";
  auto test_args = get_test_args(argc, argv);
  if (!test_args.empty()) {
    auto options
      = caf::config_option_set{}
          .add(vast_loglevel, "vast-verbosity", "console verbosity for libvast")
          .add<bool>("help", "print this help text");
    caf::settings cfg;
    auto res = options.parse(cfg, test_args);
    if (res.first != caf::pec::success) {
      std::cout << "error while parsing argument \"" << *res.second
                << "\": " << to_string(res.first) << "\n\n";
      std::cout << options.help_text() << std::endl;
      return 1;
    }
    if (caf::get_or(cfg, "help", false)) {
      std::cout << options.help_text() << std::endl;
      return 0;
    }
    vast::test::config = {
      std::make_move_iterator(std::begin(test_args)),
      std::make_move_iterator(std::end(test_args)),
    };
  }
  // TODO: Only initialize built-in endpoints here by default,
  // and allow the unit tests to specify a list of required
  // plugins and their config.
  for (auto& plugin : vast::plugins::get_mutable()) {
    if (plugin->enabled({}, {})) {
      if (auto err = plugin->initialize({}, {})) {
        fmt::print(stderr, "failed to initialize plugin {}: {}", plugin->name(),
                   err);
        return EXIT_FAILURE;
      }
    }
  }
  caf::settings log_settings;
  put(log_settings, "vast.console-verbosity", vast_loglevel);
  put(log_settings, "vast.console-format", "%^[%s:%#] %v%$");
  auto log_context = vast::create_log_context(vast::invocation{}, log_settings);
  // Initialize factories.
  [[maybe_unused]] auto config = vast::system::configuration{};
  // Run the unit tests.
  return caf::test::main(argc, argv);
}
