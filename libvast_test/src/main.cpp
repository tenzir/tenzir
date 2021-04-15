//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

#include "vast/data.hpp"
#include "vast/plugin.hpp"

#include <caf/message_builder.hpp>
#include <caf/test/unit_test.hpp>

#include <iostream>
#include <set>
#include <string>

namespace artifacts {
namespace logs {
namespace zeek {

const char* conn = VAST_TEST_PATH "artifacts/logs/zeek/conn.log";
const char* dns = VAST_TEST_PATH "artifacts/logs/zeek/dns.log";
const char* ftp = VAST_TEST_PATH "artifacts/logs/zeek/ftp.log";
const char* http = VAST_TEST_PATH "artifacts/logs/zeek/http.log";
const char* small_conn = VAST_TEST_PATH "artifacts/logs/zeek/small_conn.log";
const char* smtp = VAST_TEST_PATH "artifacts/logs/zeek/smtp.log";
const char* ssl = VAST_TEST_PATH "artifacts/logs/zeek/ssl.log";

} // namespace zeek

namespace syslog {

const char* syslog_msgs
  = VAST_TEST_PATH "artifacts/logs/syslog/syslog-test.txt";

} // namespace syslog
} // namespace logs

namespace traces {

const char* nmap_vsn = VAST_TEST_PATH "artifacts/traces/nmap_vsn.pcap";
const char* workshop_2011_browse
  = VAST_TEST_PATH "artifacts/traces/workshop_2011_browse.pcap";

} // namespace traces
} // namespace artifacts

namespace caf::test {

int main(int, char**);

} // namespace caf::test

namespace vast::test {

extern std::set<std::string> config;

} // namespace vast::test

int main(int argc, char** argv) {
  // Parse everything after after '--'.
  std::string delimiter = "--";
  auto start = argc;
  for (auto i = 1; i < argc - 1; ++i) {
    if (argv[i] == delimiter) {
      start = i + 1;
      break;
    }
  }
  if (start != argc) {
    auto res = caf::message_builder(argv + start, argv + argc).extract_opts({});
    if (!res.error.empty()) {
      std::cout << res.error << std::endl;
      return 1;
    }
    if (res.opts.count("help") > 0) {
      std::cout << res.helptext << std::endl;
      return 0;
    }
    vast::test::config = std::move(res.opts);
  }
  for (auto& plugin : vast::plugins::get())
    plugin->initialize({});
  // Run the unit tests.
  return caf::test::main(argc, argv);
}
