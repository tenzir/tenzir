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


#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

#include "caf/message_builder.hpp"
#include "caf/test/unit_test.hpp"

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
const char* out = VAST_TEST_PATH "artifacts/logs/zeek/out.txt";
const char* small_conn = VAST_TEST_PATH "artifacts/logs/zeek/small_conn.log";
const char* smtp = VAST_TEST_PATH "artifacts/logs/zeek/smtp.log";
const char* ssl = VAST_TEST_PATH "artifacts/logs/zeek/ssl.log";

} // namespace zeek

namespace bgpdump {

const char* updates20140821 = VAST_TEST_PATH
  "artifacts/logs/bgpdump/updates20140821.txt";
const char* updates20180124 = VAST_TEST_PATH
  "artifacts/logs/bgpdump/updates20180124.txt";

} // namespace bgpdump

namespace mrt {

const char* bview = VAST_TEST_PATH "artifacts/logs/mrt/bview.20161024.0800";
const char* updates20150505 = VAST_TEST_PATH
  "artifacts/logs/mrt/updates20150505.0";

} // namespace mrt
} // namespace logs

namespace traces {

const char* nmap_vsn = VAST_TEST_PATH "artifacts/traces/nmap_vsn.pcap";
const char* workshop_2011_browse = VAST_TEST_PATH
  "artifacts/traces/workshop_2011_browse.pcap";

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
    auto res = caf::message_builder(argv + start, argv + argc).extract_opts({
      {"gperftools,g", "enable gperftools profiler for actor tests"},
    });
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
  // Make sure we have enough resources (e.g., file descriptors).
  if (!vast::detail::adjust_resource_consumption())
    return 1;
  // Run the unit tests.
  return caf::test::main(argc, argv);
}
