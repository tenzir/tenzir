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

#include <iostream>

#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "vast/detail/adjust_resource_consumption.hpp"

#include "test.hpp"

namespace vast {
namespace test {

std::set<std::string> config;

} // namespace test
} // namespace vast

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
