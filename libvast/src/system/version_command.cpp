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

#include "vast/system/version_command.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/config.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/data.hpp"

#if VAST_ENABLE_ARROW
#  include <arrow/util/config.h>
#endif

#if VAST_ENABLE_PCAP
#  include <pcap/pcap.h>
#endif

#if VAST_ENABLE_JEMALLOC
#  include <jemalloc/jemalloc.h>
#endif

#include <iostream>
#include <sstream>

namespace vast::system {

namespace {

record retrieve_versions() {
  record result;
  result["VAST"] = VAST_VERSION;
  result["VAST Build Tree Hash"] = VAST_BUILD_TREE_HASH;
  std::ostringstream caf_version;
  caf_version << CAF_MAJOR_VERSION << '.' << CAF_MINOR_VERSION << '.'
              << CAF_PATCH_VERSION;
  result["CAF"] = caf_version.str();
#if VAST_ENABLE_ARROW
  std::ostringstream arrow_version;
  arrow_version << ARROW_VERSION_MAJOR << '.' << ARROW_VERSION_MINOR << '.'
                << ARROW_VERSION_PATCH;
  result["Apache Arrow"] = arrow_version.str();
#else
  result["Apache Arrow"] = data{};
#endif
#if VAST_ENABLE_PCAP
  result["PCAP"] = pcap_lib_version();
#else
  result["PCAP"] = data{};
#endif
#if VAST_ENABLE_JEMALLOC
  result["jemalloc"] = JEMALLOC_VERSION;
#else
  result["jemalloc"] = data{};
#endif
  record plugin_versions;
  for (auto& plugin : plugins::get())
    plugin_versions[plugin->name()] = to_string(plugin.version());
  result["plugins"] = std::move(plugin_versions);
  return result;
}

record combine(const record& lhs, const record& rhs) {
  auto result = lhs;
  for (const auto& field : rhs)
    result.insert(field);
  return result;
}

} // namespace

void print_version(const record& extra_content) {
  auto version = retrieve_versions();
  std::cout << to_string(combine(extra_content, version)) << std::endl;
}

caf::message
version_command([[maybe_unused]] const invocation& inv, caf::actor_system&) {
  VAST_TRACE_SCOPE("{}", inv);
  print_version();
  return caf::none;
}

} // namespace vast::system
