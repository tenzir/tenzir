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
#include "vast/concept/printable/vast/json.hpp"
#include "vast/config.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"

#if VAST_HAVE_ARROW
#  include <arrow/util/config.h>
#endif

#if VAST_HAVE_PCAP
#  include <pcap/pcap.h>
#endif

#if VAST_USE_JEMALLOC
#  include <jemalloc/jemalloc.h>
#endif

#include <iostream>
#include <sstream>

namespace vast::system {

namespace {

json::object retrieve_versions() {
  json::object result;
  result["VAST"] = VAST_VERSION;
  std::ostringstream caf_v;
  caf_v << CAF_MAJOR_VERSION << '.' << CAF_MINOR_VERSION << '.'
        << CAF_PATCH_VERSION;
  result["CAF"] = caf_v.str();
#if VAST_HAVE_ARROW
  std::ostringstream arrow_v;
  arrow_v << ARROW_VERSION_MAJOR << '.' << ARROW_VERSION_MINOR << '.'
          << ARROW_VERSION_PATCH;
  result["Apache Arrow"] = arrow_v.str();
#else
  result["Apache Arrow"] = json{};
#endif
#if VAST_HAVE_PCAP
  result["PCAP"] = pcap_lib_version();
#else
  result["PCAP"] = json{};
#endif
#if VAST_USE_JEMALLOC
  result["jemalloc"] = JEMALLOC_VERSION;
#else
  result["jemalloc"] = json{};
#endif
  return result;
}

} // namespace

void print_version(const json::object& extra_content) {
  auto version = retrieve_versions();
  std::cout << to_string(combine(extra_content, version)) << std::endl;
}

caf::message
version_command([[maybe_unused]] const invocation& inv, caf::actor_system&) {
  VAST_TRACE(inv);
  print_version();
  return caf::none;
}

} // namespace vast::system
