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

#include "vast/defaults.hpp"

#include <limits>

#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"

namespace vast::defaults {

namespace command {

const char* directory = "vast";
const char* endpoint = ":42000";
const char* id = "";
const char* read_path = "-";
const char* write_path = "-";
int64_t pseudo_realtime_factor = 0;
size_t cutoff = std::numeric_limits<size_t>::max();
size_t flow_expiry = 10;
size_t flush_interval = 10000;
size_t max_events = 0;
size_t max_flow_age = 60;
size_t max_flows = 1u << 20;
std::string node_id = std::string{detail::split(detail::hostname(), ".")[0]};

} // namespace command

namespace system {

size_t table_slice_size = 10000;

} // namespace system

} // namespace vast::defaults
