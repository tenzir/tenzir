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

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/settings.hpp>

#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/si_literals.hpp"

using namespace vast::si_literals;
using namespace std::literals::chrono_literals;

namespace vast::defaults {

namespace command {

std::string_view directory = "vast";
std::string_view endpoint_host = "";
uint16_t endpoint_port = 42000;
std::string_view id = "";
std::string_view read_path = "-";
std::string_view write_path = "-";
int64_t pseudo_realtime_factor = 0;
size_t cutoff = std::numeric_limits<size_t>::max();
size_t flow_expiry = 10;
size_t flush_interval = 10000;
size_t max_events = 0;
size_t max_flow_age = 60;
size_t max_flows = 1_Mi;
size_t generated_events = 100;
std::string_view node_id = "node";

caf::atom_value table_slice_type(caf::actor_system& sys,
                                 caf::settings& options) {
  if (auto val = caf::get_if<caf::atom_value>(&options, "table-slice"))
    return *val;
  return get_or(sys.config(), "vast.table-slice-type",
                system::table_slice_type);
}

} // namespace command

namespace system {

caf::atom_value table_slice_type = caf::atom("default");
size_t table_slice_size = 100;
size_t max_partition_size = 1_Mi;
size_t max_in_mem_partitions = 10;
size_t taste_partitions = 5;
size_t num_query_supervisors = 10;
size_t segments = 10;
size_t max_segment_size = 128;
size_t initially_requested_ids = 128;
std::chrono::milliseconds telemetry_rate = 1000ms;
caf::timespan aging_frequency = 24h;

} // namespace system

} // namespace vast::defaults
