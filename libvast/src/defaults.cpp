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

#include "vast/si_literals.hpp"

#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"

using namespace vast::si_literals;

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
size_t max_flows = 1_Mi;
size_t generated_events = 100;
const char* node_id = "node";

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

} // namespace system

} // namespace vast::defaults
