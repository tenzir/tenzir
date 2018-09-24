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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace vast::defaults {

namespace command {

/// Path to persistent state.
extern const char* directory;

/// Locator for connecting to a remote node in "host:port" notation.
extern const char* endpoint;

/// Server ID for the consensus module.
extern const char* id;

/// Path for reading input events or `-` for reading from STDIN.
extern const char* read_path;

/// Path to alternate schema.
extern const char* schema_path;

/// Path for writing query results or `-` for writing to STDOUT.
extern const char* write_path;

/// Inverse factor by which to delay packets. For example, if 5, then for two
/// packets spaced *t* seconds apart, the source will sleep for *t/5* seconds.
extern int64_t pseudo_realtime_factor;

/// Number of bytes to keep per event.
extern size_t cutoff;

/// Flow table expiration interval.
extern size_t flow_expiry;

/// Flush to disk after that many packets.
extern size_t flush_interval;

/// Maximum number of results.
extern size_t max_events;

/// Maximum flow lifetime before eviction.
extern size_t max_flow_age;


/// Number of concurrent flows to track.
extern size_t max_flows;

/// The unique ID of this node.
extern const char* node_id;

} // namespace command

namespace system {

/// Maximum size for sources that generate table slices.
extern size_t table_slice_size;

} // namespace system

} // namespace vast::defaults
