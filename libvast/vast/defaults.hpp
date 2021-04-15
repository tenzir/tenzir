//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"
#include "vast/config.hpp" // Needed for VAST_ENABLE_ARROW
#include "vast/table_slice_encoding.hpp"

#include <caf/fwd.hpp>

#include <chrono>
#include <cstdint>
#include <string_view>

namespace vast::defaults {

// -- global constants ---------------------------------------------------------

/// Maximum depth in recursive function calls before bailing out.
/// Note: the value must be > 0.
constexpr size_t max_recursion = 100;

// -- constants for the import command and its subcommands ---------------------

namespace import {

/// Maximum size for sources that generate table slices.
constexpr size_t table_slice_size = 1024;

#if VAST_ENABLE_ARROW

/// The default table slice type when arrow is available.
constexpr auto table_slice_type = table_slice_encoding::arrow;

#else // VAST_ENABLE_ARROW

/// The default table slice type when arrow is unavailable.
constexpr auto table_slice_type = table_slice_encoding::msgpack;

#endif // VAST_ENABLE_ARROW

/// Maximum number of results.
constexpr size_t max_events = 0;

/// Timeout after which data is forwarded to the importer regardless of
/// batching and table slices being unfinished.
constexpr std::chrono::milliseconds batch_timeout = std::chrono::seconds{10};

/// Timeout for how long readers should block while waiting for their input.
constexpr std::chrono::milliseconds read_timeout
  = std::chrono::milliseconds{20};

/// Path for reading input events or `-` for reading from STDIN.
constexpr std::string_view read = "-";

/// Contains settings for the csv subcommand.
struct csv {
  static constexpr char separator = ',';

  // TODO: agree on reasonable values
  static constexpr std::string_view set_separator = ",";

  static constexpr std::string_view kvp_separator = "=";
};

/// Contains settings for the test subcommand.
struct test {
  /// @returns a user-defined seed if available, a randomly generated seed
  /// otherwise.
  static size_t seed(const caf::settings& options);
};

} // namespace import

// -- constants for the explore command and its subcommands --------------------

namespace explore {

// A value of zero means 'unlimited' for all three limits below.
// If all limits are non-zero, the number of results is bounded
// by `min(max_events, max_events_query*max_events_context)`.

/// Maximum total number of results.
constexpr size_t max_events = std::numeric_limits<size_t>::max();

/// Maximum number of results for the initial query.
constexpr size_t max_events_query = 100;

/// Maximum number of results for every explored context.
constexpr size_t max_events_context = 100;

} // namespace explore

// -- constants for the export command and its subcommands ---------------------

// Unfortunately, `export` is a reserved keyword. The trailing `_` exists only
// for disambiguation.

namespace export_ {

/// Path for reading reading the query or `-` for reading from STDIN.
constexpr std::string_view read = "-";

/// Maximum number of results.
constexpr size_t max_events = 0;

/// Path for writing query results or `-` for writing to STDOUT.
constexpr std::string_view write = "-";

/// Contains settings for the csv subcommand.
struct csv {
  static constexpr char separator = ',';

  // TODO: agree on reasonable values
  static constexpr std::string_view set_separator = " | ";
};

} // namespace export_

// -- constants for the infer command -----------------------------------------

/// Contains settings for the csv subcommand.
struct infer {
  /// Nested category in config files for this subcommand.
  static constexpr const char* category = "vast.infer";

  /// Path for reading input events.
  static constexpr auto read = defaults::import::read;

  /// Number of bytes to buffer from input.
  static constexpr size_t buffer_size = 8'192;
};

// -- constants for the index --------------------------------------------------

/// Contains constants for value index parameterization.
namespace index {

/// The maximum length of a string before the default string index chops it off.
constexpr size_t max_string_size = 1024;

/// The maximum number elements an index for a container type (set, vector,
/// or table).
constexpr size_t max_container_elements = 256;

} // namespace index

// -- constants for the logger -------------------------------------------------
namespace logger {

/// Log filename.
constexpr const char* log_file = "server.log";

/// Log format for file output.
constexpr const char* file_format = "[%Y-%m-%dT%T.%e%z] [%n] [%l] [%s:%#] %v";

/// Log format for console output.
constexpr const char* console_format = "%^[%T.%e] %v%$";

/// Verbosity for writing to console.
constexpr const caf::atom_value console_verbosity = caf::atom("info");

/// Verbosity for writing to file.
constexpr const caf::atom_value file_verbosity = caf::atom("debug");

/// Maximum number of log messages in the logger queue.
constexpr const size_t queue_size = 1'000'000;

/// Number of logger threads.
constexpr const size_t logger_threads = 1;

/// Rotate log file if the file size exceeds threshold.
constexpr const bool disable_log_rotation = false;

/// File size threshold for the `rotating_file_sink`.
constexpr const size_t rotate_threshold = 10 * 1'024 * 1'024; // 10_Mi;

/// Maximum number of rotated log files that are kept.
constexpr const size_t rotate_files = 3;

} // namespace logger

// -- constants for the entire system ------------------------------------------

/// Contains system-wide constants.
namespace system {

/// Hostname or IP address and port of a remote node.
constexpr std::string_view endpoint = "localhost:42000/tcp";

/// Default port of a remote node.
constexpr uint16_t endpoint_port = 42000;

/// The unique ID of this node.
constexpr std::string_view node_id = "node";

/// Path to persistent state.
constexpr std::string_view db_directory = "vast.db";

/// Interval between two aging cycles.
constexpr caf::timespan aging_frequency = std::chrono::hours{24};

/// Interval between two disk scanning cycles.
constexpr std::chrono::seconds disk_scan_interval = std::chrono::minutes{1};

/// Maximum number of events per INDEX partition.
constexpr size_t max_partition_size = 1'048'576; // 1_Mi

/// Maximum number of in-memory INDEX partitions.
constexpr size_t max_in_mem_partitions = 10;

/// Number of immediately scheduled INDEX partitions.
constexpr size_t taste_partitions = 5;

/// Maximum number of concurrent INDEX queries.
constexpr size_t num_query_supervisors = 10;

/// Number of cached ARCHIVE segments.
constexpr size_t segments = 10;

/// Maximum size of ARCHIVE segments in MiB.
constexpr size_t max_segment_size = 1'024;

/// Number of initial IDs to request in the IMPORTER.
constexpr size_t initially_requested_ids = 128;

/// Rate at which telemetry data is sent to the ACCOUNTANT.
constexpr std::chrono::milliseconds telemetry_rate
  = std::chrono::milliseconds{10000};

/// Interval between checks whether a signal occured.
constexpr std::chrono::milliseconds signal_monitoring_interval
  = std::chrono::milliseconds{750};

/// Timeout for initial connections to the node.
constexpr std::chrono::seconds initial_request_timeout
  = std::chrono::seconds{10};

/// The period to wait until a shutdown sequence finishes cleanly. After the
/// elapses, the shutdown procedure escalates into a "hard kill".
/// @relates shutdown_kill_timeout
constexpr std::chrono::milliseconds shutdown_grace_period
  = std::chrono::minutes{3};

/// Time to wait until receiving a DOWN from a killed actor.
/// @relates shutdown_grace_period
constexpr std::chrono::seconds shutdown_kill_timeout = std::chrono::minutes{1};

/// The allowed false positive rate for an address_synopsis.
constexpr double address_synopsis_fp_rate = 0.01;

/// The allowed false positive rate for a string_synopsis.
constexpr double string_synopsis_fp_rate = 0.01;

} // namespace system

} // namespace vast::defaults
