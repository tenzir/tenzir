//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <caf/fwd.hpp>

#include <chrono>
#include <cstdint>
#include <string_view>

namespace tenzir::defaults {

// -- global constants ---------------------------------------------------------

/// Maximum depth in recursive function calls before bailing out.
/// Note: the value must be > 0.
inline constexpr size_t max_recursion = 100;

// -- constants for the import command and its subcommands ---------------------

namespace import {

/// Maximum size for sources that generate table slices.
inline constexpr uint64_t table_slice_size = 65'536; // 64 Ki

/// Maximum number of results.
inline constexpr size_t max_events = 0;

/// Timeout after which data is forwarded to the importer regardless of
/// batching and table slices being unfinished.
inline constexpr std::chrono::milliseconds batch_timeout
  = std::chrono::seconds{1};

/// Timeout for how long readers should block while waiting for their input.
inline constexpr std::chrono::milliseconds read_timeout
  = std::chrono::milliseconds{20};

/// Path for reading input events or `-` for reading from STDIN.
inline constexpr std::string_view read = "-";

/// Contains settings for the csv subcommand.
struct csv {
  static constexpr std::string_view separator = ",";

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

// -- constants for the export command and its subcommands ---------------------

// Unfortunately, `export` is a reserved keyword. The trailing `_` exists only
// for disambiguation.

namespace export_ {

/// Path for reading reading the query or `-` for reading from STDIN.
inline constexpr std::string_view read = "-";

/// Maximum number of results.
inline constexpr size_t max_events = 0;

/// Path for writing query results or `-` for writing to STDOUT.
inline constexpr std::string_view write = "-";

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
  static constexpr const char* category = "tenzir.infer";

  /// Path for reading input events.
  static constexpr auto read = defaults::import::read;

  /// Number of bytes to buffer from input.
  static constexpr size_t buffer_size = 8'192;
};

// -- constants for the index --------------------------------------------------

/// Contains constants for value index parameterization.
namespace index {

/// The maximum length of a string before the default string index chops it off.
inline constexpr size_t max_string_size = 1024;

/// The maximum number elements an index for a container type (set, vector,
/// or table).
inline constexpr size_t max_container_elements = 256;

} // namespace index

// -- constants for the logger -------------------------------------------------
namespace logger {

/// Log filename.
inline constexpr const char* log_file = "server.log";

/// Log format for file output.
inline constexpr const char* file_format
  = "[%Y-%m-%dT%T.%e%z] [%n] [%l] [%s:%#] %v";

/// Log format for console output.
inline constexpr const char* console_format = "%^[%T.%e] %v%$";

/// Verbosity for writing to console.
inline constexpr const char* console_verbosity = "info";

/// Verbosity for writing to file.
inline constexpr const char* file_verbosity = "debug";

/// Maximum number of log messages in the logger queue (client).
inline constexpr const size_t client_queue_size = 100;

/// Maximum number of log messages in the logger queue (server).
inline constexpr const size_t server_queue_size = 1'000'000;

/// Policy when running out of space in the log queue.
inline constexpr const char* overflow_policy = "overrun_oldest";

/// Number of logger threads.
inline constexpr const size_t logger_threads = 1;

/// Rotate log file if the file size exceeds threshold.
inline constexpr const bool disable_log_rotation = false;

/// File size threshold for the `rotating_file_sink`.
inline constexpr const size_t rotate_threshold = 10 * 1'024 * 1'024; // 10_Mi;

/// Maximum number of rotated log files that are kept.
inline constexpr const size_t rotate_files = 3;

} // namespace logger

// -- constants for the builtin REST endpoints -------------------------
namespace api {

namespace serve {

/// The duration for which results for the last set of results of a pipeline
/// is kept available after being fetched for the first time.
inline constexpr std::chrono::seconds retention_time = std::chrono::minutes{1};

/// Threshold number of events to wait for .
inline constexpr uint64_t min_events = 1;

/// Number of events returned.
inline constexpr uint64_t max_events = 1024;

/// The maximum amount of time to wait for additional having at least
/// `min_events`.
inline constexpr std::chrono::milliseconds timeout
  = std::chrono::milliseconds{2000};

/// The maximum timeout that can be requested by the client.
inline constexpr std::chrono::seconds max_timeout = std::chrono::seconds{5};

} // namespace serve

} // namespace api

// -- constants for the entire system ------------------------------------------

/// Hostname or IP address and port of a remote node.
//  (explicitly use IPv4 here to get predictable behavior even on
//   weird dual-stack setups)
inline constexpr std::string_view endpoint = "127.0.0.1:5158/tcp";

/// Default port of a remote node.
inline constexpr std::string_view endpoint_host = "127.0.0.1";

/// Default port of a remote node.
inline constexpr uint16_t endpoint_port = 5158;

/// The unique ID of this node.
inline constexpr std::string_view node_id = "node";

/// Path to persistent state.
inline constexpr std::string_view state_directory = "tenzir.db";

/// Interval between two aging cycles.
inline constexpr caf::timespan aging_frequency = std::chrono::hours{24};

/// Interval between two disk scanning cycles.
inline constexpr std::chrono::seconds disk_scan_interval
  = std::chrono::minutes{1};

/// Number of partitions to remove before re-checking disk size.
inline constexpr size_t disk_monitor_step_size = 1;

/// Maximum number of events per INDEX partition.
inline constexpr size_t max_partition_size = 4'194'304; // 4 Mi

/// Timeout after which an active partition is forcibly flushed.
inline constexpr caf::timespan active_partition_timeout
  = std::chrono::seconds{30};

/// Timeout after which a new automatic rebuild is triggered.
inline constexpr caf::timespan rebuild_interval = std::chrono::minutes{120};

/// Maximum number of in-memory INDEX partitions.
inline constexpr size_t max_in_mem_partitions = 1;

/// Number of immediately scheduled INDEX partitions.
inline constexpr size_t taste_partitions = 5;

/// Maximum number of concurrent INDEX queries.
inline constexpr size_t num_query_supervisors = 10;

/// The store backend to use.
inline constexpr const char* store_backend = "feather";

/// Rate at which telemetry data is sent to the ACCOUNTANT.
inline constexpr std::chrono::milliseconds telemetry_rate
  = std::chrono::milliseconds{10000};

/// The timeout for the cascading requests of 'tenzir status' in seconds.
inline constexpr std::chrono::milliseconds status_request_timeout
  = std::chrono::seconds{10};

/// Timeout for initial connections to the node.
inline constexpr std::chrono::milliseconds node_connection_timeout
  = std::chrono::minutes{5};

/// Timeout for the scheduler to give up on a partition.
inline constexpr std::chrono::milliseconds scheduler_timeout
  = std::chrono::minutes{15};

/// The period to wait until a shutdown sequence finishes cleanly. After the
/// elapses, the shutdown procedure escalates into a "hard kill".
/// @relates shutdown_kill_timeout
inline constexpr std::chrono::milliseconds shutdown_grace_period
  = std::chrono::minutes{3};

/// Time to wait until receiving a DOWN from a killed actor.
/// @relates shutdown_grace_period
inline constexpr std::chrono::seconds shutdown_kill_timeout
  = std::chrono::minutes{1};

/// The allowed false positive rate for a synopsis.
inline constexpr double fp_rate = 0.01;

/// Flag that enables creation of partition indexes in the database.
inline constexpr bool create_partition_index = true;

/// Time to wait before trying to make another connection attempt to a remote
/// Tenzir node.
inline constexpr auto node_connection_retry_delay = std::chrono::seconds{3u};

/// The time interval for sending metrics of the currently running pipeline
/// operator.
inline constexpr auto metrics_interval = std::chrono::seconds{1};

} // namespace tenzir::defaults
