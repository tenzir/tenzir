//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/logger.hpp"

#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/settings.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/configuration.hpp"
#include "vast/systemd.hpp"

#include <caf/local_actor.hpp>
#include <spdlog/async.h>
#include <spdlog/common.h>
#include <spdlog/sinks/ansicolor_sink.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/syslog_sink.h>

#include <cctype>

#if VAST_ENABLE_JOURNALD_LOGGING
#  include <spdlog/sinks/systemd_sink.h>
#endif

#include <cassert>
#include <memory>

namespace vast {

caf::expected<caf::detail::scope_guard<void (*)()>>
create_log_context(const vast::invocation& cmd_invocation,
                   const caf::settings& cfg_file) {
  if (!vast::detail::setup_spdlog(cmd_invocation, cfg_file))
    return caf::make_error(vast::ec::unspecified);
  return {caf::detail::make_scope_guard(
    std::addressof(vast::detail::shutdown_spdlog))};
}

/// Convert a log level to an int.
/// @note x is passed by value because it is modified.
int loglevel_to_int(std::string x, int default_value) {
  for (auto& ch : x)
    ch = std::tolower(ch);
  if (x == "quiet")
    return VAST_LOG_LEVEL_QUIET;
  if (x == "error")
    return VAST_LOG_LEVEL_ERROR;
  if (x == "warning")
    return VAST_LOG_LEVEL_WARNING;
  if (x == "info")
    return VAST_LOG_LEVEL_INFO;
  if (x == "verbose")
    return VAST_LOG_LEVEL_VERBOSE;
  if (x == "debug")
    return VAST_LOG_LEVEL_DEBUG;
  if (x == "trace")
    return VAST_LOG_LEVEL_TRACE;
  return default_value;
}

namespace {

constexpr bool is_vast_loglevel(const int value) {
  switch (value) {
    case VAST_LOG_LEVEL_QUIET:
    case VAST_LOG_LEVEL_ERROR:
    case VAST_LOG_LEVEL_WARNING:
    case VAST_LOG_LEVEL_INFO:
    case VAST_LOG_LEVEL_VERBOSE:
    case VAST_LOG_LEVEL_DEBUG:
    case VAST_LOG_LEVEL_TRACE:
      return true;
  }
  return false;
}

/// Converts a vast log level to spdlog level
spdlog::level::level_enum vast_loglevel_to_spd(const int value) {
  VAST_ASSERT(is_vast_loglevel(value));
  spdlog::level::level_enum level = spdlog::level::off;
  switch (value) {
    case VAST_LOG_LEVEL_QUIET:
      break;
    case VAST_LOG_LEVEL_ERROR:
      level = spdlog::level::err;
      break;
    case VAST_LOG_LEVEL_WARNING:
      level = spdlog::level::warn;
      break;
    case VAST_LOG_LEVEL_INFO:
      level = spdlog::level::info;
      break;
    case VAST_LOG_LEVEL_VERBOSE:
      level = spdlog::level::debug;
      break;
    case VAST_LOG_LEVEL_DEBUG:
      level = spdlog::level::trace;
      break;
    case VAST_LOG_LEVEL_TRACE:
      level = spdlog::level::trace;
      break;
  }
  return level;
}

} // namespace

namespace detail {

bool setup_spdlog(const vast::invocation& cmd_invocation,
                  const caf::settings& cfg_file) try {
  if (vast::detail::logger()->name() != "/dev/null") {
    VAST_ERROR("Log already up");
    return false;
  }
  bool is_server = cmd_invocation.full_name == "start"
                   || caf::get_or(cmd_invocation.options, "vast.node", false);
  const auto& cfg_cmd = cmd_invocation.options;
  std::string console_verbosity = vast::defaults::logger::console_verbosity;
  auto cfg_console_verbosity
    = caf::get_if<std::string>(&cfg_file, "vast.console-verbosity");
  if (cfg_console_verbosity) {
    if (loglevel_to_int(*cfg_console_verbosity, -1) < 0) {
      fmt::print(stderr,
                 "failed to start logger; vast.console-verbosity '{}' is "
                 "invalid\n",
                 *cfg_console_verbosity);
      return false;
    } else {
      console_verbosity = *cfg_console_verbosity;
    }
  }
  // Allow `vast.verbosity` from the command-line to overwrite
  // the `vast.console-verbosity` setting from the config file.
  auto verbosity = caf::get_if<std::string>(&cfg_cmd, "vast.verbosity");
  if (verbosity) {
    if (loglevel_to_int(*verbosity, -1) < 0) {
      fmt::print(stderr,
                 "failed to start logger; vast.verbosity '{}' is invalid\n",
                 *verbosity);
      return false;
    }
    console_verbosity = *verbosity;
  }
  std::string file_verbosity = vast::defaults::logger::file_verbosity;
  auto cfg_file_verbosity
    = caf::get_if<std::string>(&cfg_file, "vast.file-verbosity");
  if (cfg_file_verbosity) {
    if (loglevel_to_int(*cfg_file_verbosity, -1) < 0) {
      fmt::print(
        stderr, "failed to start logger; vast.file-verbosity '{}' is invalid\n",
        *cfg_file_verbosity);
      return false;
    } else {
      file_verbosity = *cfg_file_verbosity;
    }
  }
  auto vast_file_verbosity = loglevel_to_int(file_verbosity);
  auto vast_console_verbosity = loglevel_to_int(console_verbosity);
  auto vast_verbosity = std::max(vast_file_verbosity, vast_console_verbosity);
  // Helper to set the color mode
  spdlog::color_mode log_color = [&]() -> spdlog::color_mode {
    auto config_value = caf::get_or(cfg_file, "vast.console", "automatic");
    if (config_value == "automatic")
      return spdlog::color_mode::automatic;
    if (config_value == "always")
      return spdlog::color_mode::always;

    return spdlog::color_mode::never;
  }();
  auto log_file = caf::get_or(cfg_file, "vast.log-file",
                              std::string{defaults::logger::log_file});
  auto cmdline_log_file = caf::get_if<std::string>(&cfg_cmd, "vast.log-file");
  if (cmdline_log_file)
    log_file = *cmdline_log_file;
  if (is_server) {
    if (log_file == defaults::logger::log_file
        && vast_file_verbosity != VAST_LOG_LEVEL_QUIET) {
      std::filesystem::path log_dir = caf::get_or(
        cfg_file, "vast.db-directory", defaults::system::db_directory);
      std::error_code err{};
      if (!std::filesystem::exists(log_dir, err)) {
        const auto created_log_dir
          = std::filesystem::create_directory(log_dir, err);
        if (!created_log_dir) {
          fmt::print(stderr,
                     "failed to start logger; unable to create directory {}: "
                     "{}\n",
                     log_dir, err.message());
          return false;
        }
      }
      log_file = (log_dir / log_file).string();
    }
  } else {
    // Please note, client file does not go to db_directory!
    auto client_log_file
      = caf::get_if<std::string>(&cfg_cmd, "vast.client-log-file");
    if (!client_log_file)
      client_log_file
        = caf::get_if<std::string>(&cfg_file, "vast.client-log-file");
    if (client_log_file)
      log_file = *client_log_file;
    else // If there is no client log file, turn off file logging
      vast_file_verbosity = VAST_LOG_LEVEL_QUIET;
  }
  auto queue_size = caf::get_or(cfg_file, "vast.log-queue-size",
                                defaults::logger::queue_size);
  spdlog::init_thread_pool(queue_size, defaults::logger::logger_threads);
  std::vector<spdlog::sink_ptr> sinks;
  // Add console sink.
  std::string default_sink_type
    = VAST_ENABLE_JOURNALD_LOGGING && systemd::connected_to_journal()
        ? "journald"
        : "stderr";
  auto sink_type
    = caf::get_or(cfg_file, "vast.console-sink", default_sink_type);
  auto console_sink = [&]() -> spdlog::sink_ptr {
    if (sink_type == "stderr") {
      auto stderr_sink
        = std::make_shared<spdlog::sinks::ansicolor_stderr_sink_mt>(log_color);
      return stderr_sink;
    } else if (sink_type == "journald") {
#if !VAST_ENABLE_JOURNALD_LOGGING
      fmt::print(stderr, "failed to start logger; vast.console-sink 'journald' "
                         "required VAST built with systemd support\n");
      return nullptr;
#else
      auto spdlog_sink = std::make_shared<spdlog::sinks::systemd_sink_mt>();
      return std::static_pointer_cast<spdlog::sinks::sink>(spdlog_sink);
#endif
    } else if (sink_type == "syslog") {
      auto syslog_sink = std::make_shared<spdlog::sinks::syslog_sink_mt>(
        "vast", /*options = */ 0, LOG_USER, /*enable_formatting = */ true);
      return std::static_pointer_cast<spdlog::sinks::sink>(syslog_sink);
    } else {
      fmt::print(stderr,
                 "failed to start logger; vast.console-sink '{}' is invalid "
                 "(expected 'stderr', 'journald', or 'syslog')\n",
                 sink_type);
    }
    return nullptr;
  }();
  if (!console_sink)
    return false;
  auto console_format
    = caf::get_or(cfg_file, "vast.console-format",
                  std::string{defaults::logger::console_format});
  console_sink->set_pattern(console_format);
  console_sink->set_level(vast_loglevel_to_spd(vast_console_verbosity));
  sinks.push_back(console_sink);
  // Add file sink.
  if (vast_file_verbosity != VAST_LOG_LEVEL_QUIET) {
    bool disable_rotation = caf::get_or(cfg_file, "vast.disable-log-rotation",
                                        defaults::logger::disable_log_rotation);
    spdlog::sink_ptr file_sink = nullptr;
    if (!disable_rotation) {
      auto threshold_str
        = detail::get_bytesize(cfg_file, "vast.log-rotation-threshold",
                               defaults::logger::rotate_threshold);
      if (!threshold_str) {
        fmt::print(stderr,
                   "failed to start logger; vast.log-rotation-threshold is "
                   "invalid: {}\n",
                   threshold_str.error());
        return false;
      }
      file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        log_file, *threshold_str, defaults::logger::rotate_files);
    } else {
      file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file);
    }
    file_sink->set_level(vast_loglevel_to_spd(vast_file_verbosity));
    auto file_format = caf::get_or(cfg_file, "vast.file-format",
                                   std::string{defaults::logger::file_format});
    file_sink->set_pattern(file_format);
    sinks.push_back(file_sink);
  }
  // Replace the /dev/null logger that was created during init.
  logger() = std::make_shared<spdlog::async_logger>(
    "vast", sinks.begin(), sinks.end(), spdlog::thread_pool(),
    spdlog::async_overflow_policy::block);
  logger()->set_level(vast_loglevel_to_spd(vast_verbosity));
  spdlog::register_logger(logger());
  return true;
} catch (const spdlog::spdlog_ex& err) {
  std::cerr << err.what() << "\n";
  return false;
}

void shutdown_spdlog() {
  VAST_DEBUG("shut down logging");
  spdlog::shutdown();
}

std::shared_ptr<spdlog::logger>& logger() {
  static std::shared_ptr<spdlog::logger> vast_logger
    = spdlog::async_factory::template create<spdlog::sinks::null_sink_mt>(
      "/dev/null");
  return vast_logger;
}

} // namespace detail
} // namespace vast
