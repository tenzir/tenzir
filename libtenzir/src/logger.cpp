//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/logger.hpp"

#include "tenzir/command.hpp"
#include "tenzir/config.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/systemd.hpp"

#include <caf/local_actor.hpp>
#include <spdlog/async.h>
#include <spdlog/common.h>
#include <spdlog/sinks/ansicolor_sink.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/syslog_sink.h>

#include <cctype>

#if TENZIR_ENABLE_JOURNALD_LOGGING
#  include <spdlog/sinks/systemd_sink.h>
#endif

#include <cassert>
#include <memory>

namespace tenzir {

caf::expected<caf::detail::scope_guard<void (*)()>>
create_log_context(bool is_server, const tenzir::invocation& cmd_invocation,
                   const caf::settings& cfg_file) {
  if (!tenzir::detail::setup_spdlog(is_server, cmd_invocation, cfg_file))
    return caf::make_error(tenzir::ec::unspecified);
  return {caf::detail::make_scope_guard(
    std::addressof(tenzir::detail::shutdown_spdlog))};
}

/// Convert a log level to an int.
/// @note x is passed by value because it is modified.
int loglevel_to_int(std::string x, int default_value) {
  for (auto& ch : x)
    ch = std::tolower(ch);
  if (x == "quiet")
    return TENZIR_LOG_LEVEL_QUIET;
  if (x == "error")
    return TENZIR_LOG_LEVEL_ERROR;
  if (x == "warning")
    return TENZIR_LOG_LEVEL_WARNING;
  if (x == "info")
    return TENZIR_LOG_LEVEL_INFO;
  if (x == "verbose")
    return TENZIR_LOG_LEVEL_VERBOSE;
  if (x == "debug")
    return TENZIR_LOG_LEVEL_DEBUG;
  if (x == "trace")
    return TENZIR_LOG_LEVEL_TRACE;
  return default_value;
}

namespace {

/// Converts a tenzir log level to spdlog level
spdlog::level::level_enum tenzir_loglevel_to_spd(const int value) {
  spdlog::level::level_enum level = spdlog::level::off;
  switch (value) {
    case TENZIR_LOG_LEVEL_QUIET:
      break;
    case TENZIR_LOG_LEVEL_ERROR:
      level = spdlog::level::err;
      break;
    case TENZIR_LOG_LEVEL_WARNING:
      level = spdlog::level::warn;
      break;
    case TENZIR_LOG_LEVEL_INFO:
      level = spdlog::level::info;
      break;
    case TENZIR_LOG_LEVEL_VERBOSE:
      level = spdlog::level::debug;
      break;
    case TENZIR_LOG_LEVEL_DEBUG:
      level = spdlog::level::trace;
      break;
    case TENZIR_LOG_LEVEL_TRACE:
      level = spdlog::level::trace;
      break;
    default:
      TENZIR_ASSERT(false, "unhandled log level");
  }
  return level;
}

} // namespace

namespace detail {

bool setup_spdlog(bool is_server, const tenzir::invocation& cmd_invocation,
                  const caf::settings& cfg_file) try {
  if (tenzir::detail::logger()->name() != "/dev/null") {
    TENZIR_ERROR("Log already up");
    return false;
  }
  const auto& cfg_cmd = cmd_invocation.options;
  std::string console_verbosity = tenzir::defaults::logger::console_verbosity;
  auto cfg_console_verbosity
    = caf::get_if<std::string>(&cfg_file, "tenzir.console-verbosity");
  if (cfg_console_verbosity) {
    if (loglevel_to_int(*cfg_console_verbosity, -1) < 0) {
      fmt::print(stderr,
                 "failed to start logger; tenzir.console-verbosity '{}' is "
                 "invalid\n",
                 *cfg_console_verbosity);
      return false;
    } else {
      console_verbosity = *cfg_console_verbosity;
    }
  }
  std::string file_verbosity = tenzir::defaults::logger::file_verbosity;
  auto cfg_file_verbosity
    = caf::get_if<std::string>(&cfg_file, "tenzir.file-verbosity");
  if (cfg_file_verbosity) {
    if (loglevel_to_int(*cfg_file_verbosity, -1) < 0) {
      fmt::print(stderr,
                 "failed to start logger; tenzir.file-verbosity '{}' is "
                 "invalid\n",
                 *cfg_file_verbosity);
      return false;
    } else {
      file_verbosity = *cfg_file_verbosity;
    }
  }
  auto tenzir_file_verbosity = loglevel_to_int(file_verbosity);
  auto tenzir_console_verbosity = loglevel_to_int(console_verbosity);
  auto tenzir_verbosity
    = std::max(tenzir_file_verbosity, tenzir_console_verbosity);
  // Helper to set the color mode
  spdlog::color_mode log_color = [&]() -> spdlog::color_mode {
    auto config_value = caf::get_or(cfg_file, "tenzir.console", "automatic");
    if (config_value == "automatic")
      return spdlog::color_mode::automatic;
    if (config_value == "always")
      return spdlog::color_mode::always;

    return spdlog::color_mode::never;
  }();
  auto log_file = caf::get_or(cfg_file, "tenzir.log-file",
                              std::string{defaults::logger::log_file});
  auto cmdline_log_file = caf::get_if<std::string>(&cfg_cmd, "tenzir.log-file");
  if (cmdline_log_file)
    log_file = *cmdline_log_file;
  if (is_server) {
    if (log_file == defaults::logger::log_file
        && tenzir_file_verbosity != TENZIR_LOG_LEVEL_QUIET) {
      std::filesystem::path log_dir = caf::get_or(
        cfg_file, "tenzir.state-directory", defaults::state_directory.data());
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
    // Please note, client file does not go to state_directory!
    auto client_log_file
      = caf::get_if<std::string>(&cfg_cmd, "tenzir.client-log-file");
    if (!client_log_file)
      client_log_file
        = caf::get_if<std::string>(&cfg_file, "tenzir.client-log-file");
    if (client_log_file)
      log_file = *client_log_file;
    else // If there is no client log file, turn off file logging
      tenzir_file_verbosity = TENZIR_LOG_LEVEL_QUIET;
  }
  auto default_queue_size = is_server ? defaults::logger::server_queue_size
                                      : defaults::logger::client_queue_size;
  auto queue_size
    = caf::get_or(cfg_file, "tenzir.log-queue-size", default_queue_size);
  spdlog::init_thread_pool(queue_size, defaults::logger::logger_threads);
  std::vector<spdlog::sink_ptr> sinks;
  // Add console sink.
  std::string default_sink_type
    = TENZIR_ENABLE_JOURNALD_LOGGING && systemd::connected_to_journal()
        ? "journald"
        : "stderr";
  auto sink_type
    = caf::get_or(cfg_file, "tenzir.console-sink", default_sink_type);
  auto console_sink = [&]() -> spdlog::sink_ptr {
    if (sink_type == "stderr") {
      auto stderr_sink
        = std::make_shared<spdlog::sinks::ansicolor_stderr_sink_mt>(log_color);
      return stderr_sink;
    } else if (sink_type == "journald") {
#if !TENZIR_ENABLE_JOURNALD_LOGGING
      fmt::print(stderr,
                 "failed to start logger; tenzir.console-sink 'journald' "
                 "required Tenzir built with systemd support\n");
      return nullptr;
#else
      auto spdlog_sink = std::make_shared<spdlog::sinks::systemd_sink_mt>();
      return std::static_pointer_cast<spdlog::sinks::sink>(spdlog_sink);
#endif
    } else if (sink_type == "syslog") {
      auto syslog_sink = std::make_shared<spdlog::sinks::syslog_sink_mt>(
        "tenzir", /*options = */ 0, LOG_USER, /*enable_formatting = */ true);
      return std::static_pointer_cast<spdlog::sinks::sink>(syslog_sink);
    } else {
      fmt::print(stderr,
                 "failed to start logger; tenzir.console-sink '{}' is invalid "
                 "(expected 'stderr', 'journald', or 'syslog')\n",
                 sink_type);
    }
    return nullptr;
  }();
  if (!console_sink)
    return false;
  auto console_format
    = caf::get_or(cfg_file, "tenzir.console-format",
                  std::string{defaults::logger::console_format});
  console_sink->set_pattern(console_format);
  console_sink->set_level(tenzir_loglevel_to_spd(tenzir_console_verbosity));
  sinks.push_back(console_sink);
  // Add file sink.
  if (tenzir_file_verbosity != TENZIR_LOG_LEVEL_QUIET) {
    bool disable_rotation = caf::get_or(cfg_file, "tenzir.disable-log-rotation",
                                        defaults::logger::disable_log_rotation);
    spdlog::sink_ptr file_sink = nullptr;
    if (!disable_rotation) {
      auto threshold_str
        = detail::get_bytesize(cfg_file, "tenzir.log-rotation-threshold",
                               defaults::logger::rotate_threshold);
      if (!threshold_str) {
        fmt::print(stderr,
                   "failed to start logger; tenzir.log-rotation-threshold is "
                   "invalid: {}\n",
                   threshold_str.error());
        return false;
      }
      file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        log_file, *threshold_str, defaults::logger::rotate_files);
    } else {
      file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file);
    }
    file_sink->set_level(tenzir_loglevel_to_spd(tenzir_file_verbosity));
    auto file_format = caf::get_or(cfg_file, "tenzir.file-format",
                                   std::string{defaults::logger::file_format});
    file_sink->set_pattern(file_format);
    sinks.push_back(file_sink);
  }
  // Replace the /dev/null logger that was created during init.
  logger() = std::make_shared<spdlog::async_logger>(
    "tenzir", sinks.begin(), sinks.end(), spdlog::thread_pool(),
    spdlog::async_overflow_policy::block);
  logger()->set_level(tenzir_loglevel_to_spd(tenzir_verbosity));
  spdlog::register_logger(logger());
  return true;
} catch (const spdlog::spdlog_ex& err) {
  std::cerr << err.what() << "\n";
  return false;
}

void shutdown_spdlog() {
  TENZIR_DEBUG("shut down logging");
  spdlog::shutdown();
}

std::shared_ptr<spdlog::logger>& logger() {
  static std::shared_ptr<spdlog::logger> tenzir_logger
    = spdlog::async_factory::template create<spdlog::sinks::null_sink_mt>(
      "/dev/null");
  return tenzir_logger;
}

} // namespace detail
} // namespace tenzir
