//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/logger.hpp"

#include "tenzir/command.hpp"
#include "tenzir/config.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/option.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/systemd.hpp"

#include <caf/settings.hpp>
#include <spdlog/async.h>
#include <spdlog/common.h>
#include <spdlog/sinks/ansicolor_sink.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/syslog_sink.h>

#include <cctype>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string_view>
#include <vector>

#if TENZIR_ENABLE_JOURNALD_LOGGING
#  include <spdlog/sinks/systemd_sink.h>
#endif

namespace tenzir {

caf::expected<detail::scope_guard<void (*)() noexcept>>
create_log_context(bool is_server, const tenzir::invocation& cmd_invocation,
                   const caf::settings& cfg_file) {
  if (detail::setup_spdlog(is_server, cmd_invocation, cfg_file)) {
    return detail::scope_guard{&detail::shutdown_spdlog};
  }
  return caf::make_error(ec::unspecified);
}

/// Convert a log level to an int.
/// @note x is passed by value because it is modified.
int loglevel_to_int(std::string x, int default_value) {
  for (auto& ch : x) {
    ch = std::tolower(ch);
  }
  if (x == "quiet") {
    return TENZIR_LOG_LEVEL_QUIET;
  }
  if (x == "error") {
    return TENZIR_LOG_LEVEL_ERROR;
  }
  if (x == "warning") {
    return TENZIR_LOG_LEVEL_WARNING;
  }
  if (x == "info") {
    return TENZIR_LOG_LEVEL_INFO;
  }
  if (x == "verbose") {
    return TENZIR_LOG_LEVEL_VERBOSE;
  }
  if (x == "debug") {
    return TENZIR_LOG_LEVEL_DEBUG;
  }
  if (x == "trace") {
    return TENZIR_LOG_LEVEL_TRACE;
  }
  return default_value;
}

namespace {

/// The name of the placeholder logger that discards everything until
/// `setup_spdlog` replaces it.
constexpr auto discarding_logger_name = std::string_view{"/dev/null"};

/// Maps a Tenzir log level onto the spdlog level that the logging macros in
/// `tenzir/logger.hpp` use for it.
auto to_spdlog_level(int tenzir_level) -> spdlog::level::level_enum {
  switch (tenzir_level) {
    case TENZIR_LOG_LEVEL_QUIET:
      return spdlog::level::off;
    case TENZIR_LOG_LEVEL_ERROR:
      return spdlog::level::err;
    case TENZIR_LOG_LEVEL_WARNING:
      return spdlog::level::warn;
    case TENZIR_LOG_LEVEL_INFO:
      return spdlog::level::info;
    case TENZIR_LOG_LEVEL_VERBOSE:
      return spdlog::level::debug;
    case TENZIR_LOG_LEVEL_DEBUG:
    case TENZIR_LOG_LEVEL_TRACE:
      return spdlog::level::trace;
    default:
      TENZIR_ASSERT(false, "unhandled log level");
      return spdlog::level::off;
  }
}

/// Reads the verbosity option `key` from the configuration file and converts
/// it into a Tenzir log level, using `fallback` when the option is absent.
/// Returns `None` after reporting an unknown level name.
auto configured_level(const caf::settings& cfg_file, std::string_view key,
                      std::string_view fallback) -> Option<int> {
  const auto* configured = caf::get_if<std::string>(&cfg_file, key);
  if (not configured) {
    return loglevel_to_int(std::string{fallback});
  }
  auto level = loglevel_to_int(*configured, -1);
  if (level < 0) {
    fmt::print(stderr, "failed to start logger; {} '{}' is invalid\n", key,
               *configured);
    return None{};
  }
  return level;
}

/// Translates the `tenzir.console` option into an spdlog color mode. Values
/// other than `automatic` and `always` disable colors.
auto configured_color_mode(const caf::settings& cfg_file)
  -> spdlog::color_mode {
  auto mode = caf::get_or(cfg_file, "tenzir.console", "automatic");
  if (mode == "always") {
    return spdlog::color_mode::always;
  }
  if (mode == "automatic") {
    return spdlog::color_mode::automatic;
  }
  return spdlog::color_mode::never;
}

/// Looks up a string option, letting the command line take precedence over
/// the configuration file.
auto lookup_option(const caf::settings& cfg_cmd, const caf::settings& cfg_file,
                   std::string_view key) -> Option<std::string> {
  for (const auto* settings : {&cfg_cmd, &cfg_file}) {
    if (const auto* value = caf::get_if<std::string>(settings, key)) {
      return *value;
    }
  }
  return None{};
}

/// Places a server's default log file in its state directory, creating the
/// directory if needed. Returns `None` after reporting a failure.
auto server_log_file(const caf::settings& cfg_file, std::string log_file,
                     bool file_logging_enabled) -> Option<std::string> {
  if (log_file != defaults::logger::log_file or not file_logging_enabled) {
    return log_file;
  }
  auto state_dir = std::filesystem::path{caf::get_or(
    cfg_file, "tenzir.state-directory", defaults::state_directory.data())};
  auto err = std::error_code{};
  if (not std::filesystem::exists(state_dir, err)
      and not std::filesystem::create_directory(state_dir, err)) {
    fmt::print(stderr,
               "failed to start logger; unable to create directory {}: {}\n",
               state_dir, err.message());
    return None{};
  }
  return (state_dir / log_file).string();
}

} // namespace

namespace detail {

bool setup_spdlog(bool is_server, const tenzir::invocation& cmd_invocation,
                  const caf::settings& cfg_file) try {
  if (logger()->name() != discarding_logger_name) {
    TENZIR_ERROR("Log already up");
    return false;
  }
  const auto& cfg_cmd = cmd_invocation.options;
  auto console_level = configured_level(cfg_file, "tenzir.console-verbosity",
                                        defaults::logger::console_verbosity);
  auto file_level = configured_level(cfg_file, "tenzir.file-verbosity",
                                     defaults::logger::file_verbosity);
  if (not console_level or not file_level) {
    return false;
  }
  // The logger itself must let through everything that any sink wants, so we
  // compute its level before the client case below may disable the file sink.
  auto logger_level = std::max(*console_level, *file_level);
  auto log_file = std::string{};
  if (is_server) {
    auto configured = caf::get_or(cfg_file, "tenzir.log-file",
                                  std::string{defaults::logger::log_file});
    if (const auto* from_cmd
        = caf::get_if<std::string>(&cfg_cmd, "tenzir.log-file")) {
      configured = *from_cmd;
    }
    auto resolved = server_log_file(cfg_file, std::move(configured),
                                    *file_level != TENZIR_LOG_LEVEL_QUIET);
    if (not resolved) {
      return false;
    }
    log_file = std::move(*resolved);
  } else if (auto client_file
             = lookup_option(cfg_cmd, cfg_file, "tenzir.client-log-file")) {
    // Clients write their log file relative to the working directory, not to
    // the state directory.
    log_file = std::move(*client_file);
  } else {
    // Clients only log to a file when explicitly asked to.
    file_level = TENZIR_LOG_LEVEL_QUIET;
  }
  auto default_queue_size = is_server ? defaults::logger::server_queue_size
                                      : defaults::logger::client_queue_size;
  auto queue_size
    = caf::get_or(cfg_file, "tenzir.log-queue-size", default_queue_size);
  spdlog::init_thread_pool(queue_size, defaults::logger::logger_threads);
  auto sinks = std::vector<spdlog::sink_ptr>{};
  // Add console sink.
  std::string default_sink_type
    = TENZIR_ENABLE_JOURNALD_LOGGING and systemd::connected_to_journal()
        ? "journald"
        : "stderr";
  auto sink_type
    = caf::get_or(cfg_file, "tenzir.console-sink", default_sink_type);
  auto console_sink = [&]() -> spdlog::sink_ptr {
    if (sink_type == "stderr") {
      return std::make_shared<spdlog::sinks::ansicolor_stderr_sink_mt>(
        configured_color_mode(cfg_file));
    } else if (sink_type == "journald") {
#if ! TENZIR_ENABLE_JOURNALD_LOGGING
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
  if (not console_sink) {
    return false;
  }
  auto console_format
    = caf::get_or(cfg_file, "tenzir.console-format",
                  std::string{defaults::logger::console_format});
  console_sink->set_pattern(console_format);
  console_sink->set_level(to_spdlog_level(*console_level));
  sinks.push_back(std::move(console_sink));
  // Add file sink.
  if (*file_level != TENZIR_LOG_LEVEL_QUIET) {
    bool disable_rotation = caf::get_or(cfg_file, "tenzir.disable-log-rotation",
                                        defaults::logger::disable_log_rotation);
    spdlog::sink_ptr file_sink = nullptr;
    if (not disable_rotation) {
      auto threshold_str
        = detail::get_bytesize(cfg_file, "tenzir.log-rotation-threshold",
                               defaults::logger::rotate_threshold);
      if (not threshold_str) {
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
    file_sink->set_level(to_spdlog_level(*file_level));
    file_sink->set_pattern(
      caf::get_or(cfg_file, "tenzir.file-format",
                  std::string{defaults::logger::file_format}));
    sinks.push_back(std::move(file_sink));
  }
  auto overflow_policy = caf::get_or(cfg_file, "tenzir.log-overflow-policy",
                                     defaults::logger::overflow_policy);
  if (overflow_policy != "block" and overflow_policy != "overrun_oldest") {
    fmt::print(stderr, "failed to start logger; invalid value for "
                       "tenzir.log-overflow-policy");
    return false;
  }
  auto spdlog_overflow_policy
    = overflow_policy == "block"
        ? spdlog::async_overflow_policy::block
        : spdlog::async_overflow_policy::overrun_oldest;
  // Swap out the discarding placeholder logger.
  logger()
    = std::make_shared<spdlog::async_logger>("tenzir", sinks.begin(),
                                             sinks.end(), spdlog::thread_pool(),
                                             spdlog_overflow_policy);
  logger()->set_level(to_spdlog_level(logger_level));
  spdlog::register_logger(logger());
  return true;
} catch (const spdlog::spdlog_ex& err) {
  std::cerr << err.what() << "\n";
  return false;
}

void shutdown_spdlog() noexcept {
  TENZIR_DEBUG("shut down logging");
  // The asynchronous logger owns a worker thread, which this joins after
  // flushing all pending messages.
  spdlog::shutdown();
}

std::shared_ptr<spdlog::logger>& logger() {
  // Log messages emitted before `setup_spdlog` runs go nowhere.
  static auto instance = std::shared_ptr<spdlog::logger>{
    spdlog::async_factory::create<spdlog::sinks::null_sink_mt>(
      std::string{discarding_logger_name}),
  };
  return instance;
}

} // namespace detail
} // namespace tenzir
