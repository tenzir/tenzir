//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/config.hpp"
#include "vast/detail/discard.hpp"
#include "vast/error.hpp"

#include <string>
#include <utility>

// TENZIR_INFO -> spdlog::info
// TENZIR_VERBOSE -> spdlog::debug
// TENZIR_DEBUG -> spdlog::trace
// TENZIR_TRACE_SCOPE -> spdlog::trace

#if TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_TRACE
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#elif TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_DEBUG
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#elif TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_VERBOSE
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#elif TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_INFO
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
#elif TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_WARNING
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_WARN
#elif TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_ERROR
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_ERROR
#elif TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_CRITICAL
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_CRITICAL
#elif TENZIR_LOG_LEVEL == TENZIR_LOG_LEVEL_QUIET
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_OFF
#endif

// Important: keep that below the log level mapping
#include "vast/detail/logger.hpp"
#include "vast/detail/logger_formatters.hpp"

// fmt::runtime is a function that selectively disables compile-time format
// string evaluation. The TENZIR_FMT_RUNTIME macro wraps it for backwards
// compatibility with {fmt} 7.x. The macro must be used for format strings that
// cannot be known at compile-time.
#if FMT_VERSION >= 80000 // {fmt} 8+
#  define TENZIR_FMT_RUNTIME(...) ::fmt::runtime(__VA_ARGS__)
#else
#  define TENZIR_FMT_RUNTIME(...) (__VA_ARGS__)
#endif

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_TRACE

// A debugging macro that emits an additional log statement when leaving the
// current scope.
#  define TENZIR_TRACE_SCOPE(...)                                              \
    /* NOLINTNEXTLINE */                                                       \
    auto CAF_UNIFYN(vast_log_trace_guard_)                                     \
      = [func_name_ = static_cast<const char*>(__func__)](                     \
          const std::string& format, auto&&... args) {                         \
          TENZIR_DEBUG(TENZIR_FMT_RUNTIME("ENTER {} " + format), __func__,     \
                       std::forward<decltype(args)>(args)...);                 \
          return ::caf::detail::make_scope_guard([func_name_] {                \
            TENZIR_DEBUG("EXIT {}", func_name_);                               \
          });                                                                  \
        }(__VA_ARGS__);

#  define TENZIR_TRACE(...)                                                    \
    SPDLOG_LOGGER_TRACE(::vast::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_TRACE

#  define TENZIR_TRACE_SCOPE(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#  define TENZIR_TRACE(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_TRACE

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_DEBUG

#  define TENZIR_DEBUG(...)                                                    \
    SPDLOG_LOGGER_TRACE(::vast::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_DEBUG

#  define TENZIR_DEBUG(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_DEBUG

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_VERBOSE

#  define TENZIR_VERBOSE(...)                                                  \
    SPDLOG_LOGGER_DEBUG(::vast::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_VERBOSE

#  define TENZIR_VERBOSE(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_VERBOSE

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_INFO

#  define TENZIR_INFO(...)                                                     \
    SPDLOG_LOGGER_INFO(::vast::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_INFO

#  define TENZIR_INFO(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_INFO

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_WARNING

#  define TENZIR_WARN(...)                                                     \
    SPDLOG_LOGGER_WARN(::vast::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_WARNING

#  define TENZIR_WARN(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_WARNING

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_ERROR

#  define TENZIR_ERROR(...)                                                    \
    SPDLOG_LOGGER_ERROR(::vast::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_ERROR

#  define TENZIR_ERROR(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_ERROR

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_CRITICAL

#  define TENZIR_CRITICAL(...)                                                 \
    SPDLOG_LOGGER_CRITICAL(::vast::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_CRITICAL

#  define TENZIR_CRITICAL(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_CRITICAL

namespace vast {

/// Converts a verbosity to its integer counterpart. For unknown values,
/// the `default_value` parameter will be returned.
/// Used to make log level strings from config, like 'debug', to a log level int.
int loglevel_to_int(std::string c, int default_value = TENZIR_LOG_LEVEL_QUIET);

[[nodiscard]] caf::expected<caf::detail::scope_guard<void (*)()>>
create_log_context(const vast::invocation& cmd_invocation,
                   const caf::settings& cfg_file);

} // namespace vast

// -- TENZIR_ARG utility for formatting log output -------------------------------

// NOLINTNEXTLINE
#define TENZIR_ARG_1(x) ::vast::detail::make_arg_wrapper(#x, x)

// NOLINTNEXTLINE
#define TENZIR_ARG_2(x_name, x) ::vast::detail::make_arg_wrapper(x_name, x)

// NOLINTNEXTLINE
#define TENZIR_ARG_3(x_name, first, last)                                      \
  ::vast::detail::make_arg_wrapper(x_name, first, last)

/// Nicely formats a variable or argument. For example, `TENZIR_ARG(foo)`
/// generates `foo = ...` in log output, where `...` is the content of the
/// variable. `TENZIR_ARG("size", xs.size())` generates the output
/// `size = xs.size()`.
// NOLINTNEXTLINE
#define TENZIR_ARG(...)                                                        \
  TENZIR_PP_OVERLOAD(TENZIR_ARG_, __VA_ARGS__)(__VA_ARGS__)
