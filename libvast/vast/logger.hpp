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

#include "vast/config.hpp"
#include "vast/detail/discard.hpp"
#include "vast/error.hpp"
// Please note this mapping was specified
// VAST_INFO -> spdlog::info
// VAST_VERBOSE -> spdlog::debug
// VAST_DEBUG -> spdlog::trace
// VAST_TRACE -> spdlog::trace

#if VAST_LOG_LEVEL == VAST_LOG_LEVEL_TRACE
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#elif VAST_LOG_LEVEL == VAST_LOG_LEVEL_DEBUG
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#elif VAST_LOG_LEVEL == VAST_LOG_LEVEL_VERBOSE
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#elif VAST_LOG_LEVEL == VAST_LOG_LEVEL_INFO
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
#elif VAST_LOG_LEVEL == VAST_LOG_LEVEL_WARNING
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_WARN
#elif VAST_LOG_LEVEL == VAST_LOG_LEVEL_ERROR
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_ERROR
#elif VAST_LOG_LEVEL == VAST_LOG_LEVEL_CRITICAL
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_CRITICAL
#elif VAST_LOG_LEVEL == VAST_LOG_LEVEL_QUIET
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_OFF
#endif

// Important: keep that below the log level mapping
#include "vast/detail/logger.hpp"
#include "vast/detail/logger_formatters.hpp"

#define VAST_LOG_SPD_TRACE(...)                                                \
  SPDLOG_LOGGER_TRACE(vast::detail::logger(), __VA_ARGS__)
#define VAST_LOG_SPD_DEBUG(...)                                                \
  SPDLOG_LOGGER_DEBUG(vast::detail::logger(), __VA_ARGS__)
#define VAST_LOG_SPD_VERBOSE(...)                                              \
  SPDLOG_LOGGER_DEBUG(vast::detail::logger(), __VA_ARGS__)
#define VAST_LOG_SPD_INFO(...)                                                 \
  SPDLOG_LOGGER_INFO(vast::detail::logger(), __VA_ARGS__)
#define VAST_LOG_SPD_WARN(...)                                                 \
  SPDLOG_LOGGER_WARN(vast::detail::logger(), __VA_ARGS__)
#define VAST_LOG_SPD_ERROR(...)                                                \
  SPDLOG_LOGGER_ERROR(vast::detail::logger(), __VA_ARGS__)
#define VAST_LOG_SPD_CRITICAL(...)                                             \
  SPDLOG_LOGGER_CRITICAL(vast::detail::logger(), __VA_ARGS__)

// -------

namespace vast {

/// Converts a verbosity atom to its integer counterpart. For unknown atoms,
/// the `default_value` parameter will be returned.
/// Used to make log level strings from config, like 'debug', to a log level int
int loglevel_to_int(caf::atom_value ll, int default_value
                                        = VAST_LOG_LEVEL_QUIET);

[[nodiscard]] caf::expected<caf::detail::scope_guard<void (*)()>>
create_log_context(const vast::invocation& cmd_invocation,
                   const caf::settings& cfg_file);

} // namespace vast

// -- VAST logging macros ------------------------------------------------------

#if defined(VAST_LOG_LEVEL)

#  if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_TRACE

#    define VAST_TRACE(...)                                                    \
      SPDLOG_LOGGER_TRACE(                                                     \
        vast::detail::logger(),                                                \
        vast::detail::spd_msg_from_args(1, 2, __VA_ARGS__).str(), "ENTER",     \
        __func__, __VA_ARGS__);                                                \
      auto CAF_UNIFYN(vast_log_trace_guard_)                                   \
        = ::caf::detail::make_scope_guard([=, func_name_ = __func__] {         \
            SPDLOG_LOGGER_TRACE(vast::detail::logger(), "EXIT {}",             \
                                func_name_);                                   \
          })

#  else // VAST_LOG_LEVEL > VAST_LOG_LEVEL_TRACE

#    define VAST_TRACE(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  endif // VAST_LOG_LEVEL > VAST_LOG_LEVEL_TRACE

#  define VAST_ERROR(c, ...)                                                   \
    SPDLOG_LOGGER_DEBUG(vast::detail::logger(),                                \
                        vast::detail::spd_msg_from_args(c, __VA_ARGS__).str(), \
                        ::vast::detail::id_or_name(c), __VA_ARGS__)

#  define VAST_ERROR_ANON(...)                                                 \
    SPDLOG_LOGGER_DEBUG(vast::detail::logger(),                                \
                        vast::detail::spd_msg_from_args(__VA_ARGS__).str(),    \
                        __VA_ARGS__)

#  define VAST_WARNING(c, ...)                                                 \
    SPDLOG_LOGGER_WARN(vast::detail::logger(),                                 \
                       vast::detail::spd_msg_from_args(c, __VA_ARGS__).str(),  \
                       ::vast::detail::id_or_name(c), __VA_ARGS__)

#  define VAST_WARNING_ANON(...)                                               \
    SPDLOG_LOGGER_WARN(vast::detail::logger(),                                 \
                       vast::detail::spd_msg_from_args(__VA_ARGS__).str(),     \
                       __VA_ARGS__)

#  define VAST_INFO(c, ...)                                                    \
    SPDLOG_LOGGER_INFO(vast::detail::logger(),                                 \
                       vast::detail::spd_msg_from_args(c, __VA_ARGS__).str(),  \
                       ::vast::detail::id_or_name(c), __VA_ARGS__)

#  define VAST_INFO_ANON(...)                                                  \
    SPDLOG_LOGGER_INFO(vast::detail::logger(),                                 \
                       vast::detail::spd_msg_from_args(__VA_ARGS__).str(),     \
                       __VA_ARGS__)

#  define VAST_VERBOSE(c, ...)                                                 \
    SPDLOG_LOGGER_DEBUG(vast::detail::logger(),                                \
                        vast::detail::spd_msg_from_args(c, __VA_ARGS__).str(), \
                        ::vast::detail::id_or_name(c), __VA_ARGS__)

#  define VAST_VERBOSE_ANON(...)                                               \
    SPDLOG_LOGGER_DEBUG(vast::detail::logger(),                                \
                        vast::detail::spd_msg_from_args(__VA_ARGS__).str(),    \
                        __VA_ARGS__)

#  define VAST_DEBUG(c, ...)                                                   \
    SPDLOG_LOGGER_TRACE(vast::detail::logger(),                                \
                        vast::detail::spd_msg_from_args(c, __VA_ARGS__).str(), \
                        ::vast::detail::id_or_name(c), __VA_ARGS__)

#  define VAST_DEBUG_ANON(...)                                                 \
    SPDLOG_LOGGER_TRACE(vast::detail::logger(),                                \
                        vast::detail::spd_msg_from_args(__VA_ARGS__).str(),    \
                        __VA_ARGS__)

#  define VAST_CRITICAL(c, ...)                                                \
    SPDLOG_LOGGER_CRITICAL(                                                    \
      vast::detail::logger(),                                                  \
      vast::detail::spd_msg_from_args(c, __VA_ARGS__).str(),                   \
      ::vast::detail::id_or_name(c), __VA_ARGS__)

#  define VAST_CRITICAL_ANON(...)                                              \
    SPDLOG_LOGGER_CRITICAL(vast::detail::logger(),                             \
                           vast::detail::spd_msg_from_args(__VA_ARGS__).str(), \
                           __VA_ARGS__)
#else // defined(VAST_LOG_LEVEL)

#  define VAST_LOG(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  define VAST_LOG_COMPONENT(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  define VAST_TRACE(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  define VAST_ERROR(...) VAST_DISCARD_ARGS(__VA_ARGS__)
#  define VAST_ERROR_ANON(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  define VAST_WARNING(...) VAST_DISCARD_ARGS(__VA_ARGS__)
#  define VAST_WARNING_ANON(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  define VAST_INFO(...) VAST_DISCARD_ARGS(__VA_ARGS__)
#  define VAST_INFO_ANON(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  define VAST_VERBOSE(...) VAST_DISCARD_ARGS(__VA_ARGS__)
#  define VAST_VERBOSE_ANON(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#  define VAST_DEBUG(...) VAST_DISCARD_ARGS(__VA_ARGS__)
#  define VAST_DEBUG_ANON(...) VAST_DISCARD_ARGS(__VA_ARGS__)

#endif // defined(VAST_LOG_LEVEL)

// -- VAST_ARG utility for formatting log output -------------------------------

#define VAST_ARG_1(x) vast::detail::make_arg_wrapper(#x, x)

#define VAST_ARG_2(x_name, x) vast::detail::make_arg_wrapper(x_name, x)

#define VAST_ARG_3(x_name, first, last)                                        \
  vast::detail::make_arg_wrapper(x_name, first, last)

/// Nicely formats a variable or argument. For example, `VAST_ARG(foo)`
/// generates `foo = ...` in log output, where `...` is the content of the
/// variable. `VAST_ARG("size", xs.size())` generates the output
/// `size = xs.size()`.
#define VAST_ARG(...) VAST_PP_OVERLOAD(VAST_ARG_, __VA_ARGS__)(__VA_ARGS__)
