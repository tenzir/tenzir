//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp"
#include "tenzir/detail/discard.hpp"
#include "tenzir/error.hpp"

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

using argument_map = detail::stack_vector<std::pair<const char*, tenzir::data>, stack_element_count>;

static std::atomic_int runtime_log_level = 0;

std::vector<log_sink> sinks;

void emit(const char* msg, argument_map& data) {
    for (auto& sink : sinks)
        sink.handle_log_msg(msg, data);
}

struct structured_log_msg {
    ~structured_log_msg() {
        TENZIR_WARN("{} {}", msg_, as_flat_string())
        // emit(msg_, data_);
    }

    structured_log_msg(log_level, const char* msg) {
        msg_ = msg;
    }

    template<typename T>
    auto arg(std::string_view msg, T&& data) -> structured_log_msg& {
        data_.emplace_back({std::move(msg), std::move(data)})
        return *this;
    }

private:
    void as_flat_string() {
      auto s = fmt::format("{}", fmt::join(data_ | std::views::transform([](const auto& p) {
        return fmt::format("{}={}", p.first, p.second);
      }), " "));
    }

    static constexpr size_t stack_element_count = 10;

    const char* msg_;
    argument_map data_;
};

struct log_sink {
  virtual void handle(structured_log_msg const& msg) = 0; 
};

struct console_log_sink : public log_sink {
  void handle(structured_log_msg const& msg) override {
    TENZIR_WARN("{} {}", msg_, as_flat_string())
  }
};


#define TENZIR_STRUCTURED_WARN(msg) \
    if(TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_WARNING)
        structured_log_msg(log_level::warn, msg)


// # Old log message:
// TENZIR_DEBUG("{} got {} new hits for predicate at position {}", *self,
//                rank(result), position);

// # New log message
TENZIR_WARN("evaluator: new hits for predicate")
    .arg("actor_id", self->id())
    .arg("num", rank(result))
    .arg("position", position);

// -> Terminal log:
//   evaluator: new hits for predicate actor_id=23 num=100 position=2
// -> log operator
//   record{
//     ts: time
//     msg: "evaluator: new hits for predicate"
//     args: '{"actor_id": "23", "num": "100", "position": "2"}'
//   }
// -> cloudwatch sink
//   (...fully structured log format...)

// Important: keep that below the log level mapping
#include "tenzir/detail/logger.hpp"
#include "tenzir/detail/logger_formatters.hpp"

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
    auto CAF_UNIFYN(tenzir_log_trace_guard_)                                   \
      = [func_name_ = static_cast<const char*>(__func__)](                     \
          const std::string& format, auto&&... args) {                         \
          TENZIR_DEBUG(TENZIR_FMT_RUNTIME("ENTER {} " + format), __func__,     \
                       std::forward<decltype(args)>(args)...);                 \
          return ::caf::detail::make_scope_guard([func_name_] {                \
            TENZIR_DEBUG("EXIT {}", func_name_);                               \
          });                                                                  \
        }(__VA_ARGS__);

#  define TENZIR_TRACE(...)                                                    \
    SPDLOG_LOGGER_TRACE(::tenzir::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_TRACE

#  define TENZIR_TRACE_SCOPE(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#  define TENZIR_TRACE(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_TRACE

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_DEBUG

#  define TENZIR_DEBUG(...)                                                    \
    SPDLOG_LOGGER_TRACE(::tenzir::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_DEBUG

#  define TENZIR_DEBUG(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_DEBUG

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_VERBOSE

#  define TENZIR_VERBOSE(...)                                                  \
    SPDLOG_LOGGER_DEBUG(::tenzir::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_VERBOSE

#  define TENZIR_VERBOSE(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_VERBOSE

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_INFO

#  define TENZIR_INFO(...)                                                     \
    SPDLOG_LOGGER_INFO(::tenzir::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_INFO

#  define TENZIR_INFO(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_INFO

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_WARNING

#  define TENZIR_WARN(...)                                                     \
    SPDLOG_LOGGER_WARN(::tenzir::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_WARNING

#  define TENZIR_WARN(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_WARNING

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_ERROR

#  define TENZIR_ERROR(...)                                                    \
    SPDLOG_LOGGER_ERROR(::tenzir::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_ERROR

#  define TENZIR_ERROR(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_ERROR

#if TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_CRITICAL

#  define TENZIR_CRITICAL(...)                                                 \
    SPDLOG_LOGGER_CRITICAL(::tenzir::detail::logger(), __VA_ARGS__)

#else // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_CRITICAL

#  define TENZIR_CRITICAL(...) TENZIR_DISCARD_ARGS(__VA_ARGS__)

#endif // TENZIR_LOG_LEVEL < TENZIR_LOG_LEVEL_CRITICAL

namespace tenzir {

/// Converts a verbosity to its integer counterpart. For unknown values,
/// the `default_value` parameter will be returned.
/// Used to make log level strings from config, like 'debug', to a log level int.
int loglevel_to_int(std::string c, int default_value = TENZIR_LOG_LEVEL_QUIET);

[[nodiscard]] caf::expected<caf::detail::scope_guard<void (*)()>>
create_log_context(bool is_server, const tenzir::invocation& cmd_invocation,
                   const caf::settings& cfg_file);

} // namespace tenzir

// -- TENZIR_ARG utility for formatting log output -------------------------------

// NOLINTNEXTLINE
#define TENZIR_ARG_1(x) ::tenzir::detail::make_arg_wrapper(#x, x)

// NOLINTNEXTLINE
#define TENZIR_ARG_2(x_name, x) ::tenzir::detail::make_arg_wrapper(x_name, x)

// NOLINTNEXTLINE
#define TENZIR_ARG_3(x_name, first, last)                                      \
  ::tenzir::detail::make_arg_wrapper(x_name, first, last)

/// Nicely formats a variable or argument. For example, `TENZIR_ARG(foo)`
/// generates `foo = ...` in log output, where `...` is the content of the
/// variable. `TENZIR_ARG("size", xs.size())` generates the output
/// `size = xs.size()`.
// NOLINTNEXTLINE
#define TENZIR_ARG(...)                                                        \
  TENZIR_PP_OVERLOAD(TENZIR_ARG_, __VA_ARGS__)(__VA_ARGS__)
