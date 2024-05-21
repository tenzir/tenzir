//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/discard.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/detail/stack_vector.hpp"
#include "tenzir/detail/string_literal.hpp"
#include "tenzir/error.hpp"

#include <caf/actor.hpp>
#include <caf/fwd.hpp>

#include <concepts>
#include <cstddef>
#include <iostream>
#include <source_location>
#include <sstream>
#include <string>
#include <thread>
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

namespace tenzir::logger {
constexpr static size_t stack_field_count = 10;
using argument_field_type = std::pair<std::string_view, ::tenzir::data>;
using argument_map_type
  = detail::stack_vector<argument_field_type,
                         stack_field_count * sizeof(argument_field_type)>;

static std::atomic_int runtime_log_level = 0;

TENZIR_ENUM(level, debug, verbose, info, warning, error, critical);

struct structured_message {
  friend void emit(const structured_message&);

  ~structured_message() {
    emit(*this);
  }

  structured_message() = default;

  template <size_t N>
  structured_message(level lvl, detail::string_literal<N> message,
                     std::source_location sloc, std::thread::id tid,
                     caf::actor_id aid)
    : level_{lvl}, message_{message}, location_{sloc}, tid_{tid}, aid_{aid} {
  }

  template <size_t N, typename T>
    requires std::convertible_to<T, tenzir::data>
  auto
  field(detail::string_literal<N> field_name, T&& data) -> structured_message& {
    data_.emplace_back(field_name, std::forward<T>(data));
    return *this;
  }

  level level_;
  std::string_view message_;
  std::source_location location_;
  std::thread::id tid_;
  caf::actor_id aid_;
  argument_map_type data_;
};
} // namespace tenzir::logger

template <>
struct fmt::formatter<std::thread::id> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const std::thread::id& loc, FormatContext& ctx) const {
    std::ostringstream oss;
    oss << loc;

    return fmt::format_to(ctx.out(), "{}", std::move(oss).str());
  }
};

template <>
struct fmt::formatter<std::source_location> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const std::source_location& loc, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{}:{}:{}", loc.file_name(),
                          loc.function_name(), loc.line());
  }
};

template <>
struct fmt::formatter<tenzir::logger::structured_message> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const tenzir::logger::structured_message& msg,
              FormatContext& ctx) const {
    auto out = ctx.out();
    out = fmt::format_to(out, "{{ {} : {} }}", "level", msg.level_);
    out = fmt::format_to(out, "{{ {} : {} }}", "message", msg.message_);
    out = fmt::format_to(out, "{{ {} : {} }}", "location", msg.location_);
    out = fmt::format_to(out, "{{ {} : {} }}", "thread_id", msg.tid_);
    out = fmt::format_to(out, "{{ {} : {} }}", "actor_id", msg.aid_);

    for (const auto& f : msg.data_) {
      out = fmt::format_to(out, "{{ {} : {} }} ", f.first, f.second);
    }

    return out;
  }
};

namespace tenzir::logger {
struct sink {
  std::atomic<level> level_;
  virtual void handle(structured_message const& msg) = 0;
};

extern std::vector<sink*> sinks;
} // namespace tenzir::logger

#define TENZIR_STRUCTURED_WARN(msg)                                            \
  if (TENZIR_LOG_LEVEL >= TENZIR_LOG_LEVEL_WARNING)                            \
    tenzir::logger::structured_message {                                       \
      tenzir::logger::level::warn, msg, std::source_location::current(),       \
        std::this_thread::get_id(), caf::actor_id {                            \
      }                                                                        \
    }

// # Old log message:
// TENZIR_DEBUG("{} got {} new hits for predicate at position {}", *self,
//                rank(result), position);

// # New log message
// TENZIR_WARN("evaluator: new hits for predicate")
//     .field("actor_id", self->id())
//     .field("num", rank(result))
//     .field("position", position);

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
