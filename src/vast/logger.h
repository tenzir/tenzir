#ifndef VAST_LOGGER_H
#define VAST_LOGGER_H

#include <sstream>

#include "vast/config.h"
#include "vast/singleton.h"
#include "vast/util/pp.h"

// Defines vast::operator<<, which must be declared prior to the call site.
#include "vast/concept/printable/stream.h"

namespace vast {

/// A simple singleton logger and tracer.
class logger : public singleton<logger> {
  friend class singleton<logger>;

public:
  enum level : int {
    quiet = 0,
    error = 1,
    warn = 2,
    info = 3,
    verbose = 4,
    debug = 5,
    trace = 6
  };

  /// A formatted message containing timestamp and thread ID.
  class message {
    friend logger;

  public:
    /// Constructs a message with a certain level.
    /// @param lvl The level of the message.
    message(level lvl = quiet);

    message(message const& other);
    message(message&&) = default;

    /// Sets thread ID and timestamp.
    void coin();

    /// Sets the function.
    /// @param The value of `__PRETTY_FUNCTION__`.
    void function(char const* f);

    template <typename T>
    friend message& operator<<(message& m, T const& x) {
      m.ss_ << x;
      return m;
    }

    void clear();

    bool empty();

    level lvl() const;

    double timestamp() const;

    std::string const& thread_id() const;

    std::string const& context() const;

    std::string const& function() const;

    std::string msg() const;

  private:
    level lvl_ = quiet;
    double timestamp_ = 0.0;
    std::string thread_id_;
    std::string context_;
    std::string function_;
    std::stringstream ss_;

    friend message& operator<<(message& msg, std::nullptr_t);
  };

  /// Facilitates RAII-style tracing.
  class tracer {
  public:
    enum fill_type { left_arrow, right_arrow, bar };

    tracer(message&& msg);
    ~tracer();

    template <typename T>
    friend tracer& operator<<(tracer& t, T&& x) {
      t.msg_ << std::forward<T>(x);
      return t;
    }

    void fill(fill_type t);
    void commit();
    void reset(bool exit);

  private:
    message msg_;
  };

  /// Initializes the file backend.
  /// @param verbosity The log level to filter messages.
  /// @param filename The path of the log file.
  /// @returns `true` on success.
  static bool file(level verbosity, std::string const& filename = "");

  /// Initializes the console backend.
  /// @param verbosity The log level to filter messages.
  /// @param colorized Whether to colorize console messages.
  /// @returns `true` on success.
  static bool console(level verbosity, bool colorized = true);

  /// Logs a record.
  /// @param msg The log message.
  static void log(message msg);

  /// Checks whether the logger takes a given level.
  /// @param lvl The level to check.
  /// @param `true` if the logger takes at least *lvl*.
  static bool takes(level lvl);

  /// Constructs a message which accepts arbitrary values via `operator<<`.
  /// @param lvl The log level.
  /// @param ctx The log context/facility.
  /// @param fun The caller function, typically `__PRETTY_FUNCTION__`.
  static message make_message(level lvl, std::string ctx, char const* fun);

private:
  /// Implementation of the logger. We use PIMPL here to reduce the footprint
  /// of the header file, as the logger constitutes a heavily used component.
  struct impl;

  /// Default-constructs a logger.
  logger() = default;

  // Singleton implementation.
  static logger* create();
  void initialize();
  void destroy();
  void dispose();

  void run();

  std::unique_ptr<impl> impl_;
};

std::ostream& operator<<(std::ostream& stream, logger::level lvl);

} // namespace vast

#define VAST_VOID static_cast<void>(0)

#ifndef VAST_LOG_CONTEXT
// Before including this header file, users can set a file-global log context
// by defining VAST_LOG_CONTEXT. This makes it possible to use VAST_XXX(..)
// instead of VAST_XXX_AT(ctx, ...) with a repetitive first argument *ctx*.
#define VAST_LOG_CONTEXT ""
#endif

#define VAST_LOG_CTX(lvl, ctx, msg)                                            \
  do {                                                                         \
    if (::vast::logger::takes(lvl)) {                                          \
      std::ostringstream __vast_ctx;                                           \
      __vast_ctx << ctx;                                                       \
      auto __vast_msg = ::vast::logger::make_message(lvl, __vast_ctx.str(),    \
                                                     __PRETTY_FUNCTION__);     \
      __vast_msg << msg;                                                       \
      ::vast::logger::log(std::move(__vast_msg));                              \
    }                                                                          \
  } while (false)

#define VAST_LOG(lvl, msg)                                                     \
  VAST_LOG_CTX(lvl, VAST_LOG_CONTEXT, msg)

#define VAST_LOG_CTX_MSG_3(lvl, ctx, m1) VAST_LOG_CTX(lvl, ctx, m1)
#define VAST_LOG_CTX_MSG_4(lvl, ctx, m1, m2)                                   \
  VAST_LOG_CTX_MSG_3(lvl, ctx, m1 << ' ' << m2)
#define VAST_LOG_CTX_MSG_5(lvl, ctx, m1, m2, m3)                               \
  VAST_LOG_CTX_MSG_4(lvl, ctx, m1, m2 << ' ' << m3)
#define VAST_LOG_CTX_MSG_6(lvl, ctx, m1, m2, m3, m4)                           \
  VAST_LOG_CTX_MSG_5(lvl, ctx, m1, m2, m3 << ' ' << m4)
#define VAST_LOG_CTX_MSG_7(lvl, ctx, m1, m2, m3, m4, m5)                       \
  VAST_LOG_CTX_MSG_6(lvl, ctx, m1, m2, m3, m4 << ' ' << m5)
#define VAST_LOG_CTX_MSG_8(lvl, ctx, m1, m2, m3, m4, m5, m6)                   \
  VAST_LOG_CTX_MSG_7(lvl, ctx, m1, m2, m3, m4, m5 << ' ' << m6)
#define VAST_LOG_CTX_MSG_9(lvl, ctx, m1, m2, m3, m4, m5, m6, m7)               \
  VAST_LOG_CTX_MSG_8(lvl, ctx, m1, m2, m3, m4, m5, m6 << ' ' << m7)
#define VAST_LOG_CTX_MSG_10(lvl, ctx, m1, m2, m3, m4, m5, m6, m7, m8)          \
  VAST_LOG_CTX_MSG_9(lvl, ctx, m1, m2, m3, m4, m5, m6, m7 << ' ' << m8)
#define VAST_LOG_CTX_MSG_11(lvl, ctx, m1, m2, m3, m4, m5, m6, m7, m8, m9)      \
  VAST_LOG_CTX_MSG_10(lvl, ctx, m1, m2, m3, m4, m5, m6, m7, m8 << ' ' << m9)
#define VAST_LOG_CTX_MSG(...)                                                  \
  VAST_PP_OVERLOAD(VAST_LOG_CTX_MSG_, __VA_ARGS__)(__VA_ARGS__)

#define VAST_LOG_MSG_2(lvl, m1) VAST_LOG(lvl, m1)
#define VAST_LOG_MSG_3(lvl, m1, m2) VAST_LOG_MSG_2(lvl, m1 << ' ' << m2)
#define VAST_LOG_MSG_4(lvl, m1, m2, m3) VAST_LOG_MSG_3(lvl, m1, m2 << ' ' << m3)
#define VAST_LOG_MSG_5(lvl, m1, m2, m3, m4)                                    \
  VAST_LOG_MSG_4(lvl, m1, m2, m3 << ' ' << m4)
#define VAST_LOG_MSG_6(lvl, m1, m2, m3, m4, m5)                                \
  VAST_LOG_MSG_5(lvl, m1, m2, m3, m4 << ' ' << m5)
#define VAST_LOG_MSG_7(lvl, m1, m2, m3, m4, m5, m6)                            \
  VAST_LOG_MSG_6(lvl, m1, m2, m3, m4, m5 << ' ' << m6)
#define VAST_LOG_MSG_8(lvl, m1, m2, m3, m4, m5, m6, m7)                        \
  VAST_LOG_MSG_7(lvl, m1, m2, m3, m4, m5, m6 << ' ' << m7)
#define VAST_LOG_MSG_9(lvl, m1, m2, m3, m4, m5, m6, m7, m8)                    \
  VAST_LOG_MSG_8(lvl, m1, m2, m3, m4, m5, m6, m7 << ' ' << m8)
#define VAST_LOG_MSG_10(lvl, m1, m2, m3, m4, m5, m6, m7, m8, m9)               \
  VAST_LOG_MSG_9(lvl, m1, m2, m3, m4, m5, m6, m7, m8 << ' ' << m9)
#define VAST_LOG_MSG(...)                                                      \
  VAST_PP_OVERLOAD(VAST_LOG_MSG_, __VA_ARGS__)(__VA_ARGS__)

#define VAST_LOG_LEVEL_QUIET 0
#define VAST_LOG_LEVEL_ERROR 1
#define VAST_LOG_LEVEL_WARN 2
#define VAST_LOG_LEVEL_INFO 3
#define VAST_LOG_LEVEL_VERBOSE 4
#define VAST_LOG_LEVEL_DEBUG 5
#define VAST_LOG_LEVEL_TRACE 6

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_ERROR
#define VAST_ERROR(...) VAST_LOG_MSG(::vast::logger::error, __VA_ARGS__)
#define VAST_ERROR_AT(ctx, ...)                                                \
  VAST_LOG_CTX_MSG(::vast::logger::error, ctx, __VA_ARGS__)
#else
#define VAST_ERROR(...) VAST_VOID
#define VAST_ERROR_AT(...) VAST_VOID
#endif

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_WARN
#define VAST_WARN(...) VAST_LOG_MSG(::vast::logger::warn, __VA_ARGS__)
#define VAST_WARN_AT(ctx, ...)                                                 \
  VAST_LOG_CTX_MSG(::vast::logger::warn, ctx, __VA_ARGS__)
#else
#define VAST_WARN(...) VAST_VOID
#define VAST_WARN_AT(...) VAST_VOID
#endif

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
#define VAST_INFO(...) VAST_LOG_MSG(::vast::logger::info, __VA_ARGS__)
#define VAST_INFO_AT(ctx, ...)                                                 \
  VAST_LOG_CTX_MSG(::vast::logger::info, ctx, __VA_ARGS__)
#else
#define VAST_INFO(...) VAST_VOID
#define VAST_INFO_AT(...) VAST_VOID
#endif

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_VERBOSE
#define VAST_VERBOSE(...) VAST_LOG_MSG(::vast::logger::verbose, __VA_ARGS__)
#define VAST_VERBOSE_AT(ctx, ...)                                              \
  VAST_LOG_CTX_MSG(::vast::logger::verbose, ctx, __VA_ARGS__)
#else
#define VAST_VERBOSE(...) VAST_VOID
#define VAST_VERBOSE_AT(...) VAST_VOID
#endif

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_DEBUG
#define VAST_DEBUG(...) VAST_LOG_MSG(::vast::logger::debug, __VA_ARGS__)
#define VAST_DEBUG_AT(ctx, ...)                                                \
  VAST_LOG_CTX_MSG(::vast::logger::debug, ctx, __VA_ARGS__)
#else
#define VAST_DEBUG(...) VAST_VOID
#define VAST_DEBUG_AT(...) VAST_VOID
#endif

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_TRACE
#define VAST_TRACE(...) VAST_LOG_MSG(::vast::logger::trace, __VA_ARGS__)
#define VAST_TRACE_AT(ctx, ...)                                                \
  VAST_LOG_CTX_MSG(::vast::logger::trace, ctx, __VA_ARGS__)
#else
#define VAST_TRACE(...) VAST_VOID
#define VAST_TRACE_AT(...) VAST_VOID
#endif

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_TRACE
#define VAST_ARGS_ENTER(args) "--> (" << args << ')'
#define VAST_ARGS_LEAVE(args) "<-- (" << args << ')'
#define VAST_ENTER_ARGS(args, msg)                                             \
  auto __vast_msg = ::vast::logger::make_message(                              \
    ::vast::logger::level::trace, VAST_LOG_CONTEXT, __PRETTY_FUNCTION__);      \
  ::vast::logger::tracer __vast_tracer{std::move(__vast_msg)};                 \
  __vast_tracer << VAST_ARGS_ENTER(args) << msg;                               \
  __vast_tracer.commit()
#define VAST_ENTER() VAST_ENTER_ARGS('*', "")
#define VAST_ENTER_MSG(msg) VAST_ENTER_ARGS('*', ' ' << msg)
#define VAST_ENTER_WITH_1(args) VAST_ENTER_ARGS(args, "")
#define VAST_ENTER_WITH_2(args, msg) VAST_ENTER_ARGS(args, " " << msg)
#define VAST_ENTER_WITH(...)                                                   \
  VAST_PP_OVERLOAD(VAST_ENTER_WITH_, __VA_ARGS__)(__VA_ARGS__)
#define VAST_ARG_1(A) #A << " = " << A
#define VAST_ARG_2(A, B) VAST_ARG_1(A) << ", " << #B << " = " << B
#define VAST_ARG_3(A, B, C) VAST_ARG_2(A, B) << ", " << #C << " = " << C
#define VAST_ARG_4(A, B, C, D) VAST_ARG_3(A, B, C) << ", " << #D << " = " << D
#define VAST_ARG_5(A, B, C, D, E)                                              \
  VAST_ARG_4(A, B, C, D) << ", " << #E << " = " << E
#define VAST_ARG(...) VAST_PP_OVERLOAD(VAST_ARG_, __VA_ARGS__)(__VA_ARGS__)
#define VAST_ARGF(arg, f) #arg << " = " << f(arg)
#define VAST_ARGM(arg, m) #arg << " = " << arg.m()
#define VAST_THIS "*this = " << *this
#define VAST_MSG(msg)                                                          \
  __vast_tracer.reset(false);                                                  \
  __vast_tracer << msg;                                                        \
  __vast_tracer.commit()
#define VAST_LEAVE(msg)                                                        \
  {                                                                            \
    __vast_tracer.reset(true);                                                 \
    __vast_tracer << VAST_ARGS_LEAVE("void") << ' ' << msg;                    \
    return;                                                                    \
  }                                                                            \
  VAST_VOID
#define VAST_RETURN_VAL_MSG(value, msg)                                        \
  {                                                                            \
    auto&& vast_result = value;                                                \
    __vast_tracer.reset(true);                                                 \
    __vast_tracer << VAST_ARGS_LEAVE(vast_result) << ' ' << msg;               \
    return vast_result;                                                        \
  }                                                                            \
  VAST_VOID
#define VAST_RETURN_1(val) VAST_RETURN_VAL_MSG(val, "")
#define VAST_RETURN_2(val, msg) VAST_RETURN_VAL_MSG(val, msg)
#define VAST_RETURN(...)                                                       \
  VAST_PP_OVERLOAD(VAST_RETURN_, __VA_ARGS__)(__VA_ARGS__)
#else
#define VAST_ENTER() VAST_VOID
#define VAST_ENTER_MSG() VAST_VOID
#define VAST_ENTER_WITH(...) VAST_VOID
#define VAST_MSG(...) VAST_VOID
#define VAST_LEAVE(...) return
#define VAST_RETURN_1(val) return val
#define VAST_RETURN_2(val, msg) return val
#define VAST_RETURN(...)                                                       \
  VAST_PP_OVERLOAD(VAST_RETURN_, __VA_ARGS__)(__VA_ARGS__)
#define VAST_ARG(...)
#define VAST_ARGF
#define VAST_ARGM
#define VAST_THIS
#endif

#endif
