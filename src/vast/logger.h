#ifndef VAST_LOGGER_H
#define VAST_LOGGER_H

#include <sstream>
#include "vast/config.h"
#include "vast/singleton.h"
#include "vast/print.h"
#include "vast/util/pp.h"

namespace vast {

class path;

/// A simple singleton logger and tracer.
class logger : public singleton<logger>
{
  friend class singleton<logger>;

public:
  enum level : uint32_t
  {
    quiet     = 0,
    error     = 1,
    warn      = 2,
    info      = 3,
    verbose   = 4,
    debug     = 5,
    trace     = 6
  };

  /// Attempts to parse a textual representation of a log-level.
  /// @param str The string holding the level description
  /// @returns An engaged trial on success.
  static trial<level> parse_level(std::string const& str);

  /// A formatted message containing timestamp and thread ID.
  class message
  {
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
    friend message& operator<<(message& m, T&& x)
    {
      m.ss_ << std::forward<T>(x);
      return m;
    }

    void clear();

    bool empty();

    level lvl() const;

    double timestamp() const;

    std::string const& thread_id() const;

    std::string const& facility() const;

    std::string const& function() const;

    std::string msg() const;

  private:
    level lvl_ = quiet;
    double timestamp_ = 0.0;
    std::string thread_id_;
    std::string facility_;
    std::string function_;
    std::ostringstream ss_;

    friend message& operator<<(message& msg, std::nullptr_t);
  };

  /// Facilitates RAII-style tracing.
  class tracer
  {
  public:
    enum fill_type
    {
      left_arrow,
      right_arrow,
      bar
    };

    tracer(message&& msg);
    ~tracer();

    template <typename T>
    friend tracer& operator<<(tracer& t, T&& x)
    {
      t.msg_ << std::forward<T>(x);
      return t;
    }

    void fill(fill_type t);
    void commit();
    void reset(bool exit);

  private:
    message msg_;
  };

  ~logger();

  /// Initializes the logger.
  /// This function must be called once prior to using any logging macro.
  /// @param console The log level of the console.
  /// @param colors Whether to use colors in the console output.
  /// @param file The log level of the logfile.
  /// @param show_fns Whether to print function names in the output.
  /// @param dir The directory to create the log file in.
  /// @returns `true` on success.
  bool init(level console, level file, bool colors, bool show_fns, path dir);

  /// Logs a record.
  /// @param msg The log message.
  void log(message msg);

  /// Checks whether the logger takes a given level.
  /// @param lvl The level to check.
  /// @param `true` if the logger takes at least *lvl*.
  bool takes(level lvl) const;

  /// Constructs a message which accepts arbitrary values via `operator<<`.
  /// @param lvl The log level.
  /// @param facility The facility or component.
  /// @param fun The caller function, typically `__PRETTY_FUNCTION__`.
  message make_message(level lvl, char const* facility, char const* fun) const;

private:
  /// Implementation of the logger. We use PIMPL here to reduce the footprint
  /// of the header file, as the logger constitutes a heavily used component.
  struct impl;

  /// Default-constructs a logger.
  logger();

  // Singleton implementation.
  static logger* create();
  void initialize();
  void destroy();
  void dispose();

  void run();

  impl* impl_;
  bool show_fns_;

  friend std::ostream& operator<<(std::ostream& stream, logger::level lvl);
};

} // namespace vast

#define VAST_VOID static_cast<void>(0)

#ifndef VAST_LOG_FACILITY
#  define VAST_LOG_FACILITY "\0"
#endif

#define VAST_LOG(lvl, msg)                                                    \
  do                                                                          \
  {                                                                           \
    if (::vast::logger::instance()->takes(lvl))                               \
    {                                                                         \
      auto m = ::vast::logger::instance()->make_message(                      \
          lvl, VAST_LOG_FACILITY, __PRETTY_FUNCTION__);                       \
      m << msg;                                                               \
      ::vast::logger::instance()->log(std::move(m));                          \
    }                                                                         \
  }                                                                           \
  while (false)

#define VAST_THIS_ACTOR(name) \
  *this << ":" << name

#if VAST_LOG_LEVEL > 0
#  define VAST_LOG_ERROR(message)     VAST_LOG(::vast::logger::error, message)
#  define VAST_LOG_ACTOR_ERROR_2(name, message)                               \
     VAST_LOG(::vast::logger::error, VAST_THIS_ACTOR(name) << ' ' << message)
#  define VAST_LOG_ACTOR_ERROR_1(message)                                     \
     VAST_LOG_ACTOR_ERROR_2(this->describe(), message)
#  define VAST_LOG_ACTOR_ERROR(...)   VAST_PP_OVERLOAD(VAST_LOG_ACTOR_ERROR_, \
                                              __VA_ARGS__)(__VA_ARGS__)
#else
#  define VAST_LOG_ERROR(message)     VAST_VOID
#  define VAST_LOG_ACTOR_ERROR(...)   VAST_VOID
#endif
#if VAST_LOG_LEVEL > 1
#  define VAST_LOG_WARN(message)      VAST_LOG(::vast::logger::warn, message)
#  define VAST_LOG_ACTOR_WARN_2(name, message)                                \
     VAST_LOG(::vast::logger::warn, VAST_THIS_ACTOR(name) << ' ' << message)
#  define VAST_LOG_ACTOR_WARN_1(message)                                      \
     VAST_LOG_ACTOR_WARN_2(this->describe(), message)
#  define VAST_LOG_ACTOR_WARN(...)    VAST_PP_OVERLOAD(VAST_LOG_ACTOR_WARN_,  \
                                              __VA_ARGS__)(__VA_ARGS__)
#else
#  define VAST_LOG_WARN(message)      VAST_VOID
#  define VAST_LOG_ACTOR_WARN(...)    VAST_VOID
#endif
#if VAST_LOG_LEVEL > 2
#  define VAST_LOG_INFO(message)     VAST_LOG(::vast::logger::info, message)
#  define VAST_LOG_ACTOR_INFO_2(name, message)                               \
     VAST_LOG(::vast::logger::info, VAST_THIS_ACTOR(name) << ' ' << message)
#  define VAST_LOG_ACTOR_INFO_1(message)                                     \
     VAST_LOG_ACTOR_INFO_2(this->describe(), message)
#  define VAST_LOG_ACTOR_INFO(...)   VAST_PP_OVERLOAD(VAST_LOG_ACTOR_INFO_,  \
                                              __VA_ARGS__)(__VA_ARGS__)
#else
#  define VAST_LOG_INFO(message)     VAST_VOID
#  define VAST_LOG_ACTOR_INFO(...)   VAST_VOID
#endif
#if VAST_LOG_LEVEL > 3
#  define VAST_LOG_VERBOSE(message)     VAST_LOG(::vast::logger::verbose, message)
#  define VAST_LOG_ACTOR_VERBOSE_2(name, message)                                 \
     VAST_LOG(::vast::logger::verbose, VAST_THIS_ACTOR(name) << ' ' << message)
#  define VAST_LOG_ACTOR_VERBOSE_1(message)                                       \
     VAST_LOG_ACTOR_VERBOSE_2(this->describe(), message)
#  define VAST_LOG_ACTOR_VERBOSE(...)   VAST_PP_OVERLOAD(VAST_LOG_ACTOR_VERBOSE_, \
                                              __VA_ARGS__)(__VA_ARGS__)
#else
#  define VAST_LOG_VERBOSE(message)     VAST_VOID
#  define VAST_LOG_ACTOR_VERBOSE(...)   VAST_VOID
#endif
#if VAST_LOG_LEVEL > 4
#  define VAST_LOG_DEBUG(message)     VAST_LOG(::vast::logger::debug, message)
#  define VAST_LOG_ACTOR_DEBUG_2(name, message)                               \
     VAST_LOG(::vast::logger::debug, VAST_THIS_ACTOR(name) << ' ' << message)
#  define VAST_LOG_ACTOR_DEBUG_1(message)                                     \
     VAST_LOG_ACTOR_DEBUG_2(this->describe(), message)
#  define VAST_LOG_ACTOR_DEBUG(...)   VAST_PP_OVERLOAD(VAST_LOG_ACTOR_DEBUG_, \
                                              __VA_ARGS__)(__VA_ARGS__)
#else
#  define VAST_LOG_DEBUG(message)     VAST_VOID
#  define VAST_LOG_ACTOR_DEBUG(...)   VAST_VOID
#endif
#if VAST_LOG_LEVEL > 5
#  define VAST_ENTER_ARGS(args)                                               \
      auto vast_msg = ::vast::logger::instance()->make_message(               \
          ::vast::logger::level::trace,                                       \
          VAST_LOG_FACILITY, __PRETTY_FUNCTION__);                            \
                                                                              \
     ::vast::logger::tracer vast_tracer(std::move(vast_msg));                 \
     vast_tracer << "--> (" << args << ')';                                   \
     vast_tracer.commit();                                                    \
     VAST_VOID
#  define VAST_ENTER_ARGS_MSG(args, msg)                                      \
     ::vast::logger::tracer vast_tracer(__PRETTY_FUNCTION__);                 \
     vast_tracer << "--> (" << args << ") " << msg;                           \
     vast_tracer.commit();                                                    \
     VAST_VOID
#  define VAST_ENTER_MSG(msg) VAST_ENTER_ARGS_MSG('*', msg)

#  define VAST_ENTER_0()     VAST_ENTER_ARGS('*')
#  define VAST_ENTER_1(A)    VAST_ENTER_ARGS(A)
#  define VAST_ENTER_2(A, B) VAST_ENTER_ARGS_MSG(A, B)
#  define VAST_ENTER(...)    VAST_PP_OVERLOAD(VAST_ENTER_, \
                                              __VA_ARGS__)(__VA_ARGS__)

#  define VAST_ARG_1(A) #A << " = " << A
#  define VAST_ARG_2(A, B)                                      \
    VAST_ARG_1(A) << ", " << #B << " = " << B
#  define VAST_ARG_3(A, B, C)                                   \
    VAST_ARG_2(A, B) << ", " << #C << " = " << C
#  define VAST_ARG_4(A, B, C, D)                                \
    VAST_ARG_3(A, B, C) << ", " << #D << " = " << D
#  define VAST_ARG_5(A, B, C, D, E)                             \
    VAST_ARG_4(A, B, C, D) << ", " << #E << " = " << E
#  define VAST_ARG(...) VAST_PP_OVERLOAD(VAST_ARG_, __VA_ARGS__)(__VA_ARGS__)

#  define VAST_ARGF(arg, f) #arg << " = " << f(arg)
#  define VAST_ARGM(arg, m) #arg << " = " << arg.m()
#  define VAST_THIS "*this = " << *this
#  define VAST_MSG(msg)                                                      \
     vast_tracer.reset(false);                                               \
     vast_tracer << msg;                                                     \
     vast_tracer.commit();                                                   \
     VAST_VOID
#  define VAST_LEAVE(msg)                                                    \
     {                                                                       \
       vast_tracer.reset(true);                                              \
       vast_tracer << "<-- (void) " << msg;                                  \
       return;                                                               \
     }                                                                       \
     VAST_VOID
#  define VAST_RETURN_VAL_MSG(value, msg)                                    \
     {                                                                       \
       auto&& vast_result = value;                                           \
       vast_tracer.reset(true);                                              \
       vast_tracer << "<-- (" << vast_result << ") " << msg;                 \
       return vast_result;                                                   \
     }                                                                       \
     VAST_VOID
#  define VAST_RETURN_1(val)       VAST_RETURN_VAL_MSG(val, "")
#  define VAST_RETURN_2(val, msg)  VAST_RETURN_VAL_MSG(val, msg)
#  define VAST_RETURN(...) \
     VAST_PP_OVERLOAD(VAST_RETURN_, __VA_ARGS__)(__VA_ARGS__)

#else
#  define VAST_ENTER(...) VAST_VOID
#  define VAST_MSG(msg) VAST_VOID
#  define VAST_LEAVE(msg) return
#  define VAST_RETURN(value, ...) return value
#  define VAST_ARG(...)
#  define VAST_ARGF
#  define VAST_ARGM
#  define VAST_THIS
#endif // VAST_LOG_LEVEL > 5

#endif // VAST_LOGGER_H
