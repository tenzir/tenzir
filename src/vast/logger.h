#ifndef VAST_LOGGER_H
#define VAST_LOGGER_H

#include <sstream>
#include "vast/config.h"
#include "vast/singleton.h"
#include "vast/traits.h"
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

  /// A formatted message containing timestamp and thread ID.
  class message
  {
  public:
    enum fill_type
    {
      left_arrow,
      right_arrow,
      bar
    };

    /// Appends the current timestamp and the thread ID.
    void append_header(level lvl = quiet);

    /// Appends the name of a function.
    /// @param The value of `__PRETTY_FUNCTION__`.
    void append_function(char const* f);

    void append_fill(fill_type t);

    bool fast_forward();

    void clear();

    std::string str() const;

    template <typename T>
    message& operator<<(T&& x)
    {
      ss_ << std::forward<T>(x);
      return *this;
    }

  private:
    std::ostringstream ss_;

    friend message& operator<<(message& msg, std::nullptr_t);
  };

  /// Facilitates RAII-style tracing.
  class tracer
  {
  public:
    tracer(char const* fun);
    ~tracer();

    template <typename T>
    friend tracer& operator<<(tracer& t, T&& x)
    {
      t.msg_ << std::forward<T>(x);
      return t;
    }

    void commit();
    void reset(bool exit);

  private:
    char const* fun_;
    message msg_;
  };

  /// Destroys the logger.
  ~logger();

  /// Initializes the logger.
  /// This function must be called once prior to using any logging macro.
  /// @param console The log level of the console.
  /// @param file The log level of the logfile.
  /// @param show_fns Whether to print function names in the output.
  /// @param dir The directory to create the log file in.
  /// @returns `true` on success.
  bool init(level console, level file, bool show_fns, path dir);

  /// Logs a record.
  /// @param lvl The log level.
  /// @param msg The log message.
  void log(level lvl, std::string&& msg);

  /// Checks whether the logger takes a given level.
  /// @param lvl The level to check.
  /// @param `true` if the logger takes at least *lvl*.
  bool takes(level lvl) const;

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
  {                                                                           \
    if (::vast::logger::instance()->takes(lvl))                               \
    {                                                                         \
      auto m = ::vast::logger::instance()->make_message(                      \
          lvl, VAST_LOG_FACILITY, __PRETTY_FUNCTION__);                       \
      m << msg;                                                               \
      ::vast::logger::instance()->log(lvl, m.str());                          \
    }                                                                         \
  } VAST_VOID

#define VAST_ACTOR(name, actor) \
  "@" << actor->id() << ":" << name

#define VAST_THIS_ACTOR(name) \
  VAST_ACTOR(name, cppa::self)

#if VAST_LOG_LEVEL > 0
#  define VAST_LOG_ERROR(message)   VAST_LOG(::vast::logger::error, message)
#  define VAST_LOG_ACT_ERROR(name, message) \
     VAST_LOG(::vast::logger::error, VAST_THIS_ACTOR(name) << ' ' << message)
#else
#  define VAST_LOG_ERROR(message)              VAST_VOID
#  define VAST_LOG_ACT_ERROR(name, message)    VAST_VOID
#endif
#if VAST_LOG_LEVEL > 1
#  define VAST_LOG_WARN(message)    VAST_LOG(::vast::logger::warn, message)
#  define VAST_LOG_ACT_WARN(name, message) \
     VAST_LOG(::vast::logger::warn, VAST_THIS_ACTOR(name) << ' ' << message)
#else
#  define VAST_LOG_WARN(message)               VAST_VOID
#  define VAST_LOG_ACT_WARN(name, message)     VAST_VOID
#endif
#if VAST_LOG_LEVEL > 2
#  define VAST_LOG_INFO(message)    VAST_LOG(::vast::logger::info, message)
#  define VAST_LOG_ACT_INFO(name, message) \
     VAST_LOG(::vast::logger::info, VAST_THIS_ACTOR(name) << ' ' << message)
#else
#  define VAST_LOG_INFO(message)               VAST_VOID
#  define VAST_LOG_ACT_INFO(name, message)     VAST_VOID
#endif
#if VAST_LOG_LEVEL > 3
#  define VAST_LOG_VERBOSE(message) VAST_LOG(::vast::logger::verbose, message)
#  define VAST_LOG_ACT_VERBOSE(name, message) \
     VAST_LOG(::vast::logger::verbose, VAST_THIS_ACTOR(name) << ' ' << message)
#else
#  define VAST_LOG_VERBOSE(message)            VAST_VOID
#  define VAST_LOG_ACT_VERBOSE(name, message)  VAST_VOID
#endif
#if VAST_LOG_LEVEL > 4
#  define VAST_LOG_DEBUG(message)   VAST_LOG(::vast::logger::debug, message)
#  define VAST_LOG_ACT_DEBUG(name, message) \
     VAST_LOG(::vast::logger::debug, VAST_THIS_ACTOR(name) << ' ' << message)
#else
#  define VAST_LOG_DEBUG(message)              VAST_VOID
#  define VAST_LOG_ACT_DEBUG(name, message)    VAST_VOID
#endif
#if VAST_LOG_LEVEL > 5
#  define VAST_ENTER_ARGS(args)                                              \
     ::vast::logger::tracer vast_tracer(__PRETTY_FUNCTION__);                \
     vast_tracer << " -->(" << args << ')';                                  \
     vast_tracer.commit();                                                   \
     VAST_VOID
#  define VAST_ENTER_ARGS_MSG(args, msg)                                     \
     ::vast::logger::tracer vast_tracer(__PRETTY_FUNCTION__);                    \
     vast_tracer << " -->(" << args << ") " << msg;                          \
     vast_tracer.commit();                                                   \
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
       vast_tracer << " <--(void) " << msg;                                  \
       return;                                                               \
     }                                                                       \
     VAST_VOID
#  define VAST_RETURN_VAL_MSG(value, msg)                                    \
     {                                                                       \
       auto&& vast_result = value;                                           \
       vast_tracer.reset(true);                                              \
       vast_tracer << " <--(" << vast_result << ") " << msg;                 \
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
