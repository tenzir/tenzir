#ifndef VAST_LOGGER_H
#define VAST_LOGGER_H

#include <sstream>
#include "vast/config.h"
#include "vast/singleton.h"
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
  struct message : public std::ostringstream
  {
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
  };

  /// Facilitates RAII-style tracing.
  class tracer
  {
  public:
    tracer(char const* pretty_function);
    ~tracer();

    template <typename T>
    message& operator<<(T&& x)
    {
      msg_ << std::forward<T>(x);
      return msg_;
    }

    void commit();
    void reset(bool exit);

  private:
    std::string fun_;
    message msg_;
  };

  /// Destroys the logger.
  ~logger();

  /// Initializes the logger.
  /// This function must be called once prior to using any logging macro.
  /// @param console The log level of the console.
  /// @param file The log level of the logfile.
  /// @param dir The directory to create the log file in.
  void init(level console, level file, path dir);

  /// Logs a record.
  /// @param lvl The log level.
  /// @param msg The log message.
  void log(level lvl, std::string&& msg);

  /// Checks whether the logger takes a given level.
  /// @param lvl The level to check.
  /// @param `true` if the logger takes at least *lvl*.
  bool takes(level lvl) const;

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
};

template <typename Stream>
Stream& operator<<(Stream& stream, logger::level lvl)
{
  switch (lvl)
  {
    default:
      stream << "invalid";
      break;
    case logger::quiet:
      stream << "quiet  ";
      break;
    case logger::error:
      stream << "error  ";
      break;
    case logger::warn:
      stream << "warning";
      break;
    case logger::info:
      stream << "info   ";
      break;
    case logger::verbose:
      stream << "verbose";
      break;
    case logger::debug:
      stream << "debug  ";
      break;
    case logger::trace:
      stream << "trace  ";
      break;
  }
  return stream;
}

} // namespace vast

#define VAST_VOID static_cast<void>(0)

#ifndef VAST_LOG_FACILITY
#  define VAST_LOG_FACILITY '\0'
#endif

#define VAST_LOG(lvl, msg)                                                  \
  {                                                                         \
    if (::vast::logger::instance()->takes(lvl))                             \
    {                                                                       \
      ::vast::logger::message m;                                            \
      m.append_header(lvl);                                                 \
      m.append_function(__PRETTY_FUNCTION__);                               \
      if (VAST_LOG_FACILITY)                                                \
        m << " [" << VAST_LOG_FACILITY << ']';                              \
      m << ' ' << msg;                                                      \
      ::vast::logger::instance()->log(lvl, m.str());                        \
    }                                                                       \
  } VAST_VOID

#if VAST_LOG_LEVEL > 0
#  define VAST_LOG_ERROR(message)   VAST_LOG(::vast::logger::error, message)
#else
#  define VAST_LOG_ERROR(message)   VAST_VOID
#endif
#if VAST_LOG_LEVEL > 1
#  define VAST_LOG_WARN(message)    VAST_LOG(::vast::logger::warn, message)
#else
#  define VAST_LOG_WARN(message)    VAST_VOID
#endif
#if VAST_LOG_LEVEL > 2
#  define VAST_LOG_INFO(message)    VAST_LOG(::vast::logger::info, message)
#else
#  define VAST_LOG_INFO(message)    VAST_VOID
#endif
#if VAST_LOG_LEVEL > 3
#  define VAST_LOG_VERBOSE(message) VAST_LOG(::vast::logger::verbose, message)
#else
#  define VAST_LOG_VERBOSE(message) VAST_VOID
#endif
#if VAST_LOG_LEVEL > 4
#  define VAST_LOG_DEBUG(message)   VAST_LOG(::vast::logger::debug, message)
#else
#  define VAST_LOG_DEBUG(message)   VAST_VOID
#endif
#if VAST_LOG_LEVEL > 5
#  define VAST_ENTER_ARGS(args)                                              \
     ::vast::logger::tracer ze_tracer(__PRETTY_FUNCTION__);                    \
     ze_tracer << " -->(" << args << ')';                                    \
     ze_tracer.commit();                                                     \
     VAST_VOID
#  define VAST_ENTER_ARGS_MSG(args, msg)                                     \
     ::vast::logger::tracer ze_tracer(__PRETTY_FUNCTION__);                    \
     ze_tracer << " -->(" << args << ") " << msg;                            \
     ze_tracer.commit();                                                     \
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
     ze_tracer.reset(false);                                                 \
     ze_tracer << msg;                                                       \
     ze_tracer.commit();                                                     \
     VAST_VOID
#  define VAST_LEAVE(msg)                                                    \
     {                                                                       \
       ze_tracer.reset(true);                                                \
       ze_tracer << " <--(void) " << msg;                                    \
       return;                                                               \
     }                                                                       \
     VAST_VOID
#  define VAST_RETURN_VAL_MSG(value, msg)                                    \
     {                                                                       \
       auto&& ze_result = value;                                             \
       ze_tracer.reset(true);                                                \
       ze_tracer << " <--(" << ze_result << ") " << msg;                     \
       return ze_result;                                                     \
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
