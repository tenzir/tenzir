#include "vast/logger.h"

#include <cassert>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <thread>
#include "vast/filesystem.h"
#include "vast/time.h"
#include "vast/util/color.h"
#include "vast/util/queue.h"
#include "vast/util/system.h"

namespace vast {
namespace {

// TODO: replace with thread_local once the compilers implement it.
__thread size_t call_depth = 0;

// TODO: fix potential bugs due to `operator<</>>` occurring in return type via
// decltype.
std::string prettify(char const* pretty_func)
{
  auto paren = pretty_func;
  auto c = pretty_func;
  auto templates = 0;
  while (*c && (*c != ' ' || templates > 0))
  {
    switch (*c)
    {
      default:
        break;
      case 'v':
        {
          static char const* vi = "virtual";
          auto v = vi;
          auto i = c;
          while (*++v == *++i)
            ;
          if (*v == '\0')
            c += 7;
        }
        break;
      case 't':
        {
          static char const* tn = "typename";
          auto t = tn;
          auto i = c;
          while (*++t == *++i)
            ;
          if (*t == '\0')
            c += 8;
        }
        break;
      case '<':
        ++templates;
        break;
      case '>':
        --templates;
        break;
      case '(':
        {
          assert(paren == pretty_func);
          paren = c;
        }
        break;
    }
    ++c;
  }

  // No whitespace found, could be (con|des)tructor.
  if (! c)
    return {pretty_func, c};

  if (*paren != '(')
    while (*paren != '(')
      ++paren;

  // The space occurs before the '(', so we have a return type.
  if (++c < paren)
    return {c, paren};

  // If we went beyond the left parenthesis, we're in a (con|des)tructor.
  while (*paren && *paren != '(')
    ++paren;
  return {pretty_func, paren};
}

} // namespace <anonymous>

struct logger::impl
{
  bool init(level console, level file, bool color, bool show_fns, path dir)
  {
    show_functions_ = show_fns;
    console_level_ = console;
    file_level_ = file;
    use_colors_ = color;
    if (! log_file_.is_open())
    {
      if (! exists(dir) && ! mkdir(dir))
        return false;
      log_file_.open((dir / "vast.log").str());
      if (! log_file_)
        return false;
      log_thread_ = std::thread([=] { run(); });
    }
    return log_thread_.joinable();
  }

  bool takes(logger::level lvl) const
  {
    return lvl <= std::max(file_level_, console_level_);
  }

  void log(message&& msg)
  {
    messages_.push(std::move(msg));
  }

  void run()
  {
    assert(log_file_);

    while (true)
    {
      auto m = messages_.pop();
      if (m.lvl() == quiet)
      {
        if (log_file_)
          log_file_.close();
        return;
      }

      if (m.lvl() <= file_level_)
      {
        log_file_
          << std::setprecision(15) << std::setw(16) << std::left << std::setfill('0')
          << m.timestamp()
          << ' '
          << "0x" << std::setw(14) << std::setfill(' ') << m.thread_id()
          << ' '
          << m.lvl()
          << ' '
          << m.msg()
          << std::endl;
      }

      if (m.lvl() <= console_level_)
      {
        if (use_colors_)
          std::cerr << util::color::cyan;

        std::cerr
          << std::setprecision(15) << std::setw(16) << std::left << std::setfill('0')
          << m.timestamp()
          << ' ';

        if (use_colors_)
          std::cerr << util::color::blue;

        std::cerr
          << "0x" << std::setw(14) << std::setfill(' ')
          << m.thread_id()
          << ' ';

        if (use_colors_)
        {
          switch (m.lvl())
          {
            default:
              break;
            case error:
              std::cerr << util::color::red;
              break;
            case warn:
              std::cerr << util::color::yellow;
              break;
            case info:
              std::cerr << util::color::green;
              break;
            case verbose:
              std::cerr << util::color::cyan;
              break;
            case debug:
            case trace:
              std::cerr << util::color::blue;
              break;
          }
        }

        std::cerr << m.lvl() << ' ';

        if (use_colors_)
          std::cerr << util::color::reset;

        std::cerr << m.msg() << std::endl;
      }
    }
  }

  void stop()
  {
    // A quiet message never arrives at the log thread, hence we use it to
    // signal termination.
    messages_.push({});
    log_thread_.join();
  }

  bool show_functions_;
  bool use_colors_;
  level console_level_;
  level file_level_;
  std::ofstream log_file_;
  std::thread log_thread_;
  util::queue<message> messages_;
};


trial<logger::level> logger::parse_level(std::string const& str)
{
  if (str == "quiet" || str == "0")
    return quiet;
  if (str == "error" || str == "1")
    return error;
  if (str == "warn" || str == "2")
    return warn;
  if (str == "info" || str == "3")
    return info;
  if (str == "verbose" || str == "4")
    return verbose;
  if (str == "debug" || str == "5")
    return debug;
  if (str == "trace" || str == "6")
    return trace;

  return util::error{"could not parse log level"};
}

logger::message::message(level lvl)
  : lvl_{lvl}
{
  std::ostringstream ss;
  ss << std::hex << std::this_thread::get_id();
  thread_id_ = ss.str();
}

logger::message::message(message const& other)
  : lvl_{other.lvl_},
    timestamp_{other.timestamp_},
    thread_id_{other.thread_id_},
    facility_{other.facility_},
    function_{other.function_}
{
  ss_ << other.msg();
}

void logger::message::coin()
{
  timestamp_ = time::now().time_since_epoch().double_seconds();
}

void logger::message::function(char const* f)
{
  function_ = prettify(f);
}

void logger::message::clear()
{
  ss_.str("");
  ss_.clear();
}

bool logger::message::empty()
{
  return ss_.tellp() == 0;
}

logger::level logger::message::lvl() const
{
  return lvl_;
}

double logger::message::timestamp() const
{
  return timestamp_;
}

std::string const& logger::message::thread_id() const
{
  return thread_id_;
}

std::string const& logger::message::facility() const
{
  return facility_;
}

std::string const& logger::message::function() const
{
  return function_;
}

std::string logger::message::msg() const
{
  return ss_.str();
}

logger::message& operator<<(logger::message& msg, std::nullptr_t)
{
  return msg;
}

std::ostream& operator<<(std::ostream& stream, logger::level lvl)
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

logger::tracer::tracer(message&& msg)
  : msg_{std::move(msg)}
{
  ++call_depth;
  fill(right_arrow);
  msg_ << "::" << msg_.function() << ' ';
}

void logger::tracer::fill(fill_type t)
{
  assert(call_depth >= 1);

  std::string f(3 + call_depth, '-');
  f[f.size() - 1] = ' ';

  f[0] = '|';
  if (t == right_arrow)
    f[f.size() - 2] = '\\';
  else if (t == left_arrow)
    f[f.size() - 2] = '/';
  else if (t == bar)
    f[f.size() - 2] = '|';

  msg_ << f << ' ';
}

void logger::tracer::commit()
{
  msg_.coin();
  instance()->log(msg_);
  msg_.clear();
}

void logger::tracer::reset(bool exit)
{
  msg_.clear();

  if (exit)
  {
    fill(left_arrow);
    msg_ << "::" << msg_.function() << ' ';
  }
  else
  {
    fill(bar);
  }
}

logger::tracer::~tracer()
{
  if (msg_.empty())
  {
    fill(left_arrow);
    msg_ << "::" << msg_.function();
  }

  commit();
  --call_depth;
}


logger::logger()
{
  impl_ = new impl;
}

logger::~logger()
{
  delete impl_;
}

bool logger::init(level console, level file, bool colors, bool show_fns,
                  path dir)
{
  return impl_->init(console, file, colors, show_fns, dir);
}

void logger::log(message msg)
{
  impl_->log(std::move(msg));
}

bool logger::takes(logger::level lvl) const
{
  return impl_->takes(lvl);
}

logger::message logger::make_message(logger::level lvl,
                                     char const* facility,
                                     char const* fun) const
{
  assert(facility != nullptr);

  message m{lvl};
  if (impl_->show_functions_ || impl_->console_level_ == trace
      || impl_->file_level_ == trace)
    m.function_ = prettify(fun);

  if (*facility)
    m.facility_ = facility;

  m.coin();
  return m;
}

logger* logger::create()
{
  return new logger;
}

void logger::initialize()
{
  /* Nothing to do. */
}

void logger::destroy()
{
  impl_->stop();
  delete this;
}

void logger::dispose()
{
  delete this;
}

} // namespace vast
