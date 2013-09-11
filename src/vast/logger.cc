#include "vast/logger.h"

#include <cassert>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <thread>
#include "vast/file_system.h"
#include "vast/time.h"
#include "vast/util/queue.h"

#ifdef VAST_POSIX
#  include <unistd.h> // getpid
#endif

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
  struct record
  {
    logger::level lvl;
    std::string msg;
  };

  bool init(level console, level file, bool show_fns, path dir)
  {
    show_functions = show_fns;
    console_level = console;
    file_level = file;

    std::ostringstream filename;
    filename << "vast_" << std::time(nullptr);
#ifdef VAST_POSIX
    filename << '_' << ::getpid();
#endif
    filename << ".log";
    if (! exists(dir))
      mkdir(dir);
    log_file.open(to_string(dir / path(filename.str())));
    if (! log_file)
      return false;

    log_thread = std::thread([=] { run(); });
    return true;
  }

  bool takes(logger::level lvl) const
  {
    return lvl <= std::max(file_level, console_level);
  }

  void log(logger::level lvl, std::string&& msg)
  {
    assert(! msg.empty());
    records.push({lvl, std::move(msg)});
  }

  void run()
  {
    assert(log_file);
    while (true)
    {
      auto r = records.pop();
      if (r.msg.empty())
      {
        if (log_file)
          log_file.close();
        return;
      }
      if (r.lvl <= console_level)
        std::cerr << r.msg << std::endl;
      if (r.lvl <= file_level)
        log_file << r.msg << std::endl;
    }
  }

  void stop()
  {
    records.push({quiet, ""});
    log_thread.join();
  }

  bool show_functions;
  level console_level;
  level file_level;
  std::ofstream log_file;
  std::thread log_thread;
  util::queue<record> records;
};


void logger::message::append_header(level lvl)
{
  ss_
    << std::setprecision(15) << std::setw(16) << std::left << std::setfill('0')
    << to<double>(now())
    << ' '
    << std::setw(14) << std::setfill(' ') << std::this_thread::get_id()
    << ' ';
  if (lvl != quiet)
    ss_ << lvl << ' ';
}

void logger::message::append_function(char const* f)
{
  ss_ << prettify(f) << ' ';
}

void logger::message::append_fill(fill_type t)
{
  assert(call_depth >= 1);
  std::string fill(3 + call_depth, '-');
  fill[fill.size() - 1] = ' ';
  if (t == right_arrow)
  {
    fill[0] = '+';
    fill[fill.size() - 2] = '\\';
  }
  else if (t == left_arrow)
  {
    fill[fill.size() - 2] = '/';
    fill[0] = '<';
  }
  else if (t == bar)
  {
    fill[fill.size() - 2] = '|';
  }
  ss_ << fill << ' ';
}

bool logger::message::fast_forward()
{
  ss_.seekp(0, std::ios::end);
  return ss_.tellp() != 0;
}

void logger::message::clear()
{
  ss_.clear();
  ss_.str(std::string());
}

std::string logger::message::str() const
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

logger::tracer::tracer(char const* fun)
  : fun_(fun)
{
  ++call_depth;
  msg_.append_header(trace);
  msg_.append_fill(message::right_arrow);
  msg_.append_function(fun_);
}

void logger::tracer::commit()
{
  instance()->log(trace, msg_.str());
  msg_.clear();
}

void logger::tracer::reset(bool exit)
{
  msg_.append_header(trace);
  if (! exit)
  {
    msg_.append_fill(message::bar);
  }
  else
  {
    msg_.append_fill(message::left_arrow);
    msg_.append_function(fun_);
  }
}

logger::tracer::~tracer()
{
  if (! msg_.fast_forward())
  {
    msg_.append_header(trace);
    msg_.append_fill(message::left_arrow);
    msg_.append_function(fun_);
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

bool logger::init(level console, level file, bool show_fns, path dir)
{
  return impl_->init(console, file, show_fns, dir);
}

void logger::log(level lvl, std::string&& msg)
{
  impl_->log(lvl, std::move(msg));
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
  message m;
  m.append_header(lvl);
  if (impl_->show_functions)
    m.append_function(fun);
  if (*facility)
    m << " [" << facility << "] ";
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
