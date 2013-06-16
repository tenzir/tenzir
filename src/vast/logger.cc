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

  bool init(level console, level file, path dir)
  {
    console_level = console;
    file_level = file;

    std::ostringstream filename;
    filename << "vast_" << std::time(nullptr);
#ifdef VAST_POSIX
    filename << '_' << ::getpid();
#endif
    filename << ".log";
    log_file.open(to_string(dir / path(filename.str())), std::ios::out);
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

  level console_level;
  level file_level;
  std::fstream log_file;
  std::thread log_thread;
  util::queue<record> records;
};


void logger::message::append_header(level lvl)
{
  *this
    << std::setprecision(15) << std::setw(16) << std::left << std::setfill('0')
    << now().to_double()
    << ' '
    << std::this_thread::get_id()
    << ' ';
  if (lvl != quiet)
    *this << lvl << ' ';
}

void logger::message::append_function(char const* f)
{
  *this << prettify(f) << ' ';
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
  *this << fill << ' ';
}


logger::tracer::tracer(char const* pretty_func)
  : fun_(prettify(pretty_func))
{
  ++call_depth;
  msg_.append_header(trace);
  msg_.append_fill(message::right_arrow);
  msg_ << fun_ << ' ';
}

void logger::tracer::commit()
{
  instance()->log(trace, msg_.str());
  msg_.clear();
  msg_.str(std::string());
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
    msg_ << fun_ << ' ';
  }
}

logger::tracer::~tracer()
{
  msg_.seekp(0, std::ios::end);
  if (msg_.tellp() == 0)
  {
    msg_.append_header(trace);
    msg_.append_fill(message::left_arrow);
    msg_ << fun_;
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

void logger::init(level console, level file, path dir)
{
  impl_->init(console, file, dir);
}

void logger::log(level lvl, std::string&& msg)
{
  impl_->log(lvl, std::move(msg));
}

bool logger::takes(logger::level lvl) const
{
  return impl_->takes(lvl);
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
