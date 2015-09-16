#include <ctime>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <thread>

#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/time.h"
#include "vast/util/assert.h"
#include "vast/util/color.h"
#include "vast/util/queue.h"
#include "vast/util/string.h"
#include "vast/util/system.h"

namespace vast {
namespace {

// TODO: replace with thread_local once the compilers implement it.
__thread size_t call_depth = 0;

// TODO: fix potential bugs due to `operator<</>>` occurring in return type via
// decltype.
std::string prettify(char const* pretty_func) {
  auto paren = pretty_func;
  auto c = pretty_func;
  auto templates = 0;
  while (*c && (*c != ' ' || templates > 0)) {
    switch (*c) {
      default:
        break;
      case 'v': {
        static char const* vi = "virtual";
        auto v = vi;
        auto i = c;
        while (*++v == *++i)
          ;
        if (*v == '\0')
          c += 7;
      } break;
      case 't': {
        static char const* tn = "typename";
        auto t = tn;
        auto i = c;
        while (*++t == *++i)
          ;
        if (*t == '\0')
          c += 8;
      } break;
      case '<':
        ++templates;
        break;
      case '>':
        --templates;
        break;
      case '(': {
        VAST_ASSERT(paren == pretty_func);
        paren = c;
      } break;
    }
    ++c;
  }
  // No whitespace found, could be (con|des)tructor.
  if (!c)
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

struct logger::impl {
  bool file(level verbosity, std::string const& filename) {
    // Close and delete existing file first.
    if (log_file_.is_open()) {
      auto empty = log_file_.tellp() == 0;
      log_file_.close();
      if (empty)
        rm(filename_);
    }
    // Set new log file.
    file_level_ = verbosity;
    filename_ = filename;
    if (file_level_ != quiet) {
      if (!filename_.parent().empty() && !exists(filename_.parent())
          && !mkdir(filename_.parent()))
        return false;
      log_file_.open(filename_.str());
      if (!log_file_.is_open())
        return false;
    }
    if (!log_thread_.joinable())
      log_thread_ = std::thread([=] { run(); });
    return log_thread_.joinable();
  }

  bool console(level verbosity, bool colorized) {
    console_level_ = verbosity;
    colorized_ = colorized;
    console_ = true;
    if (!log_thread_.joinable())
      log_thread_ = std::thread([=] { run(); });
    return log_thread_.joinable();
  }

  bool takes(logger::level lvl) const {
    return lvl <= std::max(file_level_, console_level_);
  }

  void log(message&& msg) {
    messages_.push(std::move(msg));
  }

  void run() {
    while (true) {
      auto m = messages_.pop();
      // A quit message is the termination signal, as it would have already
      // been filtered out beforehand.
      if (m.lvl() == quiet) {
        auto empty = log_file_.tellp() == 0;
        if (log_file_.is_open())
          log_file_.close();
        if (empty)
          rm(filename_);
        return;
      }
      // If the message contains newlines, split it up into multiple ones to
      // preserve the correct indentation.
      auto msg = m.msg();
      for (auto& split : util::split(msg.begin(), msg.end(), "\n")) {
        auto line = std::string{split.first, split.second};
        if (log_file_ && m.lvl() <= file_level_) {
          log_file_ << std::setprecision(15) << std::setw(16) << std::left
                    << std::setfill('0') << m.timestamp() << " 0x"
                    << std::setw(14) << std::setfill(' ') << m.thread_id()
                    << ' ' << m.lvl() << ' ';
          if (! m.context().empty())
            log_file_ << m.context() << ' ';
          log_file_ << line << '\n';
        }
        if (console_ && m.lvl() <= console_level_) {
          if (colorized_) {
            switch (m.lvl()) {
              default:
                break;
              case error:
                std::cerr << util::color::bold_red;
                break;
              case warn:
                std::cerr << util::color::bold_yellow;
                break;
              case info:
                std::cerr << util::color::bold_green;
                break;
              case verbose:
                std::cerr << util::color::bold_cyan;
                break;
              case debug:
              case trace:
                std::cerr << util::color::bold_blue;
                break;
            }
          }
          std::cerr << "::";
          if (colorized_) {
            std::cerr << util::color::reset;
            if (! m.context().empty()) {
              std::cerr << ' ' << util::color::cyan << m.context();
              std::cerr << util::color::reset;
            }
          }
          std::cerr << ' ' << line << std::endl;
        }
      }
    }
  }

  void stop() {
    // A quiet message never arrives at the log thread, hence we use it to
    // signal termination.
    messages_.push({});
    log_thread_.join();
  }

  level console_level_ = logger::quiet;
  level file_level_ = logger::quiet;
  path filename_;
  std::ofstream log_file_;
  bool console_ = false;
  bool colorized_ = false;
  std::thread log_thread_;
  util::queue<message> messages_;
};

logger::message::message(level lvl) : lvl_{lvl} {
  std::ostringstream ss;
  ss << std::hex << std::this_thread::get_id();
  thread_id_ = ss.str();
}

logger::message::message(message const& other)
  : lvl_{other.lvl_},
    timestamp_{other.timestamp_},
    thread_id_{other.thread_id_},
    context_{other.context_},
    function_{other.function_} {
  ss_ << other.msg();
}

void logger::message::coin() {
  timestamp_ = time::now().time_since_epoch().double_seconds();
}

void logger::message::function(char const* f) {
  function_ = prettify(f);
}

void logger::message::clear() {
  ss_.str("");
  ss_.clear();
}

bool logger::message::empty() {
  return ss_.tellp() == 0;
}

logger::level logger::message::lvl() const {
  return lvl_;
}

double logger::message::timestamp() const {
  return timestamp_;
}

std::string const& logger::message::thread_id() const {
  return thread_id_;
}

std::string const& logger::message::context() const {
  return context_;
}

std::string const& logger::message::function() const {
  return function_;
}

std::string logger::message::msg() const {
  return ss_.str();
}

logger::message& operator<<(logger::message& msg, std::nullptr_t) {
  return msg;
}

std::ostream& operator<<(std::ostream& stream, logger::level lvl) {
  switch (lvl) {
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

logger::tracer::tracer(message&& msg) : msg_{std::move(msg)} {
  ++call_depth;
  fill(right_arrow);
  msg_ << "::" << msg_.function() << ' ';
}

void logger::tracer::fill(fill_type t) {
  VAST_ASSERT(call_depth >= 1);
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

void logger::tracer::commit() {
  msg_.coin();
  instance()->log(msg_);
  msg_.clear();
}

void logger::tracer::reset(bool exit) {
  msg_.clear();

  if (exit) {
    fill(left_arrow);
    msg_ << "::" << msg_.function() << ' ';
  } else {
    fill(bar);
  }
}

logger::tracer::~tracer() {
  if (msg_.empty()) {
    fill(left_arrow);
    msg_ << "::" << msg_.function();
  }
  commit();
  --call_depth;
}

bool logger::file(level verbosity, std::string const& filename) {
  return instance()->impl_->file(verbosity, filename);
}

bool logger::console(level verbosity) {
  return instance()->impl_->console(verbosity, false);
}

bool logger::console_colorized(level verbosity) {
  return instance()->impl_->console(verbosity, true);
}

void logger::log(message msg) {
  instance()->impl_->log(std::move(msg));
}

bool logger::takes(logger::level lvl) {
  return instance()->impl_->takes(lvl);
}

logger::message logger::make_message(logger::level lvl, std::string ctx,
                                     char const* fun) {
  VAST_ASSERT(instance()->impl_);
  auto& impl = instance()->impl_;
  message m{lvl};
  if (impl->console_level_ == trace || impl->file_level_ == trace)
    m.function_ = prettify(fun);
  m.context_ = std::move(ctx);
  m.coin();
  return m;
}

logger* logger::create() {
  return new logger;
}

void logger::initialize() {
  impl_ = std::make_unique<impl>();
}

void logger::destroy() {
  impl_->stop();
  delete this;
}

void logger::dispose() {
  delete this;
}

} // namespace vast
