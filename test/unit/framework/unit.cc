#include "framework/unit.h"

#include <cassert>
#include <cstdlib>
#include <regex>

char const* unit::color::reset        = "\033[0m";
char const* unit::color::black        = "\033[30m";
char const* unit::color::red          = "\033[31m";
char const* unit::color::green        = "\033[32m";
char const* unit::color::yellow       = "\033[33m";
char const* unit::color::blue         = "\033[34m";
char const* unit::color::magenta      = "\033[35m";
char const* unit::color::cyan         = "\033[36m";
char const* unit::color::white        = "\033[37m";
char const* unit::color::bold_black   = "\033[1m\033[30m";
char const* unit::color::bold_red     = "\033[1m\033[31m";
char const* unit::color::bold_green   = "\033[1m\033[32m";
char const* unit::color::bold_yellow  = "\033[1m\033[33m";
char const* unit::color::bold_blue    = "\033[1m\033[34m";
char const* unit::color::bold_magenta = "\033[1m\033[35m";
char const* unit::color::bold_cyan    = "\033[1m\033[36m";
char const* unit::color::bold_white   = "\033[1m\033[37m";
char const* unit::detail::suite       = nullptr;

namespace unit {

test::test(std::string name)
  : name_{std::move(name)}
{
}

void test::__pass(std::string msg)
{
  trace_.emplace_back(true, std::move(msg));
}

void test::__fail(std::string msg)
{
  trace_.emplace_back(false, std::move(msg));
}

std::vector<std::pair<bool, std::string>> const& test::__trace() const
{
  return trace_;
}

std::string const& test::__name() const
{
  return name_;
}


void engine::add(char const* name, std::unique_ptr<test> t)
{
  auto& suite = instance().suites_[std::string{name ? name : ""}];
  for (auto& x : suite)
    if (x->__name() == t->__name())
    {
      std::cout << "duplicate test name: " << t->__name() << '\n';
      std::abort();
    }

  suite.emplace_back(std::move(t));
}

namespace {

class logger
{
public:
  enum class level : int
  {
    quiet   = 0,
    error   = 1,
    info    = 2,
    verbose = 3,
    massive = 4
  };

  class message
  {
  public:
    message(logger& l, level lvl)
      : logger_{l},
        level_{lvl}
    {
    }

    template <typename T>
    message& operator<<(T const& x)
    {
      logger_.log(level_, x);
      return *this;
    }

  private:
    logger& logger_;
    level level_;
  };

  logger(int lvl_cons, int lvl_file, std::string const& logfile)
    : level_console_(static_cast<level>(lvl_cons)),
      level_file_(static_cast<level>(lvl_file)),
      console_{std::cerr}
  {
    if (! logfile.empty())
      file_.open(logfile, std::ofstream::out | std::ofstream::app);
  }

  template <typename T>
  void log(level lvl, T const& x)
  {
    if (lvl <= level_console_)
      console_ << x;

    if (lvl <= level_file_)
      file_ << x;
  }

  message error()
  {
    return message{*this, level::error};
  }

  message info()
  {
    return message{*this, level::info};
  }

  message verbose()
  {
    return message{*this, level::verbose};
  }

  message massive()
  {
    return message{*this, level::massive};
  }

private:
  level level_console_;
  level level_file_;
  std::ostream& console_;
  std::ofstream file_;
};

std::string render(std::chrono::microseconds t)
{
  return t.count() > 1000000
    ? (std::to_string(t.count() / 1000000) + '.'
      + std::to_string((t.count() % 1000000) / 10000) + " s")
    : t.count() > 1000
      ? (std::to_string(t.count() / 1000) + " ms")
      : (std::to_string(t.count()) + " us");
}

char const* check_file = "<none>";
size_t check_line = 0;

} // namespace <anonymous>

char const* engine::last_check_file()
{
  return check_file;
}

void engine::last_check_file(char const* file)
{
  check_file = file;
}

size_t engine::last_check_line()
{
  return check_line;
}

void engine::last_check_line(size_t line)
{
  check_line = line;
}

bool engine::run(configuration const& cfg)
{
  if (cfg.check("help"))
  {
    cfg.usage(std::cerr);
    return false;
  }

  if (cfg.check("no-colors"))
  {
    color::reset        = "";
    color::black        = "";
    color::red          = "";
    color::green        = "";
    color::yellow       = "";
    color::blue         = "";
    color::magenta      = "";
    color::cyan         = "";
    color::white        = "";
    color::bold_black   = "";
    color::bold_red     = "";
    color::bold_green   = "";
    color::bold_yellow  = "";
    color::bold_blue    = "";
    color::bold_magenta = "";
    color::bold_cyan    = "";
    color::bold_white   = "";
  }

  auto log_file = cfg.get("log-file");
  logger log{*cfg.as<int>("console-verbosity"),
             *cfg.as<int>("file-verbosity"),
             log_file ? *log_file : ""};

  std::chrono::microseconds runtime{0};
  size_t failed_requires = 0;
  size_t total_suites = 0;
  size_t total_tests = 0;
  size_t total_good = 0;
  size_t total_bad = 0;
  auto suite_rx = std::regex{*cfg.get("suites")};
  auto test_rx = std::regex{*cfg.get("tests")};
  auto bar = '+' + std::string(70, '-') + '+';

  std::regex not_suite_rx;
  auto not_suites = cfg.get("not-suites");
  if (not_suites)
    not_suite_rx = *not_suites;

  std::regex not_test_rx;
  auto not_tests = cfg.get("not-tests");
  if (not_tests)
    not_test_rx = *not_tests;

  for (auto& p : instance().suites_)
  {
    if (! std::regex_search(p.first, suite_rx)
        || (not_suites && std::regex_search(p.first, not_suite_rx)))
      continue;

    auto suite_name = p.first.empty() ? "<unnamed>" : p.first;
    auto pad = std::string((bar.size() - suite_name.size()) / 2, ' ');

    bool displayed_header = false;

    size_t tests_ran = 0;
    for (auto& t : p.second)
    {
      if (! std::regex_search(t->__name(), test_rx)
          || (not_tests && std::regex_search(t->__name(), not_test_rx)))
        continue;

      ++tests_ran;

      if (! displayed_header)
      {
        log.verbose()
          << color::yellow << bar << '\n' << pad << suite_name << '\n' << bar
          << color::reset << "\n\n";

        displayed_header = true;
      }

      log.verbose()
          << color::yellow << "- " << color::reset << t->__name() << '\n';

      auto failed_require = false;
      auto start = std::chrono::steady_clock::now();
      try
      {
        t->__run();
      }
      catch (require_error const& e)
      {
        failed_require = true;
      }
      auto stop = std::chrono::steady_clock::now();
      auto elapsed =
        std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

      runtime += elapsed;

      size_t good = 0;
      size_t bad = 0;
      for (auto& trace : t->__trace())
        if (trace.first)
        {
          ++good;
          log.massive() << "  " << trace.second << '\n';
        }
        else
        {
          ++bad;
          log.error() << "  " << trace.second << '\n';
        }

      if (failed_require)
      {
        ++failed_requires;
        log.error()
          << color::red << "     REQUIRED" << color::reset << '\n'
          << "     " << color::blue << last_check_file() << color::yellow
          << ":" << color::cyan << last_check_line() << color::reset
          << detail::fill(last_check_line()) << "had last successful check"
          << '\n';
      }

      total_good += good;
      total_bad += bad;

      log.verbose()
          << color::yellow << "  -> " << color::cyan << good + bad
          << color::reset << " check" << (good + bad > 1 ? "s " : " ")
          << "took " << color::cyan << render(elapsed) << color::reset;

      if (bad > 0)
        log.verbose()
          << " (" << color::green << good << color::reset << '/'
          << color::red << bad << color::reset << ")" << '\n';
      else
        log.verbose() << '\n';

      ++total_tests;
    }

    // We only counts suites which have executed one or more tests.
    if (tests_ran > 0)
      ++total_suites;

    if (displayed_header)
      log.verbose() << '\n';
  }

  auto percent_good =
    unsigned(100000 * total_good / double(total_good + total_bad)) / 1000.0;

  auto title = std::string{"summary"};
  auto pad = std::string((bar.size() - title.size()) / 2, ' ');
  auto indent = std::string(27, ' ');

  log.info()
    << color::cyan << bar << '\n' << pad << title << '\n' << bar
    << color::reset << "\n\n"
    << indent << "suites:  " << color::yellow << total_suites << color::reset << '\n'
    << indent << "tests:   " << color::yellow << total_tests << color::reset
    << '\n'
    << indent << "checks:  " << color::yellow << total_good + total_bad
    << color::reset;

  if (total_bad > 0)
    log.info()
    << " (" << color::green << total_good << color::reset << '/'
    << color::red << total_bad << color::reset << ")";

  log.info()
    << '\n' << indent << "time:    " << color::yellow << render(runtime)
    << '\n' << color::reset << indent << "success: "
    << (percent_good == 100.0 ? color::green : color::yellow)
    << percent_good << "%" << color::reset << "\n\n"
    << color::cyan << bar << color::reset << '\n';

  return total_bad == 0;
}

engine& engine::instance()
{
  static engine e;
  return e;
}

} // namespace unit
