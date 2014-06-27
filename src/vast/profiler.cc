#include "vast/profiler.h"

#include <cassert>
#include <iomanip>
#include <sys/resource.h>   // getrusage
#include <sys/time.h>       // gettimeofday
#include <cppa/cppa.hpp>
#include "vast/config.h"
#include "vast/file_system.h"

#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
#include <google/profiler.h>
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
#include <google/heap-profiler.h>
#endif

using namespace cppa;

namespace vast {

profiler::measurement::measurement()
{
  struct timeval begin;
  gettimeofday(&begin, 0);
  clock = static_cast<double>(begin.tv_sec) +
    static_cast<double>(begin.tv_usec) / 1000000.0;

  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  struct timeval& u = ru.ru_utime;
  struct timeval& s = ru.ru_stime;

  usr = static_cast<double>(u.tv_sec) +
    static_cast<double>(u.tv_usec) / 1000000.0;

  sys = static_cast<double>(s.tv_sec) +
    static_cast<double>(s.tv_usec) / 1000000.0;
}

std::ostream& operator<<(std::ostream& out, profiler::measurement const& m)
{
  out
    << std::fixed
    << std::setw(18) << m.clock
    << std::setw(14) << m.usr
    << std::setw(14) << m.sys;
  return out;
}

profiler::profiler(path log_dir, std::chrono::seconds secs)
  : log_dir_{std::move(log_dir)},
    secs_{secs}
{
}

partial_function profiler::act()
{
  trap_exit(true);

  auto filename = log_dir_ / "profile.log";
  file_.open(to_string(filename));

  VAST_LOG_ACTOR_INFO(
      "enables getrusage profiling every " <<
      (secs_.count() == 1
       ? std::string("second")
       : std::to_string(secs_.count()) + " seconds") <<
      " (" << filename << ')');

  assert(file_.good());
  file_.flags(std::ios::left);
  file_
    << std::setw(18) << "clock (c)"
    << std::setw(14) << "user (c)"
    << std::setw(14) << "sys (c)"
    << std::setw(18) << "clock (d)"
    << std::setw(14) << "user (d)"
    << std::setw(14) << "sys (d)"
    << std::endl;

  return
  {
    [=](exit_msg const& e)
    {
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
      ProfilerState state;
      ProfilerGetCurrentState(&state);
      if (state.enabled)
      {
        VAST_LOG_ACTOR_INFO("stops Gperftools CPU profiler");
        ProfilerStop();
        VAST_LOG_ACTOR_INFO(
            "recorded " << state.samples_gathered <<
            " Gperftools CPU profiler samples in " << state.profile_name);
      }
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
      if (IsHeapProfilerRunning())
      {
        VAST_LOG_ACTOR_INFO("stops Gperftools heap profiler");
        HeapProfilerDump("cleanup");
        HeapProfilerStop();
      }
#endif

      file_.close();
      quit(e.reason);
    },

#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
    on(atom("start"), atom("perftools"), atom("cpu")) >> [=]
    {
      VAST_LOG_ACTOR_INFO("starts Gperftools CPU profiler");

      auto f = to_string(log_dir_ / "perftools.cpu");
      ProfilerStart(f.c_str());
      delayed_send(this, secs_, atom("flush"));
    },

    on(atom("flush")) >> [=]
    {
      ProfilerFlush();
      delayed_send(this, secs_, atom("flush"));
    },
#endif

#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
    on(atom("start"), atom("perftools"), atom("heap")) >> [=]
    {
      VAST_LOG_ACTOR_INFO("starts Gperftools heap profiler");

      auto f = to_string(log_dir_ / "perftools.heap");
      HeapProfilerStart(f.c_str());
    },
#endif

    on(atom("start"), atom("rusage")) >> [=]
    {
      measurement now;
      delayed_send(this, secs_, atom("data"), now.clock, now.usr, now.sys);
    },

    on(atom("data"), arg_match) >> [=](double clock, double usr, double sys)
    {
      measurement now;
      file_ << now;
      delayed_send(this, secs_, atom("data"), now.clock, now.usr, now.sys);

      now.clock -= clock;
      now.usr -= usr;
      now.sys -= sys;
      file_ << now << std::endl;
    }
  };
}

std::string profiler::describe() const
{
  return "profiler";
}

} // namespace vast
