#include "vast/util/profiler.h"

#include <cassert>
#include <iomanip>
#include <sys/resource.h>   // getrusage
#include <sys/time.h>       // gettimeofday
#include "vast/config.h"
#include "vast/convert.h"
#include "vast/logger.h"
#include "vast/file_system.h"

#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
#include <google/profiler.h>
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
#include <google/heap-profiler.h>
#endif

using namespace cppa;

namespace vast {
namespace util {

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

profiler::profiler(std::string const& log_dir, std::chrono::seconds secs)
  : log_dir_(log_dir),
    secs_(secs)
{
  chaining(false);
}

void profiler::init()
{
  VAST_LOG_VERBOSE(VAST_ACTOR("profiler") << "spawned");

  auto filename = to<std::string>(path(log_dir_) / path("profile.log"));
  file_.open(filename);

  VAST_LOG_INFO(
      "enabling getrusage profiling every " <<
      (secs_.count() == 1 ?
        std::string("second") :
        std::to_string(secs_.count()) + " seconds") <<
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

  become(
      on(atom("run"), arg_match) >> [=](bool perftools_cpu, bool perftools_heap)
      {
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
        if (perftools_cpu)
        {
          VAST_LOG_INFO(
              VAST_ACTOR("profiler") << " starts Gperftools CPU profiler");

          auto f = to_string(path(log_dir_) / path("perftools.cpu"));
          ProfilerStart(f.c_str());
        }
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
        if (perftools_heap)
        {
          VAST_LOG_INFO(
              VAST_ACTOR("profiler") << " starts Gperftools heap profiler");

          auto f = to_string(path(log_dir_) / path("perftools.heap"));
          HeapProfilerStart(f.c_str());
        }
#endif
        measurement now;
        delayed_send(self, secs_, atom("data"), now.clock, now.usr, now.sys);
      },
      on(atom("data"), arg_match) >> [=](double clock, double usr, double sys)
      {
        measurement now;
        file_ << now;
        delayed_send(self, secs_, atom("data"), now.clock, now.usr, now.sys);

        now.clock -= clock;
        now.usr -= usr;
        now.sys -= sys;
        file_ << now << std::endl;
      },
      on(atom("kill")) >> [=]
      {
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
        ProfilerState state;
        ProfilerGetCurrentState(&state);
        if (state.enabled)
        {
          ProfilerStop();
          VAST_LOG_INFO(
              VAST_ACTOR("profiler") <<
              " let Gperftools CPU profiler gather " <<
              state.samples_gathered << " samples" <<
              " in file " << state.profile_name);
          VAST_LOG_INFO(
              VAST_ACTOR("profiler") << " stops Gperftools CPU profiler");
        }
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
        if (IsHeapProfilerRunning())
        {
          VAST_LOG_INFO(VAST_ACTOR("profiler") <<
                        " stops Gperftools heap profiler");

          HeapProfilerDump("cleanup");
          HeapProfilerStop();
        }
#endif
        file_.close();
        quit();
      });
}

void profiler::on_exit()
{
  VAST_LOG_VERBOSE(VAST_ACTOR("profiler") << " terminated");
}

} // namespace util
} // namespace vast
