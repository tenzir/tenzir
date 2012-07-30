#include "vast/util/profiler.h"

#include <iomanip>
#include <sys/resource.h>   // getrusage
#include <sys/time.h>       // gettimeofday
#include "vast/logger.h"
#include "vast/fs/path.h"
#include "config.h"

#ifdef USE_PERFTOOLS_CPU_PROFILER
#include <google/profiler.h>
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
#include <google/heap-profiler.h>
#endif

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
  out << std::fixed
    << std::setw(18) << m.clock
    << std::setw(14) << m.usr
    << std::setw(14) << m.sys;

  return out;
}

profiler::profiler(std::string const& log_dir, std::chrono::seconds secs)
{
  auto f = (fs::path(log_dir) / "profile.log").string();
  file_.open(f.data());

  LOG(verbose, core) << "spawning profiler @" << id();
  LOG(verbose, core)
    << "enabling getrusage profiling every "
    << (secs.count() == 1 ?
        std::string("second") :
        std::to_string(secs.count()) + " seconds")
    << " (" << f << ')';

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

  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("run"), arg_match) >> [=](bool perftools_cpu, bool perftools_heap)
      {
#ifdef USE_PERFTOOLS_CPU_PROFILER
        if (perftools_cpu)
        {
          LOG(info, core)
            << "profiler @" << id() << " starts Gperftools CPU profiler";

          auto f = fs::path(log_dir) / "perftools.cpu";
          ProfilerStart(f.string().data());
        }
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
        if (perftools_heap)
        {
          LOG(info, core)
            << "profiler @" << id() << " starts Gperftools heap profiler";

          auto f = fs::path(log_dir) / "perftools.heap";
          HeapProfilerStart(f.string().data());
        }
#endif
        measurement now;
        delayed_send(self, secs, atom("data"), now.clock, now.usr, now.sys);
      },
      on(atom("data"), arg_match) >> [=](double clock, double usr, double sys)
      {
        measurement now;
        file_ << now;
        delayed_send(self, secs, atom("data"), now.clock, now.usr, now.sys);

        now.clock -= clock;
        now.usr -= usr;
        now.sys -= sys;
        file_ << now << std::endl;
      },
      on(atom("shutdown")) >> [=]
      {
#ifdef USE_PERFTOOLS_CPU_PROFILER
        ProfilerState state;
        ProfilerGetCurrentState(&state);
        if (state.enabled)
        {
          LOG(verbose, core)
            << "Gperftools CPU profiler gathered "
            <<  state.samples_gathered << " samples"
            << " in file " << state.profile_name;

          LOG(info, core)
            << "profiler @" << id() << " stops Gperftools CPU profiler";

          ProfilerStop();
        }
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
        if (IsHeapProfilerRunning())
        {
          LOG(info, core)
            << "profiler @" << id() << " stops Gperftools heap profiler";

          HeapProfilerDump("cleanup");
          HeapProfilerStop();
        }
#endif
        file_.close();
        quit();

        LOG(verbose, core) << "profiler @" << id() << " terminated";
      });
}

} // namespace util
} // namespace vast
