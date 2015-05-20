#include "vast/actor/profiler.h"

#include <cassert>
#include <iomanip>
#include <caf/all.hpp>
#include "vast/config.h"
#include "vast/time.h"
#include "vast/filesystem.h"

#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
#include <gperftools/profiler.h>
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
#include <gperftools/heap-profiler.h>
#endif

using namespace caf;

namespace vast {

profiler::profiler(path log_dir, std::chrono::seconds secs)
  : default_actor{"profiler"},
    log_dir_{std::move(log_dir)},
    secs_{secs}
{
}

void profiler::on_exit()
{
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
  ProfilerState state;
  ProfilerGetCurrentState(&state);
  if (state.enabled)
  {
    VAST_INFO(this, "stops Gperftools CPU profiler");
    ProfilerStop();
    VAST_INFO(this, "recorded", state.samples_gathered,
              "Gperftools CPU profiler samples in", state.profile_name);
  }
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
  if (IsHeapProfilerRunning())
  {
    VAST_INFO(this, "stops Gperftools heap profiler");
    HeapProfilerDump("cleanup");
    HeapProfilerStop();
  }
#endif
}

behavior profiler::make_behavior()
{
  if (! exists(log_dir_))
  {
    auto t = mkdir(log_dir_);
    if (! t)
      VAST_ERROR(this, "could not create directory:", t.error());
  }
  return
  {
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
    [=](start_atom, cpu_atom)
    {
      VAST_INFO(this, "starts Gperftools CPU profiler");

      auto f = to_string(log_dir_ / "perftools.cpu");
      ProfilerStart(f.c_str());
      delayed_send(this, secs_, flush_atom::value);
    },
    [=](flush_atom)
    {
      ProfilerFlush();
      delayed_send(this, secs_, flush_atom::value);
    },
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
    [=](start_atom, heap_atom)
    {
      VAST_INFO(this, "starts Gperftools heap profiler");

      auto f = to_string(log_dir_ / "perftools.heap");
      HeapProfilerStart(f.c_str());
    },
#endif
    catch_unexpected()
  };
}

} // namespace vast
