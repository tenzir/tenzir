#include <cassert>

#include <gperftools/profiler.h>
#include <gperftools/heap-profiler.h>

#include <caf/all.hpp>

#include "vast/config.h"
#include "vast/time.h"
#include "vast/filesystem.h"
#include "vast/actor/profiler.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"

namespace vast {

profiler::profiler(path log_dir, std::chrono::seconds secs)
  : default_actor{"profiler"}, log_dir_{std::move(log_dir)}, secs_{secs} {
}

void profiler::on_exit() {
  ProfilerState state;
  ProfilerGetCurrentState(&state);
  if (state.enabled) {
    VAST_INFO(this, "stops Gperftools CPU profiler");
    ProfilerStop();
    VAST_INFO(this, "recorded", state.samples_gathered,
              "Gperftools CPU profiler samples in", state.profile_name);
  }
#ifdef VAST_USE_TCMALLOC
  if (IsHeapProfilerRunning()) {
    VAST_INFO(this, "stops Gperftools heap profiler");
    HeapProfilerDump("cleanup");
    HeapProfilerStop();
  }
#endif
}

behavior profiler::make_behavior() {
  if (!exists(log_dir_)) {
    auto t = mkdir(log_dir_);
    if (!t) {
      VAST_ERROR(this, "could not create directory:", t.error());
      quit(exit::error);
    }
  }
  return {
    [=](start_atom, std::string const& type) {
    if (type == "cpu") {
      VAST_INFO(this, "starts Gperftools CPU profiler");
      auto filename = to_string(log_dir_ / "perftools.cpu");
      ProfilerStart(filename.c_str());
      delayed_send(this, secs_, flush_atom::value);
    } else if (type == "heap") {
#ifdef VAST_USE_TCMALLOC
    VAST_INFO(this, "starts Gperftools heap profiler");
    auto filename = to_string(log_dir_ / "perftools.heap");
    HeapProfilerStart(filename.c_str());
#else
    VAST_ERROR(this, "cannot start heap profiler",
               "(not linked against tcmalloc)");
    quit(exit::error);
#endif
    } else {
      VAST_ERROR(this, "got invalid profiler type");
      quit(exit::error);
    }
    },
    [=](flush_atom) {
      ProfilerFlush();
      delayed_send(this, secs_, flush_atom::value);
    },
    catch_unexpected()
  };
}

} // namespace vast
