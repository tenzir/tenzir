#include <gperftools/profiler.h>
#include <gperftools/heap-profiler.h>

#include "vast/config.hpp"
#include "vast/time.hpp"
#include "vast/filesystem.hpp"
#include "vast/actor/atoms.hpp"
#include "vast/actor/profiler.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"

namespace vast {

profiler::state::state(local_actor* self)
  : basic_state{self, "profiler"} {
}

profiler::state::~state() {
  ProfilerState state;
  ProfilerGetCurrentState(&state);
  if (state.enabled) {
    VAST_INFO_AT(self, "stops gperftools CPU profiler");
    ProfilerStop();
    VAST_INFO_AT(self, "recorded", state.samples_gathered,
              "gperftools CPU profiler samples in", state.profile_name);
  }
#ifdef VAST_USE_TCMALLOC
  if (IsHeapProfilerRunning()) {
    VAST_INFO_AT(self, "stops gperftools heap profiler");
    HeapProfilerDump("cleanup");
    HeapProfilerStop();
  }
#endif
}

behavior profiler::make(stateful_actor<state>* self, path log_dir,
                        std::chrono::seconds secs) {
  return {
    [=](start_atom, std::string const& type) {
    if (!exists(log_dir)) {
      auto t = mkdir(log_dir);
      if (!t) {
        VAST_ERROR_AT(self, "could not create directory:", t.error());
        self->quit(exit::error);
      }
    }
    if (type == "cpu") {
      VAST_INFO_AT(self, "starts gperftools CPU profiler");
      auto filename = to_string(log_dir / "perftools.cpu");
      ProfilerStart(filename.c_str());
      self->delayed_send(self, secs, flush_atom::value);
    } else if (type == "heap") {
#ifdef VAST_USE_TCMALLOC
    VAST_INFO_AT(self, "starts gperftools heap profiler");
    auto filename = to_string(log_dir / "perftools.heap");
    HeapProfilerStart(filename.c_str());
#else
    VAST_ERROR_AT(self, "cannot start heap profiler",
               "(not linked against tcmalloc)");
    self->quit(exit::error);
#endif
    } else {
      VAST_ERROR_AT(self, "got invalid profiler type");
      self->quit(exit::error);
    }
    },
    [=](flush_atom) {
      ProfilerFlush();
      self->delayed_send(self, secs, flush_atom::value);
    },
    quit_on_others(self)
  };
}

} // namespace vast
