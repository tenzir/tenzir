//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/config.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>

#include <algorithm>

#ifdef TENZIR_MACOS
#  include <mach/mach_time.h>

#  include <libproc.h>
#endif

namespace tenzir::plugins::processes {

namespace {

/// A platform-independent operating system.
class os {
public:
  virtual ~os() = default;

  /// Provides a snapshot of all currently running processes.
  virtual auto processes() -> table_slice = 0;
};

/// A type that represents a format.
auto process_type() -> type {
  return type{
    "tenzir.process",
    record_type{
      {"name", string_type{}},
      {"pid", uint64_type{}},
      {"ppid", uint64_type{}},
      {"uid", uint64_type{}},
      {"gid", uint64_type{}},
      {"ruid", uint64_type{}},
      {"rgid", uint64_type{}},
      {"priority", string_type{}},
      {"startup", time_type{}},
      {"vsize", uint64_type{}},
      {"rsize", uint64_type{}},
      {"utime", duration_type{}},
      {"stime", duration_type{}},
    },
  };
}

#ifdef TENZIR_MACOS

class macos final : public os {
public:
  static auto make() -> std::unique_ptr<macos> {
    auto result = std::make_unique<macos>();
    if (mach_timebase_info(&result->timebase_) != KERN_SUCCESS) {
      TENZIR_ERROR("failed to get MACH timebase");
      return nullptr;
    }
    return result;
  }

  auto processes() -> table_slice final {
    auto num_procs = proc_listpids(PROC_ALL_PIDS, 0, nullptr, 0);
    std::vector<pid_t> pids;
    pids.resize(num_procs);
    num_procs = proc_listpids(PROC_ALL_PIDS, 0, pids.data(),
                              detail::narrow_cast<int>(pids.size()));
    if (num_procs <= 0) {
      TENZIR_ERROR("failed to get PIDs");
      return {};
    }
    auto builder = table_slice_builder{process_type()};
    for (auto pid : pids) {
      if (pid <= 0)
        continue;
      errno = 0;
      proc_bsdinfo proc{};
      auto n = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &proc, sizeof(proc));
      if (n < static_cast<int>(sizeof(proc)) || errno != 0) {
        if (errno == ESRCH) // process is gone
          continue;
        TENZIR_VERBOSE("could not get process info for PID {}", pid);
        continue;
      }
      proc_taskinfo task{};
      n = proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &task, sizeof(task));
      // Put it all together.
      auto runtime = std::chrono::seconds(proc.pbi_start_tvsec)
                     + std::chrono::microseconds(proc.pbi_start_tvusec);
      auto okay = builder.add(std::string_view{proc.pbi_name}, proc.pbi_pid,
                              proc.pbi_ppid, proc.pbi_uid, proc.pbi_gid,
                              proc.pbi_ruid, proc.pbi_rgid,
                              std::to_string(-proc.pbi_nice), time{runtime});
      TENZIR_ASSERT(okay);
      if (n < 0) {
        okay = builder.add(caf::none, caf::none, caf::none, caf::none);
        TENZIR_ASSERT(okay);
      } else {
        auto utime = task.pti_total_user * timebase_.numer / timebase_.denom;
        auto stime = task.pti_total_system * timebase_.numer / timebase_.denom;
        okay = builder.add(task.pti_virtual_size, task.pti_resident_size,
                           std::chrono::nanoseconds(utime),
                           std::chrono::nanoseconds(stime));
        TENZIR_ASSERT(okay);
      }
    }
    return builder.finish();
  }

private:
  struct mach_timebase_info timebase_ {};
};

#endif

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "processes";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto system = std::unique_ptr<os>{};
#ifdef TENZIR_MACOS
    system = macos::make();
#endif
    if (not system) {
      diagnostic::error("failed to create operating system shim")
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield system->processes();
  }
};

} // namespace

} // namespace tenzir::plugins::processes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::processes::plugin)
