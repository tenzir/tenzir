//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/os.hpp"

#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/config.hpp"
#include "tenzir/table_slice_builder.hpp"

#include <algorithm>

#if TENZIR_MACOS
#  include <mach/mach_time.h>

#  include <libproc.h>
#endif

// The current state of the implementation is highly experimental. It's a POC to
// for some demos, in order to show that it's possible to get endpoint data if
// need be. The code is basically taking inspiration from Zeek Agent v2 at
// https://github.com/zeek/zeek-agent-v2/ and making it fit into Tenzir. None of
// this has been tested extensively.

namespace tenzir {

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

auto socket_type() -> type {
  return type{
    "tenzir.socket",
    record_type{
      {"pid", uint64_type{}},
      {"process", string_type{}},
      {"protocol", uint64_type{}},
      {"local_addr", ip_type{}},
      {"local_port", uint64_type{}},
      {"remote_addr", ip_type{}},
      {"remote_port", uint64_type{}},
      {"state", string_type{}},
    },
  };
}

auto os::make() -> std::unique_ptr<os> {
#if TENZIR_MACOS
  return darwin::make();
#endif
  return nullptr;
}

namespace {

struct process {
  std::string name;
  uint32_t pid;
  uint32_t ppid;
  uid_t uid;
  gid_t gid;
  uid_t ruid;
  gid_t rgid;
  std::string priority;
  time startup;
  std::optional<uint64_t> vsize;
  std::optional<uint64_t> rsize;
  std::optional<duration> utime;
  std::optional<duration> stime;
};

struct socket {
  uint32_t pid;
  int protocol;
  ip local_addr;
  uint16_t local_port;
  ip remote_addr;
  uint16_t remote_port;
  std::string state;
};

} // namespace

#if TENZIR_MACOS

namespace {

auto fetch_processes(const auto& timebase) -> std::vector<process> {
  auto num_procs = proc_listpids(PROC_ALL_PIDS, 0, nullptr, 0);
  std::vector<pid_t> pids;
  pids.resize(num_procs);
  num_procs = proc_listpids(PROC_ALL_PIDS, 0, pids.data(),
                            detail::narrow_cast<int>(pids.size()));
  if (num_procs <= 0) {
    TENZIR_ERROR("failed to get PIDs");
    return {};
  }
  auto result = std::vector<process>{};
  result.reserve(pids.size());
  for (auto pid : pids) {
    if (pid <= 0)
      continue;
    errno = 0;
    proc_bsdinfo proc{};
    auto n = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &proc, sizeof(proc));
    if (n < detail::narrow_cast<int>(sizeof(proc)) || errno != 0) {
      if (errno == ESRCH) // process is gone
        continue;
      TENZIR_DEBUG("could not get process info for PID {}", pid);
      continue;
    }
    proc_taskinfo task{};
    n = proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &task, sizeof(task));
    // Put it all together.
    auto runtime = std::chrono::seconds(proc.pbi_start_tvsec)
                   + std::chrono::microseconds(proc.pbi_start_tvusec);
    auto p = process{
      .name = std::string{static_cast<char*>(proc.pbi_name)},
      .pid = proc.pbi_pid,
      .ppid = proc.pbi_ppid,
      .uid = proc.pbi_uid,
      .gid = proc.pbi_gid,
      .ruid = proc.pbi_ruid,
      .rgid = proc.pbi_rgid,
      .priority = std::to_string(-proc.pbi_nice),
      .startup = time{runtime},
      .vsize = {},
      .rsize = {},
      .utime = {},
      .stime = {},
    };
    if (n < 0) {
      p.vsize = task.pti_virtual_size;
      p.rsize = task.pti_resident_size;
      auto utime = task.pti_total_user * timebase.numer / timebase.denom;
      auto stime = task.pti_total_system * timebase.numer / timebase.denom;
      p.utime = std::chrono::nanoseconds(utime);
      p.stime = std::chrono::nanoseconds(stime);
    }
    result.push_back(std::move(p));
  }
  return result;
}

auto socket_state_to_string(auto proto, auto state) -> std::string_view {
  switch (proto) {
    default:
      break;
    case 6: {
      switch (state) {
        default:
          break;
        case 0:
          return "CLOSED";
        case 1:
          return "LISTEN";
        case 2:
          return "SYN_SENT";
        case 3:
          return "SYN_RECEIVED";
        case 4:
          return "ESTABLISHED";
        case 5:
          return "CLOSE_WAIT";
        case 6:
          return "FIN_WAIT_1";
        case 7:
          return "CLOSING";
        case 8:
          return "LAST_ACK";
        case 9:
          return "FIN_WAIT_2";
        case 10:
          return "TIME_WAIT";
        case 11:
          return "RESERVED";
      }
    }
  }
  return "";
}

auto sockets_for_pid(pid_t pid) -> std::vector<socket> {
  auto n = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, nullptr, 0);
  auto fds = std::vector<proc_fdinfo>{};
  fds.resize(n);
  n = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, fds.data(), n);
  if (n <= 0) {
    TENZIR_WARN("could not get file descriptors for process {}", pid);
    return {};
  }
  auto result = std::vector<socket>{};
  for (auto& fd : fds) {
    if (fd.proc_fdtype != PROX_FDTYPE_SOCKET)
      continue;
    auto info = socket_fdinfo{};
    errno = 0;
    n = proc_pidfdinfo(pid, fd.proc_fd, PROC_PIDFDSOCKETINFO, &info,
                       sizeof(socket_fdinfo));
    if (n < static_cast<int>(sizeof(socket_fdinfo)) or errno != 0)
      continue;
    // Only consider network connections.
    if (info.psi.soi_family != AF_INET and info.psi.soi_family != AF_INET6)
      continue;
    auto to_string = [](auto family, const auto& addr) -> std::string {
      auto buffer = std::array<char, INET6_ADDRSTRLEN>{};
      switch (family) {
        default:
          return {};
        case PF_INET:
          inet_ntop(family, &addr.ina_46.i46a_addr4, buffer.data(),
                    buffer.size());
          break;
        case PF_INET6:
          inet_ntop(family, &addr.ina_6, buffer.data(), buffer.size());
          break;
      };
      return std::string{buffer.data()};
    };
    // Populate socket.
    auto s = socket{};
    s.protocol = info.psi.soi_protocol;
    auto local_addr
      = to_string(info.psi.soi_family, info.psi.soi_proto.pri_in.insi_laddr);
    auto remote_addr
      = to_string(info.psi.soi_family, info.psi.soi_proto.pri_in.insi_faddr);
    if (auto addr = to<ip>(local_addr))
      s.local_addr = *addr;
    s.local_port = ntohs(info.psi.soi_proto.pri_in.insi_lport);
    if (auto addr = to<ip>(remote_addr))
      s.remote_addr = *addr;
    s.remote_port = ntohs(info.psi.soi_proto.pri_in.insi_fport);
    s.state = socket_state_to_string(info.psi.soi_protocol,
                                     info.psi.soi_proto.pri_tcp.tcpsi_state);
    result.push_back(std::move(s));
  }
  return result;
}

} // namespace

struct darwin::state {
  struct mach_timebase_info timebase_ {};
};

auto darwin::make() -> std::unique_ptr<darwin> {
  auto result = std::unique_ptr<darwin>{new darwin};
  if (mach_timebase_info(&result->state_->timebase_) != KERN_SUCCESS) {
    TENZIR_ERROR("failed to get MACH timebase");
    return nullptr;
  }
  return result;
}

darwin::darwin() : state_{std::make_unique<state>()} {
}

darwin::~darwin() = default;

auto darwin::processes() -> table_slice {
  auto builder = table_slice_builder{process_type()};
  for (const auto& proc : fetch_processes(state_->timebase_)) {
    auto okay = builder.add(proc.name, proc.pid, proc.ppid, proc.uid, proc.gid,
                            proc.ruid, proc.rgid, proc.priority, proc.startup);
    okay = builder.add(proc.vsize ? make_view(*proc.vsize) : data_view{});
    TENZIR_ASSERT(okay);
    okay = builder.add(proc.rsize ? make_view(*proc.rsize) : data_view{});
    TENZIR_ASSERT(okay);
    okay = builder.add(proc.utime ? make_view(*proc.utime) : data_view{});
    TENZIR_ASSERT(okay);
    okay = builder.add(proc.stime ? make_view(*proc.stime) : data_view{});
    TENZIR_ASSERT(okay);
  }
  return builder.finish();
}

auto darwin::sockets() -> table_slice {
  auto builder = table_slice_builder{socket_type()};
  auto procs = fetch_processes(state_->timebase_);
  for (const auto& proc : fetch_processes(state_->timebase_)) {
    auto pid = detail::narrow_cast<pid_t>(proc.pid);
    for (const auto& socket : sockets_for_pid(pid)) {
      auto okay = builder.add(uint64_t{proc.pid}, proc.name,
                              detail::narrow_cast<uint64_t>(socket.protocol),
                              socket.local_addr, uint64_t{socket.local_port},
                              socket.remote_addr, uint64_t{socket.remote_port},
                              not socket.state.empty() ? make_view(socket.state)
                                                       : data_view{});
      TENZIR_ASSERT(okay);
    }
  }
  return builder.finish();
}

#endif

} // namespace tenzir
