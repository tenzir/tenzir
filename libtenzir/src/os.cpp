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
#include "tenzir/series_builder.hpp"

#include <string_view>

#if TENZIR_LINUX
#  include <pfs/procfs.hpp>

#  include <exception>
#elif TENZIR_MACOS
#  include <mach/mach_time.h>

#  include <libproc.h>
#endif

// The current state of the implementation is highly experimental. It's a POC to
// for some demos, in order to show that it's possible to get endpoint data if
// need be. The code is takes inspiration from Zeek Agent v2 at
// https://github.com/zeek/zeek-agent-v2/ and adapts it to fit into Tenzir.

namespace tenzir {

auto process_type() -> type {
  return type{
    "tenzir.process",
    record_type{
      {"name", string_type{}},
      {"command_line", list_type{string_type{}}},
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
      {"swap", uint64_type{}},
      {"peak_mem", uint64_type{}},
      {"open_fds", uint64_type{}},
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
      {"local_port", type{"port", uint64_type{}}},
      {"remote_addr", ip_type{}},
      {"remote_port", type{"port", uint64_type{}}},
      {"state", string_type{}},
    },
  };
}

auto os::make() -> std::unique_ptr<os> {
#if TENZIR_LINUX
  return linux_os::make();
#elif TENZIR_MACOS
  return darwin_os::make();
#endif
  return nullptr;
}

auto os::current_process() -> process {
  auto pid = getpid();
  return fetch_processes(pid).at(0);
}

auto os::processes() -> table_slice {
  auto builder = series_builder{process_type()};
  for (const auto& proc : fetch_processes()) {
    auto event = builder.record();
    event.field("name", proc.name);
    if (not proc.command_line.empty()) {
      auto cli = event.field("command_line").list();
      for (const auto& arg : proc.command_line) {
        cli.data(arg);
      }
    }
    event.field("pid", static_cast<uint64_t>(proc.pid));
    event.field("ppid", static_cast<uint64_t>(proc.ppid));
    event.field("uid", static_cast<uint64_t>(proc.uid));
    event.field("gid", static_cast<uint64_t>(proc.gid));
    event.field("ruid", static_cast<uint64_t>(proc.ruid));
    event.field("rgid", static_cast<uint64_t>(proc.rgid));
    event.field("priority", proc.priority);
    event.field("startup", proc.startup);
    if (proc.vsize) {
      event.field("vsize", *proc.vsize);
    }
    if (proc.rsize) {
      event.field("rsize", *proc.rsize);
    }
    if (proc.swap) {
      event.field("swap", *proc.swap);
    }
    if (proc.peak_mem) {
      event.field("peak_mem", *proc.peak_mem);
    }
    if (proc.open_fds) {
      event.field("open_fds", *proc.open_fds);
    }
    if (proc.utime) {
      event.field("utime", *proc.utime);
    }
    if (proc.stime) {
      event.field("stime", *proc.stime);
    }
  }
  return builder.finish_assert_one_slice();
}

auto os::sockets() -> table_slice {
  auto builder = series_builder{socket_type()};
  for (const auto& socket : fetch_sockets()) {
    auto event = builder.record();
    event.field("pid", static_cast<uint64_t>(socket.pid));
    if (not socket.process_name.empty()) {
      event.field("process", socket.process_name);
    }
    event.field("protocol", static_cast<uint64_t>(socket.protocol));
    event.field("local_addr", socket.local_addr);
    event.field("local_port", static_cast<uint64_t>(socket.local_port));
    event.field("remote_addr", socket.remote_addr);
    event.field("remote_port", static_cast<uint64_t>(socket.remote_port));
    if (not socket.state.empty()) {
      event.field("state", socket.state);
    }
  }
  return builder.finish_assert_one_slice();
}

#if TENZIR_LINUX

struct linux_os::state {
  uint64_t clock_tick{};
  pfs::procfs procfs{};
};

auto linux_os::make() -> std::unique_ptr<linux_os> {
  auto result = std::unique_ptr<linux_os>(new linux_os);
  result->state_->clock_tick = sysconf(_SC_CLK_TCK);
  return result;
}

linux_os::linux_os() : state_{std::make_unique<state>()} {
}

linux_os::~linux_os() = default;

auto linux_os::current_pid() -> int {
  return getpid();
}

auto linux_os::fetch_processes(std::optional<int> pid_filter)
  -> std::vector<process> {
  auto result = std::vector<process>{};
  try {
    auto tasks = state_->procfs.get_processes();
    result.reserve(tasks.size());
    for (const auto& task : tasks) {
      if (pid_filter && task.id() != *pid_filter) {
        continue;
      }
      try {
        auto stat = task.get_stat();
        auto status = task.get_status();
        auto command_line = task.get_cmdline();
        auto open_fds = task.count_fds();
        auto proc = process{
          .name = task.get_comm(),
          .command_line = std::move(command_line),
          .pid = detail::narrow_cast<uint32_t>(task.id()),
          .ppid = detail::narrow_cast<uint32_t>(stat.ppid),
          .uid = detail::narrow_cast<uid_t>(status.uid.effective),
          .gid = detail::narrow_cast<gid_t>(status.gid.effective),
          .ruid = detail::narrow_cast<uid_t>(status.uid.real),
          .rgid = detail::narrow_cast<gid_t>(status.gid.real),
          .priority = std::to_string(stat.priority),
          .startup = {},
          .vsize = static_cast<uint64_t>(stat.vsize),
          .rsize = static_cast<uint64_t>(stat.rss * getpagesize()),
          .peak_mem = static_cast<uint64_t>(status.vm_hwm),
          .swap = static_cast<uint64_t>(status.vm_swap),
          .open_fds = open_fds,
          .utime = std::chrono::seconds{stat.utime / state_->clock_tick},
          .stime = std::chrono::seconds{stat.stime / state_->clock_tick},
        };
        result.push_back(std::move(proc));
      } catch (std::system_error&) {
        TENZIR_DEBUG("ingoring exception for PID {}", task.id());
      } catch (std::runtime_error&) {
        TENZIR_DEBUG("ingoring exception for PID {}", task.id());
      }
    }
  } catch (const std::system_error&) {
    TENZIR_WARN("failed to read /proc filesystem (system error)");
  } catch (const std::runtime_error&) {
    TENZIR_WARN("failed to read /proc filesystem (runtime error)");
  }
  return result;
}

namespace {

auto to_string(pfs::net_socket::net_state state) -> std::string {
  switch (state) {
    default:
      break;
    case pfs::net_socket::net_state::close:
      return "CLOSED";
    case pfs::net_socket::net_state::close_wait:
      return "CLOSE_WAIT";
    case pfs::net_socket::net_state::closing:
      return "CLOSING";
    case pfs::net_socket::net_state::established:
      return "ESTABLISHED";
    case pfs::net_socket::net_state::fin_wait1:
      return "FIN_WAIT_1";
    case pfs::net_socket::net_state::fin_wait2:
      return "FIN_WAIT_2";
    case pfs::net_socket::net_state::last_ack:
      return "LAST_ACK";
    case pfs::net_socket::net_state::listen:
      return "LISTEN";
    case pfs::net_socket::net_state::syn_recv:
      return "SYN_RECEIVED";
    case pfs::net_socket::net_state::syn_sent:
      return "SYN_SENT";
    case pfs::net_socket::net_state::time_wait:
      return "TIME_WAIT";
  }
  return {};
}

auto to_socket(const pfs::net_socket& s, uint32_t pid, std::string comm,
               int protocol) -> net_socket {
  auto result = net_socket{};
  result.pid = pid;
  result.process_name = std::move(comm);
  result.protocol = protocol;
  if (auto addr = to<ip>(s.local_ip.to_string())) {
    result.local_addr = *addr;
  }
  result.local_port = s.local_port;
  if (auto addr = to<ip>(s.remote_ip.to_string())) {
    result.remote_addr = *addr;
  }
  result.remote_port = s.remote_port;
  result.state = to_string(s.socket_net_state);
  return result;
}

} // namespace

// TODO: Consider using the netlink API to list sockets instead.
auto linux_os::fetch_sockets() -> std::vector<net_socket> {
  auto result = std::vector<net_socket>{};
  // First build up a global map inode -> pid.
  struct pid_name_pair {
    pid_t pid;
    std::string name;
  };
  auto processes = std::unordered_map<ino_t, pid_name_pair>{};
  for (const auto& process : state_->procfs.get_processes()) {
    try {
      for (const auto& [id, fd] : process.get_fds()) {
        try {
          auto inode = fd.get_target_stat().st_ino;
          auto pid = process.id();
          processes.emplace(inode, pid_name_pair{
                                     .pid = pid,
                                     .name = process.get_comm(),
                                   });
        } catch (const std::system_error&) {
          // Ignore no longer existent processes.
          continue;
        }
      }
    } catch (const std::exception&) {
      // Ignore permission errors.
    }
  }
  // Go through the global list of sockets and use the built map
  // to associate socket -> pid.
  auto add = [&](auto&& sockets, int proto) {
    for (const auto& pfs_socket : sockets) {
      auto pd = processes[pfs_socket.inode];
      result.emplace_back(to_socket(pfs_socket, pd.pid, pd.name, proto));
    }
  };
  auto net = state_->procfs.get_net();
  add(net.get_icmp(), IPPROTO_ICMP);
  add(net.get_icmp6(), IPPROTO_ICMPV6);
  add(net.get_raw(), IPPROTO_RAW);
  add(net.get_raw6(), IPPROTO_RAW);
  add(net.get_tcp(), IPPROTO_TCP);
  add(net.get_tcp6(), IPPROTO_TCP);
  add(net.get_udp(), IPPROTO_UDP);
  add(net.get_udp6(), IPPROTO_UDP);
  add(net.get_udplite(), IPPROTO_UDPLITE);
  add(net.get_udplite6(), IPPROTO_UDPLITE);
  return result;
}

#elif TENZIR_MACOS

struct darwin_os::state {
  struct mach_timebase_info timebase{};
};

auto darwin_os::make() -> std::unique_ptr<darwin_os> {
  auto result = std::unique_ptr<darwin_os>(new darwin_os);
  if (mach_timebase_info(&result->state_->timebase) != KERN_SUCCESS) {
    TENZIR_ERROR("failed to get MACH timebase");
    return nullptr;
  }
  return result;
}

darwin_os::darwin_os() : state_{std::make_unique<state>()} {
}

darwin_os::~darwin_os() = default;

auto darwin_os::current_pid() -> int {
  return getpid();
}

auto darwin_os::fetch_processes(std::optional<int> pid_filter)
  -> std::vector<process> {
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
    if (pid <= 0) {
      continue;
    }
    if (pid_filter && pid != *pid_filter) {
      continue;
    }
    errno = 0;
    proc_bsdinfo proc{};
    auto n = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &proc, sizeof(proc));
    if (n < detail::narrow_cast<int>(sizeof(proc)) || errno != 0) {
      if (errno == ESRCH) { // process is gone
        continue;
      }
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
      .command_line = {},
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
      .peak_mem = {},
      .swap = {},
      .open_fds = {},
      .utime = {},
      .stime = {},
    };
    if (n >= detail::narrow_cast<int>(sizeof(task))) {
      p.vsize = task.pti_virtual_size;
      p.rsize = task.pti_resident_size;
      auto timebase = state_->timebase;
      auto utime = task.pti_total_user * timebase.numer / timebase.denom;
      auto stime = task.pti_total_system * timebase.numer / timebase.denom;
      p.utime = std::chrono::nanoseconds(utime);
      p.stime = std::chrono::nanoseconds(stime);
    }
    // Count open file descriptors.
    errno = 0;
    n = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, nullptr, 0);
    if (n > 0 && errno == 0) {
      p.open_fds = n / sizeof(proc_fdinfo);
    }
    result.push_back(std::move(p));
  }
  return result;
}

namespace {

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

} // namespace

auto darwin_os::fetch_sockets() -> std::vector<net_socket> {
  auto result = std::vector<net_socket>{};
  for (const auto& proc : fetch_processes()) {
    auto pid = detail::narrow_cast<uint32_t>(proc.pid);
    auto sockets = sockets_for(pid);
    result.insert(result.end(), std::make_move_iterator(sockets.begin()),
                  std::make_move_iterator(sockets.end()));
  }
  return result;
}

auto darwin_os::sockets_for(uint32_t pid) -> std::vector<net_socket> {
  auto p = detail::narrow_cast<pid_t>(pid);
  auto n = proc_pidinfo(p, PROC_PIDLISTFDS, 0, nullptr, 0);
  auto fds = std::vector<proc_fdinfo>{};
  fds.resize(n);
  n = proc_pidinfo(p, PROC_PIDLISTFDS, 0, fds.data(), n);
  if (n <= 0) {
    TENZIR_WARN("could not get file descriptors for process {}", p);
    return {};
  }
  auto result = std::vector<net_socket>{};
  for (auto& fd : fds) {
    if (fd.proc_fdtype != PROX_FDTYPE_SOCKET) {
      continue;
    }
    auto info = socket_fdinfo{};
    errno = 0;
    n = proc_pidfdinfo(p, fd.proc_fd, PROC_PIDFDSOCKETINFO, &info,
                       sizeof(socket_fdinfo));
    if (n < static_cast<int>(sizeof(socket_fdinfo)) or errno != 0) {
      continue;
    }
    // Only consider network connections.
    if (info.psi.soi_family != AF_INET and info.psi.soi_family != AF_INET6) {
      continue;
    }
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
    auto s = net_socket{};
    s.protocol = info.psi.soi_protocol;
    auto local_addr
      = to_string(info.psi.soi_family, info.psi.soi_proto.pri_in.insi_laddr);
    auto remote_addr
      = to_string(info.psi.soi_family, info.psi.soi_proto.pri_in.insi_faddr);
    if (auto addr = to<ip>(local_addr)) {
      s.local_addr = *addr;
    }
    s.local_port = ntohs(info.psi.soi_proto.pri_in.insi_lport);
    if (auto addr = to<ip>(remote_addr)) {
      s.remote_addr = *addr;
    }
    s.remote_port = ntohs(info.psi.soi_proto.pri_in.insi_fport);
    s.state = socket_state_to_string(info.psi.soi_protocol,
                                     info.psi.soi_proto.pri_tcp.tcpsi_state);
    result.push_back(std::move(s));
  }
  return result;
}

#endif

} // namespace tenzir
