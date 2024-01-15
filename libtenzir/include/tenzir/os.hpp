//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/config.hpp"
#include "tenzir/ip.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace tenzir {

/// An operating system process.
struct process {
  std::string name;
  std::vector<std::string> command_line;
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
  std::optional<uint64_t> peak_mem;
  std::optional<uint64_t> swap;
  std::optional<uint64_t> open_fds;
  std::optional<duration> utime;
  std::optional<duration> stime;
};

/// A network socket.
struct socket {
  uint32_t pid;
  std::string process_name;
  int protocol;
  ip local_addr;
  uint16_t local_port;
  ip remote_addr;
  uint16_t remote_port;
  std::string state;
};

/// A type representing an OS process.
auto process_type() -> type;

/// A type representing an OS process.
auto socket_type() -> type;

/// A platform-independent operating system.
class os {
public:
  static auto make() -> std::unique_ptr<os>;

  virtual ~os() = default;

  /// Provides information about the current process.
  auto current_process() -> process;

  /// Provides a snapshot of all currently running processes.
  auto processes() -> table_slice;

  /// Provides a snapshot of all open sockets.
  auto sockets() -> table_slice;

protected:
  virtual auto current_pid() -> int = 0;
  virtual auto fetch_processes(std::optional<int> pid_filter = std::nullopt)
    -> std::vector<process>
    = 0;
  virtual auto fetch_sockets() -> std::vector<socket> = 0;
};

#if TENZIR_LINUX

/// An abstraction of linux_os.
class linux_os final : public os {
public:
  static auto make() -> std::unique_ptr<linux_os>;

  ~linux_os() final;

  auto current_pid() -> int final;
  auto fetch_processes(std::optional<int> pid_filterstd = std::nullopt)
    -> std::vector<process> final;
  auto fetch_sockets() -> std::vector<socket> final;

private:
  linux_os();

  struct state;
  std::unique_ptr<state> state_;
};

#elif TENZIR_MACOS

/// An abstraction of macOS.
class darwin_os final : public os {
public:
  static auto make() -> std::unique_ptr<darwin_os>;

  ~darwin_os() final;

  auto current_pid() -> int final;
  auto fetch_processes(std::optional<int> pid_filter = std::nullopt)
    -> std::vector<process> final;
  auto fetch_sockets() -> std::vector<socket> final;

private:
  darwin_os();

  auto sockets_for(uint32_t pid) -> std::vector<socket>;

  struct state;
  std::unique_ptr<state> state_;
};

#endif

} // namespace tenzir
