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

#undef linux
#undef unix

namespace tenzir {

/// An operating system process.
struct process {
  std::string name;
  std::string command_line;
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

  /// Provides a snapshot of all currently running processes.
  auto processes() -> table_slice;

  /// Provides a snapshot of all open sockets.
  auto sockets() -> table_slice;

protected:
  virtual auto fetch_processes() -> std::vector<process> = 0;
  virtual auto fetch_sockets() -> std::vector<socket> = 0;

  // virtual auto sockets_for(uint32_t pid) -> std::vector<socket> = 0;
};

#if TENZIR_LINUX

/// An abstraction of Linux.
class linux final : public os {
public:
  static auto make() -> std::unique_ptr<linux>;

  ~linux() final;

  auto fetch_processes() -> std::vector<process> final;
  auto fetch_sockets() -> std::vector<socket> final;

private:
  linux();

  struct state;
  std::unique_ptr<state> state_;
};

#elif TENZIR_MACOS

/// An abstraction of macOS.
class darwin final : public os {
public:
  static auto make() -> std::unique_ptr<darwin>;

  ~darwin() final;

  auto fetch_processes() -> std::vector<process> final;
  auto fetch_sockets() -> std::vector<socket> final;

  auto sockets_for(uint32_t pid) -> std::vector<socket>;

private:
  darwin();

  struct state;
  std::unique_ptr<state> state_;
};

#endif

} // namespace tenzir
