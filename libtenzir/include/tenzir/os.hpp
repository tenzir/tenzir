//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/config.hpp>
#include <tenzir/table_slice_builder.hpp>

#include <algorithm>

#ifdef TENZIR_MACOS
#  include <mach/mach_time.h>
#endif

namespace tenzir {

/// A type representing an OS process.
auto process_type() -> type;

/// A type representing an OS process.
auto socket_type() -> type;

/// A platform-independent operating system.
class os {
public:
  virtual ~os() = default;

  /// Provides a snapshot of all currently running processes.
  virtual auto processes() -> table_slice = 0;

  /// Provides a snapshot of all open sockets.
  virtual auto sockets() -> table_slice = 0;
};

#ifdef TENZIR_MACOS

class darwin final : public os {
public:
  static auto make() -> std::unique_ptr<darwin>;

  auto processes() -> table_slice final;

  auto sockets() -> table_slice final;

private:
  struct mach_timebase_info timebase_ {};
};

#endif

} // namespace tenzir
