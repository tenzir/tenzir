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

namespace tenzir {

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
  virtual auto processes() -> table_slice = 0;

  /// Provides a snapshot of all open sockets.
  virtual auto sockets() -> table_slice = 0;
};

#if TENZIR_MACOS

/// An abstraction of macOS.
class darwin final : public os {
public:
  static auto make() -> std::unique_ptr<darwin>;

  ~darwin() final;

  auto processes() -> table_slice final;

  auto sockets() -> table_slice final;

private:
  darwin();

  struct state;
  std::unique_ptr<state> state_;
};

#endif

} // namespace tenzir
