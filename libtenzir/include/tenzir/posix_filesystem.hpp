//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/weak_handle.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

namespace tenzir {

/// The state for the POSIX filesystem.
/// @relates posix_filesystem
struct posix_filesystem_state {
  /// The filesystem root.
  std::filesystem::path root = {};

  /// The actor name.
  static inline const char* name = "posix-filesystem";

  // Rename a file and update statistics.
  caf::expected<atom::done> rename_single_file(const std::filesystem::path&,
                                               const std::filesystem::path&);
};

/// A filesystem implemented with POSIX system calls.
/// @param self The actor handle.
/// @param root The filesystem root. The actor prepends this path to all
///             operations that include a path parameter.
/// @returns The actor behavior.
filesystem_actor::behavior_type
posix_filesystem(filesystem_actor::stateful_pointer<posix_filesystem_state> self,
                 std::filesystem::path root);

} // namespace tenzir
