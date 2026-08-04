//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace tenzir::plugins::iceberg {

/// The checkpoint-restored facts `start()` consults to decide how to treat
/// the catalog state it finds after a restart.
struct RestoredState {
  /// A checkpoint was restored before `start()` ran.
  bool restored = false;
  /// The operator created the table itself in an earlier incarnation.
  bool created_table = false;
  /// Number of commit epochs that already landed at the catalog.
  uint64_t commit_seq = 0;
  /// Number of closed-but-uncommitted data file handles in the checkpoint.
  size_t restored_files = 0;
};

/// Decides whether `mode="create"` may treat a table that already exists at
/// start as its own previous work and resume appending to it. A restored
/// checkpoint alone proves nothing: it can predate the first input, in which
/// case an externally created table of the same name must still raise the
/// create-mode conflict. Only checkpoint state showing that this operator
/// created or wrote the table lifts the conflict.
constexpr auto may_resume_existing_table(RestoredState const& state) -> bool {
  return state.restored
         and (state.created_table or state.commit_seq > 0
              or state.restored_files > 0);
}

/// Decides whether a table that no longer exists at start is fatal. Once the
/// checkpoint records committed epochs or holds uncommitted file handles,
/// recreating a fresh table from post-checkpoint input would silently lose
/// the rows written before the checkpoint. Before the first write, a missing
/// table is indistinguishable from a fresh start and gets recreated.
constexpr auto missing_table_is_fatal(RestoredState const& state) -> bool {
  return state.commit_seq > 0 or state.restored_files > 0;
}

/// Decides whether the table found at start is a different table than the
/// one the checkpoint wrote to. Iceberg mints a table UUID at creation that
/// survives renames and metadata updates; a mismatch means the table was
/// dropped and recreated in between, so the rows committed before the
/// checkpoint are gone and resuming would silently continue on the
/// impostor. A checkpoint that never saw a table records no UUID and
/// matches anything.
constexpr auto restored_table_identity_conflict(std::string_view recorded_uuid,
                                                std::string_view current_uuid)
  -> bool {
  return not recorded_uuid.empty() and recorded_uuid != current_uuid;
}

} // namespace tenzir::plugins::iceberg
