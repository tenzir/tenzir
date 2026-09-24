//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/try.hpp"

#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>

namespace tenzir::plugins::clickhouse {

// Group owns references to original slices until its write is acknowledged.
// Only definite schema rejections permit one local remap and retry.
template <class Group, class Write, class Refresh, class Prepare>
auto drain_pending(diagnostic_handler& dh, std::vector<Group> pending,
                   Write write, Refresh refresh, Prepare prepare)
  -> failure_or<void> {
  auto retried = false;
  auto next = size_t{0};
  while (next < pending.size()) {
    try {
      auto& group = pending[next];
      TRY(write(group));
      // Release acknowledged input immediately. It is never part of a retry.
      group.originals.clear();
      ++next;
    } catch (::clickhouse::ServerException const& error) {
      using enum ::clickhouse::ErrorCodes;
      auto const code = error.GetCode();
      // Only schema rejections known to occur before a block is accepted are
      // replayable. Network, timeout, and arbitrary execution errors are not.
      auto const rejected = code == NO_SUCH_COLUMN_IN_TABLE
                            or code == NOT_FOUND_COLUMN_IN_BLOCK
                            or code == TYPE_MISMATCH;
      if (retried or not rejected) {
        throw;
      }
      auto changed = [&]() -> failure_or<bool> {
        try {
          return refresh();
        } catch (std::exception const& refresh_error) {
          diagnostic::error("metadata refresh failed: {}", refresh_error.what())
            .emit(dh);
          return failure::promise();
        }
      }();
      if (not changed.is_success()) {
        // The refresh already emitted its diagnostic. Report the rejection
        // that caused it as well, without replaying any pending input.
        diagnostic::error("insertion rejected: {}", error.what()).emit(dh);
        return failure::promise();
      }
      if (not *changed) {
        throw;
      }
      auto originals = std::vector<table_slice>{};
      for (auto i = next; i < pending.size(); ++i) {
        for (auto& original : pending[i].originals) {
          originals.push_back(std::move(original));
        }
      }
      pending.clear();
      next = 0;
      retried = true;
      TRY(pending, prepare(originals));
    }
  }
  return {};
}

} // namespace tenzir::plugins::clickhouse
