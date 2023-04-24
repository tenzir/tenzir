//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/specialization_of.hpp"

#include <caf/stream_stage.hpp>

namespace vast::detail {

/// Flushes and shuts down a `caf::stream_stage` connected to
/// a `caf::broadcast_downstream_manager` as diligently as possible
/// without a continuation. Note that this version still has a race
/// if an upstream has already sent a message that hasn't been placed
/// in the inbound queue yet when this function runs.
template <typename In,
          detail::specialization_of<caf::broadcast_downstream_manager>
            DownstreamManager>
void shutdown_stream_stage(caf::stream_stage_ptr<In, DownstreamManager>& stage) {
  if (!stage)
    return;
  // First we call `shutdown()` to notify all upstream connections
  // that this stage is closed and will not accept any new messages.
  stage->shutdown();
  // Then, we copy all data from the global input buffer to each
  // path-specific output buffer.
  stage->out().fan_out_flush();
  // Next, we `close()` the outbound paths to notify downstream
  // connections that this stage is closed.
  // This will remove all clean outbound paths, so we need to call
  // `fan_out_flush()` before, but it will keep all paths that still
  // have data. No new data will be pushed from the global buffer to
  // closing paths.
  stage->out().close();
  // Finally we call `force_emit_batches()` to move messages from the
  // outbound path buffers to the inboxes of the receiving actors.
  // The 'force_' here means that caf should ignore the batch size
  // and capacity of the channel and push both overfull and underfull
  // batches. Technically just `emit_batches()` would have the same
  // effect since the buffered downstream manager always forces batches
  // if all paths are closing.
  stage->out().force_emit_batches();
}

} // namespace vast::detail
