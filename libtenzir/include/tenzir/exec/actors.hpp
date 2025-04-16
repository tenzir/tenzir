//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/exec/handshake.hpp"

#include <caf/type_list.hpp>

namespace tenzir::exec {

struct checkpoint_reader_traits {
  using signatures = caf::type_list<
    // Restores a checkpoint for a given operator.
    // TOOD: Assign a name to the (id, index) pair used to identify an operator
    // here.
    auto(uuid id, uint64_t index)->caf::result<chunk_ptr>>;
};

using checkpoint_reader_actor = caf::typed_actor<checkpoint_reader_traits>;

// TODO: is this the rollback manager?
struct checkpoint_receiver_actor_traits {
  using signatures = caf::type_list<
    // Accepts and acknowledges checkpoints.
    auto(checkpoint, chunk_ptr)->caf::result<void>>;
};

using checkpoint_receiver_actor
  = caf::typed_actor<checkpoint_receiver_actor_traits>;

struct stop_handler_actor_traits {
  using signatures = caf::type_list<
    // Handler for when an operator declares that it doesn't need any more
    // input. If an operator receives this from a downstream operator it should
    // cause the operator to only forward checkpoints from that moment on, as
    // the actual output is no longer relevant and will be ignored.
    auto(atom::stop)->caf::result<void>>;
};

using stop_handler_actor = caf::typed_actor<stop_handler_actor_traits>;

struct shutdown_handler_actor_traits {
  using signatures = caf::type_list<
    // Signal that the operator is shutting down.
    auto(atom::done)->caf::result<void>>;
};

using shutdown_handler_actor = caf::typed_actor<shutdown_handler_actor_traits>;

struct operator_actor_traits {
  using signatures = caf::type_list<
    // Initial setup.
    auto(handshake hs)->caf::result<handshake_response>,
    // Post-commit.
    auto(checkpoint cp)->caf::result<void>>
    // Signal that the actual output is no longer relevant, only checkpoints.
    ::append_from<stop_handler_actor_traits::signatures>;
};

using operator_actor = caf::typed_actor<operator_actor_traits>;

struct pipeline_actor_traits {
  using signatures = caf::type_list<
    // Starts the pipeline, returning after the pipeline's startup sequence has
    // completed. This handler requires the pipeline to be closed.
    auto(atom::start)->caf::result<void>,
    // Starts the pipeline with an existing handshake. The handshake's type must
    // match the pipeline's input type. The handler returns the handshake from
    // the pipeline's last operator.
    auto(atom::start, handshake hs)->caf::result<handshake_response>>;
};

using pipeline_actor = caf::typed_actor<pipeline_actor_traits>;

} // namespace tenzir::exec
