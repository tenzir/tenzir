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

#include <caf/type_list.hpp>
#include <caf/typed_actor.hpp>

namespace tenzir::exec {

struct checkpoint_reader_traits {
  using signatures = caf::type_list<
    // Restores a checkpoint for a given operator.
    // TOOD: Assign a name to the (id, index) pair used to identify an operator
    // here.
    auto(atom::get, uuid id, uint64_t index)->caf::result<chunk_ptr>>;
};

using checkpoint_reader_actor = caf::typed_actor<checkpoint_reader_traits>;

struct checkpoint_receiver_actor_traits {
  using signatures = caf::type_list<
    // Accepts and acknowledges checkpoints.
    auto(checkpoint, chunk_ptr)->caf::result<void>>;
};

using checkpoint_receiver_actor
  = caf::typed_actor<checkpoint_receiver_actor_traits>;

struct downstream_actor_traits {
  using signatures = caf::type_list<
    // TODO: Consider further batching `table_slice` for better performance in
    // case of high heterogeneity.
    // Must not be called if the downstream type is void.
    auto(atom::push, table_slice slice)->caf::result<void>,
    auto(atom::push, chunk_ptr chunk)->caf::result<void>,
    //
    auto(atom::persist, checkpoint)->caf::result<void>,
    // Used to notify that no more pushes will come.
    auto(atom::done)->caf::result<void>>;
};
using downstream_actor = caf::typed_actor<downstream_actor_traits>;

struct upstream_actor_traits {
  using signatures = caf::type_list<
    // Request more items. Must not be called if the upstream type is void.
    auto(atom::pull, uint64_t items)->caf::result<void>,
    // Handler for when the downstream operator declares that it doesn't need
    // any more input. If an operator receives this should only forward
    // checkpoints from that moment on, as the actual output is no longer
    // relevant and will be ignored.
    auto(atom::stop)->caf::result<void>>;
};
using upstream_actor = caf::typed_actor<upstream_actor_traits>;

struct shutdown_actor_traits {
  using signatures = caf::type_list<
    // Call this when the sender is ready to shutdown.
    auto(atom::shutdown)->caf::result<void>>;
};
using shutdown_actor = caf::typed_actor<shutdown_actor_traits>;

struct connect_t {
  upstream_actor upstream;
  downstream_actor downstream;
  checkpoint_receiver_actor checkpoint_receiver;
  shutdown_actor shutdown;

  friend auto inspect(auto& f, connect_t& x) -> bool {
    return f.object(x).fields(
      f.field("upstream", x.upstream), f.field("downstream", x.downstream),
      f.field("checkpoint_receiver", x.checkpoint_receiver),
      f.field("shutdown", x.shutdown));
  }
};

struct operator_actor_traits {
  using signatures = caf::type_list<
    // Initialize this operator with everything it needs.
    auto(connect_t connect)->caf::result<void>,
    // Notification when all operators in this pipeline were connected. Note
    // that this is not guaranteed to arrive before messages from upstream and
    // downstream operators!
    auto(atom::start)->caf::result<void>,
    //
    auto(atom::commit)->caf::result<void>
    // Support the other interfaces.
    >::append_from<upstream_actor::signatures, downstream_actor::signatures>;
};
using operator_actor = caf::typed_actor<operator_actor_traits>;

struct subpipeline_actor_traits {
  using signatures = caf::type_list<>::append_from<operator_actor::signatures>;
};
using subpipeline_actor = caf::typed_actor<subpipeline_actor_traits>;

struct pipeline_actor_traits {
  using signatures = caf::type_list<
    // Start the execution. Returns when start has completed.
    auto(atom::start)->caf::result<void>>;
};
using pipeline_actor = caf::typed_actor<pipeline_actor_traits>;

} // namespace tenzir::exec
