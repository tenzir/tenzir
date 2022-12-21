//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/pipeline.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/sink.hpp"
#include "vast/table_slice.hpp"

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/settings.hpp>
#include <caf/stream_stage.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <memory>

namespace vast::system {

using transformer_stream_stage_ptr
  = caf::stream_stage_ptr<detail::framed<table_slice>,
                          caf::broadcast_downstream_manager<table_slice>>;

struct transformer_state {
  /// The pipelines that can be applied.
  pipeline_executor executor;

  /// The stream stage.
  transformer_stream_stage_ptr stage;

  /// Name of this transformer.
  std::string transformer_name;

  /// Whether the source requires us to shutdown the stream stage.
  /// This will usually be the case for transformers attached to node
  /// components with persistent stream stages, ie. the importer.
  bool source_requires_shutdown;

  /// The cached status response.
  record status;

  bool reassign_offset_ranges;

  /// Name of the TRANSFORMER actor type.
  static constexpr const char* name = "transformer";
};

/// An actor containing a pipeline_stream_stage, which is just a stream
/// stream stage that applies a `pipeline_executor` to every table slice.
/// @param self The actor handle.
/// @param pipelines The set of pipelines to be applied.
transformer_actor::behavior_type
transformer(transformer_actor::stateful_pointer<transformer_state> self,
            std::string name, std::vector<pipeline>&&);

/// A transformer actor that is attached to a system component. This reassigns
/// correct offsets to the transformed table slices.
transformer_actor::behavior_type
importer_transformer(transformer_actor::stateful_pointer<transformer_state> self,
                     std::string name, std::vector<pipeline>&&);

/// An actor that hosts a no-op stream sink for table slices, that the SOURCE
/// and IMPORTER attach to their respective TRANSFORMER actors on shutdown.
/// @param self The parent actor handle.
/// @note This serves to fix a possible deadlock in high-load situations during
/// shutdown: Given three actors A, B, and C that host a stream A -> B -> C,
/// shutting down A and C before B is done streaming may cause B to stall. This
/// is problematic for the TRANSFORMER, which is shut down via a EOF on the
/// stream instead of a regular message. As a workaround, we let the SOURCE and
/// IMPORTER attach a dummy sink to the TRANSFORMER on shutdown.
stream_sink_actor<table_slice>::behavior_type
dummy_transformer_sink(stream_sink_actor<table_slice>::pointer self);

} // namespace vast::system
