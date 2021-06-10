//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"
#include "vast/system/sink.hpp"
#include "vast/table_slice.hpp"
#include "vast/transform.hpp"

#include <caf/settings.hpp>
#include <caf/stream_stage.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <memory>

namespace vast::system {

using transformer_stream_stage_ptr = caf::stream_stage_ptr<
  stream_controlled<table_slice>,
  caf::broadcast_downstream_manager<stream_controlled<table_slice>>>;

struct transformer_state {
  /// The transforms that can be applied.
  transformation_engine transforms;

  /// The stream stage.
  transformer_stream_stage_ptr stage;

  /// Name of this transformer.
  std::string transformer_name;

  /// The cached status response.
  caf::settings status;

  /// Name of the TRANSFORMER actor type.
  static constexpr const char* name = "transformer";
};

/// An actor containing a transform_stream_stage, which is just a stream
/// stream stage that applies a `transformation_engine` to every table slice.
/// @param self The actor handle.
transformer_actor::behavior_type
transformer(transformer_actor::stateful_pointer<transformer_state> self,
            std::string name, std::vector<transform>&&);

} // namespace vast::system
