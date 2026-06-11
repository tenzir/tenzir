//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/pipeline.hpp"

#include <string>

namespace tenzir {

struct pipeline_executor_state {
  static constexpr auto name = "pipeline-executor";

  pipeline_executor_actor::pointer self = {};
};

/// Compatibility stub for the removed legacy pipeline executor.
auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline pipe, std::string definition, receiver_actor<diagnostic> diagnostics,
  metrics_receiver_actor metrics, node_actor node, bool has_terminal,
  bool is_hidden, std::string pipeline_id)
  -> pipeline_executor_actor::behavior_type;

} // namespace tenzir
