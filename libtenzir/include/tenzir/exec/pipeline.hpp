//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/bp.hpp"
#include "tenzir/exec/checkpoint_reader.hpp"
#include "tenzir/exec/pipeline_settings.hpp"

namespace tenzir::exec {

struct pipeline_actor_traits {
  using signatures = caf::type_list<
    //
    auto(atom::start)->caf::result<void>,
    //
    auto(atom::start, handshake hs)->caf::result<handshake_response>>;
};

using pipeline_actor = caf::typed_actor<pipeline_actor_traits>;

/// Create a new pipeline executor.
///
/// If `checkpoint_reader` is set, then the pipeline will be restored.
auto make_pipeline(bp::pipeline pipe, pipeline_settings settings,
                   std::optional<checkpoint_reader_actor> checkpoint_reader,
                   base_ctx ctx) -> pipeline_actor;

} // namespace tenzir::exec
