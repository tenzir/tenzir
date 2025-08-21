//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/exec/actors.hpp"
#include "tenzir/exec/pipeline_settings.hpp"
#include "tenzir/plan/pipeline.hpp"

namespace tenzir::exec {

/// Create a new pipeline executor.
///
/// If `checkpoint_reader` is set, then the pipeline will be restored.
auto make_pipeline(plan::pipeline pipe, pipeline_settings settings,
                   std::optional<checkpoint_reader_actor> checkpoint_reader,
                   base_ctx ctx) -> pipeline_actor;

} // namespace tenzir::exec
