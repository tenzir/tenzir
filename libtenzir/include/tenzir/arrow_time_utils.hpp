//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/fwd.hpp>

#include <arrow/compute/api_scalar.h>

namespace tenzir {

/// Converts a duration into the options required for Arrow Compute's
/// {Round,Floor,Ceil}Temporal functions.
/// @param time_resolution The multiple to round to.
/// The configuration of a summarize pipeline operator.
auto make_round_temporal_options(duration time_resolution) noexcept
  -> arrow::compute::RoundTemporalOptions;

} // namespace tenzir
