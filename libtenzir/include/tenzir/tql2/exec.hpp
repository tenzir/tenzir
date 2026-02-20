//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/executor.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/tql2/ast.hpp"

#include <string_view>

namespace tenzir {

auto exec2(std::string_view source, diagnostic_handler& dh,
           const exec_config& cfg, caf::actor_system& sys) -> bool;

auto compile(ast::pipeline&& pipe, session ctx) -> failure_or<pipeline>;

auto parse_and_compile(std::string_view source, session ctx)
  -> failure_or<pipeline>;

/// Run a closed pipeline from a list of operators.
auto run_plan(std::vector<AnyOperator> ops, caf::actor_system& sys,
              DiagHandler& dh, std::optional<std::string> const& profile_path,
              metrics_callback emit_fn = {}) -> Task<failure_or<void>>;

} // namespace tenzir
