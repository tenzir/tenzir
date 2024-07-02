//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

auto exec2(std::string content, std::unique_ptr<diagnostic_handler> diag,
           const exec_config& cfg, caf::actor_system& sys) -> bool;

auto prepare_pipeline(ast::pipeline&& pipe, session ctx) -> tenzir::pipeline;

} // namespace tenzir
