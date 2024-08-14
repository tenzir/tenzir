//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/fwd.hpp>

namespace tenzir {

struct exec_config {
  std::string implicit_bytes_source = "load file -";
  std::string implicit_events_source = "from stdin read json";
  std::string implicit_bytes_sink = "save file -";
  std::string implicit_events_sink = "to stdout write json";
  bool dump_tokens = false;
  bool dump_ast = false;
  bool dump_pipeline = false;
  bool dump_diagnostics = false;
  bool dump_metrics = false;
  bool tql2 = false;
  bool strict = false;
};

auto exec_pipeline(std::string content, diagnostic_handler& dh,
                   const exec_config& cfg, caf::actor_system& sys)
  -> caf::expected<void>;

auto exec_pipeline(pipeline pipe, diagnostic_handler& dh,
                   const exec_config& cfg, caf::actor_system& sys)
  -> caf::expected<void>;

} // namespace tenzir
