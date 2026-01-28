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

constexpr inline auto make_default_implicit_events_sink(bool color)
  -> std::string {
  return color ? R"(write_tql color=true | save_stdout)"
               : R"(write_tql | save_stdout)";
}

struct exec_config {
  std::string implicit_bytes_source = R"(load_stdin)";
  std::string implicit_events_source = R"(load_stdin | read_json)";
  std::string implicit_bytes_sink = R"(save_stdout)";
  std::string implicit_events_sink = make_default_implicit_events_sink(false);
  bool dump_tokens = false;
  bool dump_ast = false;
  bool dump_pipeline = false;
  bool dump_diagnostics = false;
  bool dump_metrics = false;

  bool dump_ir = false;
  bool dump_inst_ir = false;
  bool dump_opt_ir = false;

  bool multi = false;
  bool legacy = false;
  bool strict = false;
  bool neo = false;
};

auto exec_pipeline(std::string content, diagnostic_handler& dh,
                   const exec_config& cfg, caf::actor_system& sys)
  -> caf::expected<void>;

auto exec_pipeline(pipeline pipe, std::string definition,
                   diagnostic_handler& dh, const exec_config& cfg,
                   caf::actor_system& sys) -> caf::expected<void>;

} // namespace tenzir
