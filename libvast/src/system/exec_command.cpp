//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/exec_command.hpp"

#include "vast/command.hpp"
#include "vast/system/make_pipelines.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>

using namespace caf;

namespace vast::system {

caf::message exec_command(const invocation& inv, caf::actor_system& sys) {
  auto pipeline = make_pipeline(inv.arguments.front());
  std::vector<table_slice> slices;
  for (const auto& pipeline_op : pipeline.cvalue()) {
    auto transformed = pipeline_op->finish();
    if (!transformed) {
      return caf::make_message(transformed.error());
    }
    std::vector<table_slice> result_slices;
    for (const auto& batch : *transformed) {
      result_slices.emplace_back(batch.batch);
    }
    slices = std::move(result_slices);
  }
  return caf::make_message("ok");
}

} // namespace vast::system
