//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/pipeline_executor.hpp>
#include <vast/plugin.hpp>

#include <caf/scoped_actor.hpp>

namespace vast::plugins::exec {

namespace {

auto exec_command(const invocation& inv, caf::actor_system& sys)
  -> caf::expected<void> {
  if (inv.arguments.size() != 1)
    return caf::make_error(ec::invalid_argument,
                           fmt::format("expected exactly one argument, but got "
                                       "{}",
                                       inv.arguments.size()));
  auto pipeline = pipeline::parse(inv.arguments[0]);
  if (not pipeline) {
    return caf::make_error(ec::invalid_argument,
                           fmt::format("failed to parse pipeline: {}",
                                       pipeline.error()));
  }
  // If the pipeline ends with events, we implicitly write the output as JSON
  // to stdout, and if it ends with bytes, we implicitly write those bytes to
  // stdout.
  while (true) {
    if (auto out = pipeline->infer_type<void>()) {
      if (out->is<void>()) {
        break;
      }
      if (out->is<table_slice>()) {
        auto op = pipeline::parse_as_operator("write json --pretty");
        if (not op) {
          return caf::make_error(ec::invalid_argument,
                                 fmt::format("failed to append implicit 'write "
                                             "json --pretty': {}",
                                             op.error()));
        }
        pipeline->push_back(std::move(*op));
        break;
      }
      if (out->is<chunk_ptr>()) {
        auto op = pipeline::parse_as_operator("save file -");
        if (not op) {
          return caf::make_error(ec::invalid_argument,
                                 fmt::format("failed to append implicit 'save "
                                             "file -': {}",
                                             op.error()));
        }
        pipeline->push_back(std::move(*op));
        break;
      }
    }
    if (auto out = pipeline->infer_type<table_slice>()) {
      auto op = pipeline::parse_as_operator("read json");
      if (not op) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("failed to prepend implicit 'read "
                                           "json': {}",
                                           op.error()));
      }
      pipeline->push_front(std::move(*op));
      continue;
    }
    if (auto out = pipeline->infer_type<chunk_ptr>()) {
      auto op = pipeline::parse_as_operator("load file -");
      if (not op) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("failed to prepend implicit 'load "
                                           "file -': {}",
                                           op.error()));
      }
      pipeline->push_front(std::move(*op));
      continue;
    }
  }
  VAST_ASSERT(pipeline->is_closed());
  caf::scoped_actor self{sys};
  auto executor = self->spawn(pipeline_executor, std::move(*pipeline));
  auto result = caf::expected<void>{};
  // TODO: This command should probably implement signal handling, and check
  // whether a signal was raised in every iteration over the executor. This
  // will likely be easier to implement once we switch to the actor-based
  // asynchronous executor, so we may as well wait until then.
  self->request(executor, caf::infinite, atom::run_v)
    .receive(
      [] {
        VAST_DEBUG("exec command finished pipeline execution");
      },
      [&](caf::error& error) {
        result = std::move(error);
      });
  return result;
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "exec";
  }

  auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory> override {
    auto exec = std::make_unique<command>("exec", "execute a pipeline locally",
                                          command::opts("?vast.exec"));
    auto factory = command::factory{
      {"exec",
       [=](const invocation& inv, caf::actor_system& sys) -> caf::message {
         auto result = exec_command(inv, sys);
         if (not result)
           return caf::make_message(result.error());
         return {};
       }},
    };
    return {std::move(exec), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::exec

VAST_REGISTER_PLUGIN(vast::plugins::exec::plugin)
