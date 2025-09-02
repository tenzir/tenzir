//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/routes_manager_actor.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/session.hpp"
#include "tenzir/view.hpp"

#include <fmt/format.h>

namespace tenzir::plugins::routes {

static constexpr auto STATE_FILE = "routes/state.json";

routes_manager::routes_manager(routes_manager_actor::pointer self, filesystem_actor fs) : self_{self}, fs_{std::move(fs)} {
}

auto routes_manager::make_behavior() -> routes_manager_actor::behavior_type {
  // TODO: Check for data loss with startup order between?
  _restore_state();
  return {
    [this](atom::add, named_input_actor input)->caf::result<void> {
      return add(std::move(input));
    },
    [this](atom::add, named_output_actor output)->caf::result<void> {
      return add(std::move(output));
    },
    [this](atom::update, config cfg)->caf::result<void> {
      return update(std::move(cfg));
    },
    [this](atom::list)->caf::result<config> {
      return list();
    },
    [](atom::status, status_verbosity, duration) -> caf::result<record> {
      return record{};
    },
  };
}

auto routes_manager::add(named_input_actor input) -> caf::result<void> {
  if (inputs_.contains(input.name)) {
    return diagnostic::error("input `{}` already exists", input.name).to_error();
  }
  self_->monitor(input.handle, [this, name=input.name](const caf::error&) {
    inputs_.erase(name);
  });
  inputs_.emplace(input.name, std::move(input.handle));
  return {};
}

auto routes_manager::add(named_output_actor output) -> caf::result<void> {
  if (outputs_.contains(output.name)) {
    return diagnostic::error("output `{}` already exists", output.name).to_error();
  }
  self_->monitor(output.handle, [this, name=output.name](const caf::error&) {
    outputs_.erase(name);
  });
  outputs_.emplace(output.name, std::move(output.handle));
  // TODO: Mirror _run_for.
  _run_for(output.name);
  return {};
}

auto routes_manager::update(config cfg) -> caf::result<void> {
  // First persist the new config to the filesystem
  auto maybe_json = to_json(cfg.to_record());
  if (not maybe_json) {
    return diagnostic::error("failed to serialize config to JSON: {}", maybe_json.error()).to_error();
  }
  auto rp = self_->make_response_promise<void>();
  self_->mail(atom::write_v, std::filesystem::path{STATE_FILE}, chunk::make(std::move(*maybe_json)))
    .request(fs_, caf::infinite)
    .then(
      [this, rp, cfg = std::move(cfg)](atom::ok) mutable {
        // Only update the in-memory config after successful persistence
        cfg_ = std::move(cfg);
        TENZIR_DEBUG("successfully persisted routes config to state file");
        rp.deliver();
      },
      [rp](const caf::error& error) mutable {
        rp.deliver(error);
      });
  return rp;
}

auto routes_manager::list() -> caf::result<config> {
  return cfg_;
}

auto routes_manager::_restore_state() -> void {
  // TODO: Consider restoring from "self_.system().config()"
  // TODO: Store it in cfg2 or cfg?
  self_->mail(atom::read_v, std::filesystem::path{STATE_FILE})
    .request(fs_, caf::infinite)
    .await(
      [this](const chunk_ptr& chunk) {
        const auto content = std::string_view{reinterpret_cast<const char*>(chunk->data()), chunk->size()};
        const auto maybe_data = from_json(content);
        if (not maybe_data) {
          // TODO: Should this be a hard failure?
          TENZIR_WARN("failed to parse state JSON: {}", maybe_data.error());
          return;
        }
        const auto* maybe_record = try_as<record>(&*maybe_data);
        if (not maybe_record) {
          // TODO: Should this be a hard failure?
          TENZIR_WARN("state file does not contain a record");
          return;
        }
        auto dh = collecting_diagnostic_handler{};
        auto sp = session_provider::make(dh);
        auto maybe_config = config::make(make_view(*maybe_record), sp.as_session());
        if (not maybe_config) {
          // TODO: Should this be a hard failure?
          const auto diags = std::move(dh).collect();
          TENZIR_WARN("failed to restore config from state, encountered {} errors: ", diags.size());
          for (const auto& diag : diags) {
            TENZIR_WARN("- {}", diag.to_error());
          }
          return;
        }
        cfg_ = std::move(*maybe_config);
      },
      [](const caf::error& error) {
        if (error == ec::no_such_file) {
          return;
        }
        // TODO: Should this be a hard failure?
        TENZIR_WARN("failed to read state file: {}", error);
      });
}

auto routes_manager::_run_for(std::string input) -> void {
  // Find the input proxy actor
  const auto it = inputs_.find(input);
  if (it == inputs_.end()) {
    return;
  }
  // Get input from the corresponding proxy actor
  self_->mail(atom::get_v)
    .request(it->second, caf::infinite)
    .then(
      [this, input](table_slice slice) {
        TENZIR_ASSERT(slice.rows() != 0);
        self_->delay_fn([this, input]() mutable {
          _run_for(std::move(input));
        });
        for (auto output : _find_outputs(input)) {
          _forward(output, slice);
        }
      },
      [this, input](const caf::error& err) {
        // TODO: Should we really continue here?
        self_->delay_fn([this, input]() mutable {
          _run_for(std::move(input));
        });
        TENZIR_WARN("failed to get input from `{}`: {}", input, err);
      });
}

auto routes_manager::_find_outputs(std::string_view input) -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  for (const auto& connection : cfg_.connections) {
    if (connection.from == input) {
      result.emplace_back(connection.to);
    }
  }
  return result;
}

auto routes_manager::_forward(std::string_view output, table_slice slice) -> void {
  // We go through delay here to avoid stack overflows in the recursive
  // evaluation of routes.
  self_->delay_fn([this, output = std::string{output}, slice = std::move(slice)] {
    _inline_forward_to_outputs(output, slice);
    _inline_forward_to_routes(output, slice);
  });
}

auto routes_manager::_inline_forward_to_outputs(std::string_view output, table_slice slice) -> void {
  const auto it = outputs_.find(std::string{output});
  if (it == outputs_.end()) {
    return;
  }
  self_->mail(atom::put_v, slice).request(it->second, caf::infinite)
    .then(
      []() {
        // Successfully forwarded
      },
      [output = std::string{output}](const caf::error& err) {
        TENZIR_WARN("failed to forward to output `{}`: {}", output, err);
      });
}

auto routes_manager::_inline_forward_to_routes(std::string_view output, table_slice slice) -> void {
  for (const auto& [route_name, route] : cfg_.routes) {
    if (route.input == output) {
      // Evaluate the route's rules
      auto remaining_slice = slice;
      for (const auto& rule : route.rules) {
        auto [matched, unmatched] = rule.evaluate(remaining_slice);
        if (matched.rows() > 0) {
          _forward(rule.output, std::move(matched));
        }
        if (unmatched.rows() == 0) {
          break;
        }
        remaining_slice = unmatched;
      }
    }
  }
}

} // namespace tenzir::plugins::routes
