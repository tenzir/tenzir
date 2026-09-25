//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#include "tenzir/nova_active_partition.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/error.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/taxonomies.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <unordered_map>
#include <utility>

namespace tenzir {

auto nova_active_partition(
  nova_active_partition_actor::stateful_pointer<nova_active_partition_state>
    self,
  std::string name, bool internal, partition_paths paths,
  filesystem_actor filesystem, caf::settings index_opts,
  index_config synopsis_opts, store_actor_plugin const* store_plugin,
  std::shared_ptr<taxonomies> taxonomies)
  -> nova_active_partition_actor::behavior_type {
  auto& state = self->state();
  state.self = self;
  state.conversion.emplace(std::move(name), internal);
  state.paths = std::move(paths);
  state.filesystem = std::move(filesystem);
  state.index_opts = std::move(index_opts);
  state.synopsis_opts = std::move(synopsis_opts);
  state.store_plugin = store_plugin;
  state.taxonomies = std::move(taxonomies);
  return {
    [self](nova::Events& events,
           nova::storage::BitMap& selection) -> caf::result<void> {
      auto& state = self->state();
      if (state.persisting) {
        return caf::make_error(ec::logic_error,
                               "cannot append to a sealing partition");
      }
      auto result = state.conversion->add(std::move(events), selection);
      if (not result) {
        return caf::make_error(ec::type_clash, std::move(result).unwrap_err());
      }
      return {};
    },
    [self](atom::get) -> std::vector<nova::Events> {
      auto result = std::vector<nova::Events>{};
      for (auto& [schema, events] :
           self->state().conversion->selected_events()) {
        if (not self->state().published.contains(schema)) {
          result.push_back(std::move(events));
        }
      }
      return result;
    },
    [self](atom::update, type const& schema) {
      self->state().published.insert(schema);
    },
    [self](atom::persist) -> caf::result<NovaPersistResult> {
      auto& state = self->state();
      if (std::exchange(state.persisting, true)) {
        return caf::make_error(ec::logic_error,
                               "partition persistence already started");
      }
      auto converted = state.conversion->snapshot();
      if (not converted) {
        return caf::make_error(ec::type_clash,
                               std::move(converted).unwrap_err());
      }
      auto grouped = std::unordered_map<type, std::vector<table_slice>>{};
      for (auto& slice : converted.unwrap()) {
        grouped[slice.schema()].push_back(std::move(slice));
      }
      if (grouped.empty()) {
        return NovaPersistResult{};
      }
      struct Completion {
        caf::typed_response_promise<NovaPersistResult> promise;
        NovaPersistResult result;
        size_t remaining;
      };
      auto completion = std::make_shared<Completion>(Completion{
        self->make_response_promise<NovaPersistResult>(), {}, grouped.size()});
      auto complete_one = [completion] {
        if (--completion->remaining == 0) {
          completion->promise.deliver(std::move(completion->result));
        }
      };
      for (auto& [schema, slices] : grouped) {
        auto id = uuid::random();
        auto child = self->spawn(active_partition, schema, id, state.filesystem,
                                 state.index_opts, state.synopsis_opts,
                                 state.store_plugin, state.taxonomies);
        state.children.push_back(child);
        for (auto& slice : slices) {
          self->mail(std::move(slice)).send(child);
        }
        self
          ->mail(atom::persist_v, state.paths.partition(id),
                 state.paths.synopsis(id))
          .request(child, caf::infinite)
          .then(
            [completion, complete_one, id](partition_synopsis_ptr synopsis) {
              completion->result.outputs.push_back(
                NovaPersistResult::Output{id, std::move(synopsis), caf::none});
              complete_one();
            },
            [completion, complete_one, id](caf::error error) {
              completion->result.outputs.push_back(
                NovaPersistResult::Output{id, {}, std::move(error)});
              complete_one();
            });
      }
      return completion->promise;
    },
    [](atom::status, status_verbosity, duration) -> record {
      return {};
    },
    [self](caf::exit_msg const& message) {
      for (auto const& child : self->state().children) {
        self->send_exit(child, message.reason.valid()
                                 ? message.reason
                                 : caf::exit_reason::normal);
      }
      self->quit(message.reason);
    },
  };
}

} // namespace tenzir
