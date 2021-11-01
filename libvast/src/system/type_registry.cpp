//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/type_registry.hpp"

#include "vast/as_bytes.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/error.hpp"
#include "vast/event_types.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"
#include "vast/logger.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"
#include "vast/taxonomies.hpp"

#include <caf/attach_stream_sink.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/expected.hpp>

namespace vast::system {

report type_registry_state::telemetry() const {
  // TODO: Generate a status report for the accountant.
  return {};
}

record type_registry_state::status(status_verbosity v) const {
  auto result = record{};
  if (v >= status_verbosity::detailed) {
    // The list of defined concepts
    if (v >= status_verbosity::debug) {
      // TODO: Replace with a generic to data converter.
      auto to_list = [](concepts::range auto xs) {
        list l;
        for (const auto& x : xs)
          l.emplace_back(x);
        return l;
      };
      auto concepts_status = list{};
      for (const auto& [name, definition] : taxonomies.concepts) {
        auto concept_status = record{};
        concept_status["name"] = name;
        concept_status["description"] = definition.description;
        concept_status["fields"] = to_list(definition.fields);
        concept_status["concepts"] = to_list(definition.concepts);
        concepts_status.push_back(std::move(concept_status));
      }
      result["concepts"] = std::move(concepts_status);
      auto models_status = list{};
      for (const auto& [name, definition] : taxonomies.models) {
        auto model_status = record{};
        model_status["name"] = name;
        model_status["description"] = definition.description;
        model_status["definition"] = to_list(definition.definition);
        models_status.emplace_back(std::move(model_status));
      }
      result["models"] = std::move(models_status);
      // Sorted list of all keys.
      auto keys = std::vector<std::string>(data.size());
      std::transform(data.begin(), data.end(), keys.begin(),
                     [](const auto& x) { return x.first; });
      std::sort(keys.begin(), keys.end());
      result["types"] = to_list(keys);
      // The usual per-component status.
      detail::fill_status_map(result, self);
    }
  }
  return result;
}

std::filesystem::path type_registry_state::filename() const {
  return dir / name;
}

caf::error type_registry_state::save_to_disk() const {
  std::vector<char> buffer;
  caf::binary_serializer sink{self->system(), buffer};
  if (auto error = sink(data))
    return error;
  return io::save(filename(), as_bytes(buffer));
}

caf::error type_registry_state::load_from_disk() {
  // Nothing to load is not an error.
  std::error_code err{};
  const auto dir_exists = std::filesystem::exists(dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to find directory {}: {}", dir,
                                       err.message()));
  if (!dir_exists) {
    VAST_DEBUG("{} found no directory to load from", *self);
    return caf::none;
  }
  const auto fname = filename();
  const auto file_exists = std::filesystem::exists(fname, err);
  if (err)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to find file {}: {}", fname,
                                       err.message()));
  if (file_exists) {
    auto buffer = io::read(fname);
    if (!buffer)
      return buffer.error();
    caf::binary_deserializer source{self->system(), *buffer};
    if (auto error = source(data))
      return error;
    VAST_DEBUG("{} loaded state from disk", *self);
  }
  return caf::none;
}

void type_registry_state::insert(vast::legacy_type layout) {
  auto& old_layouts = data[layout.name()];
  // Insert into the existing bucket.
  auto [hint, success] = old_layouts.insert(std::move(layout));
  if (success) {
    // Check whether the new layout is compatible with the latest, i.e., whether
    // the new layout is a superset of it.
    if (old_layouts.begin() != hint) {
      if (!is_subset(*old_layouts.begin(), *hint))
        VAST_WARN("{} detected an incompatible layout change for {}", *self,
                  hint->name());
      else
        VAST_INFO("{} detected a layout change for {}", *self, hint->name());
    }
    VAST_DEBUG("{} registered {}", *self, hint->name());
  }
  // Move the newly inserted layout to the front.
  std::rotate(old_layouts.begin(), hint, std::next(hint));
}

type_set type_registry_state::types() const {
  auto result = type_set{};
  for (const auto& x : configuration_schema)
    result.insert(x);
  return result;
}

type_registry_actor::behavior_type
type_registry(type_registry_actor::stateful_pointer<type_registry_state> self,
              const std::filesystem::path& dir) {
  self->state.self = self;
  self->state.dir = dir;
  // Register the exit handler.
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    VAST_DEBUG("{} got EXIT from {}", *self, msg.source);
    if (auto telemetry = self->state.telemetry(); !telemetry.empty())
      self->send(self->state.accountant, std::move(telemetry));
    if (auto err = self->state.save_to_disk())
      VAST_ERROR("{} failed to persist state to disk: {}", *self,
                 self->system().render(err));
    self->quit(msg.reason);
  });
  // Load existing state from disk if possible.
  if (auto err = self->state.load_from_disk()) {
    self->quit(std::move(err));
    return type_registry_actor::behavior_type::make_empty_behavior();
  }
  // Load loaded schema types from the singleton.
  // TODO: Move to the load handler and re-parse the files.
  if (const auto* schema = vast::event_types::get())
    self->state.configuration_schema = *schema;
  // The behavior of the type-registry.
  return {
    [self](atom::telemetry) {
      if (auto telemetry = self->state.telemetry(); !telemetry.empty()) {
        VAST_TRACE_SCOPE("{} sends out a telemetry report to the {}", *self,
                         VAST_ARG("accountant", self->state.accountant));
        self->send(self->state.accountant, std::move(telemetry));
      }
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
    },
    [self](atom::status, status_verbosity v) {
      VAST_TRACE_SCOPE("{} sends out a status report", *self);
      return self->state.status(v);
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_TRACE_SCOPE("{} attaches to {}", *self, VAST_ARG("stream", in));
      auto result = caf::attach_stream_sink(
        self, in,
        [=](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&, table_slice x) { self->state.insert(x.layout()); });
      return result.inbound_slot();
    },
    [self](atom::get) {
      VAST_TRACE_SCOPE("{} retrieves a list of all known types", *self);
      return self->state.types();
    },
    [self](atom::put, taxonomies t) {
      VAST_TRACE_SCOPE("");
      self->state.taxonomies = std::move(t);
    },
    [self](atom::get, atom::taxonomies) {
      VAST_TRACE_SCOPE("");
      return self->state.taxonomies;
    },
    [self](atom::load) -> caf::result<atom::ok> {
      VAST_DEBUG("{} loads taxonomies", *self);
      std::error_code err{};
      auto dirs = get_schema_dirs(self->system().config());
      concepts_map concepts;
      models_map models;
      for (const auto& dir : dirs) {
        const auto dir_exists = std::filesystem::exists(dir, err);
        if (err)
          VAST_WARN("{} failed to open directory {}: {}", *self, dir,
                    err.message());
        if (!dir_exists)
          continue;
        auto yamls = load_yaml_dir(dir);
        if (!yamls)
          return yamls.error();
        for (auto& [file, yaml] : *yamls) {
          VAST_DEBUG("{} extracts taxonomies from {}", *self, file.string());
          if (auto err = convert(yaml, concepts, concepts_data_layout))
            return caf::make_error(ec::parse_error,
                                   "failed to extract concepts from file",
                                   file.string(), err.context());
          for (auto& [name, definition] : concepts) {
            VAST_DEBUG("{} extracted concept {} with {} fields", *self, name,
                       definition.fields.size());
            for (auto& field : definition.fields)
              VAST_TRACE("{} uses concept mapping {} -> {}", *self, name,
                         field);
          }
          if (auto err = convert(yaml, models, models_data_layout))
            return caf::make_error(ec::parse_error,
                                   "failed to extract models from file",
                                   file.string(), err.context());
          for (auto& [name, definition] : models) {
            VAST_DEBUG("{} extracted model {} with {} fields", *self, name,
                       definition.definition.size());
            VAST_TRACE("{} uses model mapping {} -> {}", *self, name,
                       definition.definition);
          }
        }
      }
      self->state.taxonomies
        = taxonomies{std::move(concepts), std::move(models)};
      return atom::ok_v;
    },
    [self](atom::resolve, const expression& e) {
      return resolve(self->state.taxonomies, e, self->state.data);
    },
    [self](accountant_actor accountant) {
      VAST_ASSERT(accountant);
      VAST_DEBUG("{} connects to {}", *self, VAST_ARG(accountant));
      self->state.accountant = accountant;
      self->send(self->state.accountant, atom::announce_v, self->name());
      self->delayed_send(self, defaults::system::telemetry_rate,
                         atom::telemetry_v);
    },
  };
}

} // namespace vast::system
