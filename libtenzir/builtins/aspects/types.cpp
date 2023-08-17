//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::types {

namespace {

/// A type that describes all fields in a schema.
/// A type describing a type without information loss.
auto type_type() -> type {
  return type{
    "tenzir.type",
    record_type{
      {"name", string_type{}},
      {"layout",
       record_type{
         {"basic", string_type{}},
         {"enum", list_type{record_type{
                    {"name", string_type{}},
                    {"key", uint64_type{}},
                  }}},
         {"list", string_type{}},
         {"record",
          list_type{
            record_type{
              {"name", string_type{}},
              {"type", string_type()},
            },
          }},
       }},
      {"attributes", list_type{record_type{
                       {"key", string_type{}},
                       {"value", string_type{}},
                     }}},
    },
  };
}

/// A type that describes a table in the catalog.
auto table_type() -> type {
  return type{
    "tenzir.table",
    record_type{
      {"events", uint64_type{}},
      {"min_import_time", time_type{}},
      {"max_import_time", time_type{}},
      {"schema", type_type()},
    },
  };
}

// TODO: harmonize this with type::to_definition. The goal was to create a
// single fixed schema, but it's still clunky due to nested records.
/// Adds one type definition per row to a builder.
auto add_type(auto& row, const type& t) -> caf::error {
  auto err = row.push_field("name").add(t.name());
  TENZIR_ASSERT_CHEAP(!err);
  auto layout = row.push_field("layout").push_record();
  auto f = detail::overload{
    [&](const auto&) -> caf::error {
      return layout.push_field("basic").add(fmt::to_string(t));
    },
    [&](const enumeration_type& e) -> caf::error {
      auto enum_field = layout.push_field("enum");
      auto list = enum_field.push_list();
      for (auto field : e.fields()) {
        auto field_record = list.push_record();
        auto name = field_record.push_field("name");
        if (auto err = name.add(field.name))
          return err;
        auto key = field_record.push_field("key");
        if (auto err = key.add(uint64_t{field.key}))
          return err;
      }
      return {};
    },
    [&](const list_type& l) -> caf::error {
      // TODO: recurse into nested records.
      auto list = layout.push_field("list");
      return list.add(fmt::to_string(l.value_type()));
    },
    [&](const map_type&) -> caf::error {
      die("unreachable");
    },
    [&](const record_type& r) -> caf::error {
      auto record = layout.push_field("record");
      auto list = record.push_list();
      for (const auto& field : r.fields()) {
        auto field_record = list.push_record();
        auto field_name = field_record.push_field("name");
        if (auto err = field_name.add(field.name))
          return err;
        auto field_type = field_record.push_field("type");
        if (auto err = field_type.add(fmt::to_string(field.type)))
          return err;
      }
      return {};
    },
  };
  if (auto err = caf::visit(f, t))
    return err;
  auto attributes = collect(t.attributes());
  if (attributes.empty())
    return {};
  auto list = row.push_field("attributes").push_list();
  for (auto& attribute : attributes) {
    auto record = list.push_record();
    if (auto err = record.push_field("key").add(attribute.key))
      return err;
    if (auto err = record.push_field("value").add(attribute.value))
      return err;
  }
  return {};
}

class tables_plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "tables";
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    // TODO: Some of the the requests this operator makes are blocking, so
    // we have to create a scoped actor here; once the operator API uses
    // async we can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    auto components
      = get_node_components<catalog_actor>(blocking_self, ctrl.node());
    if (!components) {
      ctrl.abort(std::move(components.error()));
      co_return;
    }
    co_yield {};
    auto [catalog] = std::move(*components);
    auto synopses = std::vector<partition_synopsis_pair>{};
    auto error = caf::error{};
    ctrl.self()
      .request(catalog, caf::infinite, atom::get_v)
      .await(
        [&synopses](std::vector<partition_synopsis_pair>& result) {
          synopses = std::move(result);
        },
        [&error](caf::error err) {
          error = std::move(err);
        });
    co_yield {};
    if (error) {
      ctrl.abort(std::move(error));
      co_return;
    }
    struct table_meta {
      uint64_t events = 0;
      time min_import_time = time::max();
      time max_import_time = time::min();
      type schema;
    };
    std::unordered_map<type, table_meta> acc = {};
    for (const auto& syn : synopses) {
      auto& s = acc[syn.synopsis->schema];
      s.min_import_time
        = std::min(s.min_import_time, syn.synopsis->min_import_time);
      s.max_import_time
        = std::max(s.max_import_time, syn.synopsis->max_import_time);
      s.events += syn.synopsis->events;
    }
    auto builder = adaptive_table_slice_builder{table_type()};
    for (const auto& [schema, meta] : acc) {
      auto row = builder.push_row();
      if (auto err = row.push_field("events").add(meta.events)) {
        diagnostic::error("failed to add table event count to builder")
          .emit(ctrl.diagnostics());
      }
      if (auto err
          = row.push_field("min_import_time").add(meta.min_import_time)) {
        diagnostic::error("failed to add table min_import_time to builder")
          .emit(ctrl.diagnostics());
      }
      if (auto err
          = row.push_field("max_import_time").add(meta.max_import_time)) {
        diagnostic::error("failed to add table max_import_time to builder")
          .emit(ctrl.diagnostics());
      }
      auto sch = row.push_field("schema").push_record();
      if (auto err = add_type(sch, schema)) {
        diagnostic::error("failed to add type to builder")
          .note("full type: {}", fmt::to_string(schema))
          .emit(ctrl.diagnostics());
      }
    }
    if (builder.rows() != 0) {
      co_yield builder.finish();
    }
  }
};

class types_plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "types";
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    // TODO: Some of the the requests this operator makes are blocking, so
    // we have to create a scoped actor here; once the operator API uses
    // async we can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    auto components
      = get_node_components<catalog_actor>(blocking_self, ctrl.node());
    if (!components) {
      ctrl.abort(std::move(components.error()));
      co_return;
    }
    co_yield {};
    auto [catalog] = std::move(*components);
    auto types = type_set{};
    auto error = caf::error{};
    ctrl.self()
      .request(catalog, caf::infinite, atom::get_v, atom::type_v)
      .await(
        [&types](type_set& result) {
          types = std::move(result);
        },
        [&error](caf::error err) {
          error = std::move(err);
        });
    co_yield {};
    if (error) {
      ctrl.abort(std::move(error));
      co_return;
    }
    auto builder = adaptive_table_slice_builder{type_type()};
    for (const auto& type : types) {
      auto row = builder.push_row();
      if (auto err = add_type(row, type))
        diagnostic::error("failed to add type to builder")
          .note("full type: {}", fmt::to_string(type))
          .emit(ctrl.diagnostics());
    }
    co_yield builder.finish();
  }
};

} // namespace

} // namespace tenzir::plugins::types

TENZIR_REGISTER_PLUGIN(tenzir::plugins::types::tables_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::types::types_plugin)
