//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

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

// TODO: harmonize this with type::to_definition. The goal was to create a
// single fixed schema, but it's still clunky due to nested records.
/// Adds one type definition per row to a builder.
auto add_type(builder_ref builder, const type& t) {
  auto row = builder.record();
  row.field("name").data(t.name());
  auto layout = row.field("layout").record();
  auto f = detail::overload{
    [&](const auto&) {
      layout.field("basic").data(fmt::to_string(t));
    },
    [&](const enumeration_type& e) {
      auto enum_field = layout.field("enum");
      auto list = enum_field.list();
      for (auto field : e.fields()) {
        auto field_record = list.record();
        field_record.field("name").data(field.name);
        field_record.field("key").data(uint64_t{field.key});
      }
    },
    [&](const list_type& l) {
      // TODO: recurse into nested records.
      layout.field("list").data(fmt::to_string(l.value_type()));
    },
    [&](const map_type&) {
      die("unreachable");
    },
    [&](const record_type& r) {
      auto record = layout.field("record");
      auto list = record.list();
      for (const auto& field : r.fields()) {
        auto field_record = list.record();
        field_record.field("name").data(field.name);
        field_record.field("type").data(fmt::to_string(field.type));
      }
    },
  };
  caf::visit(f, t);
  auto attributes = collect(t.attributes());
  if (attributes.empty())
    return;
  auto list = row.field("attributes").list();
  for (auto& attribute : attributes) {
    auto record = list.record();
    record.field("key").data(attribute.key);
    record.field("value").data(attribute.value);
  }
}
class plugin final : public virtual aspect_plugin {
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
    auto builder = series_builder{type_type()};
    for (const auto& type : types) {
      add_type(builder, type);
    }
    for (auto&& slice : builder.finish_as_table_slice()) {
      co_yield std::move(slice);
    }
  }
};

} // namespace

} // namespace tenzir::plugins::types

TENZIR_REGISTER_PLUGIN(tenzir::plugins::types::plugin)
