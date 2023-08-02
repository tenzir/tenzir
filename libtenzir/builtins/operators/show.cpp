//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/adaptive_table_slice_builder.hpp"
#include "tenzir/collect.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/uuid.hpp>

#include <caf/scoped_actor.hpp>
#include <caf/timespan.hpp>

#include <string>
#include <vector>

using namespace tenzir::si_literals;

namespace tenzir::plugins::show {

namespace {

/// Checks whether a thing is an a collection of named things.
auto contains(const auto& xs, const auto& name) -> bool {
  auto f = [&](const auto& x) {
    return x->name() == name;
  };
  return std::find_if(xs.begin(), xs.end(), f) != xs.end();
};

/// A type that represents a connector.
auto connector_type() -> type {
  return type{
    "tenzir.connector",
    record_type{
      {"name", string_type{}},
      {"loader", bool_type{}},
      {"saver", bool_type{}},
    },
  };
}

/// A type that represents a format.
auto format_type() -> type {
  return type{
    "tenzir.format",
    record_type{
      {"name", string_type{}},
      {"printer", bool_type{}},
      {"parser", bool_type{}},
    },
  };
}

/// A type that represents an operator.
auto operator_type() -> type {
  return type{
    "tenzir.operator",
    record_type{
      {"name", string_type{}},
      {"source", bool_type{}},
      {"transformation", bool_type{}},
      {"sink", bool_type{}},
    },
  };
}

/// A type that represents a partition.
auto partition_type() -> type {
  return type{
    "tenzir.partition",
    record_type{
      {"uuid", string_type{}},
      {"memory_usage", uint64_type{}},
      {"min_import_time", time_type{}},
      {"max_import_time", time_type{}},
      {"version", uint64_type{}},
      {"schema", string_type{}},
    },
  };
}

/// A type that represents a type attribute.
auto attribute_type() -> type {
  return type{
    "tenzir.attribute",
    record_type{
      {"key", string_type{}},
      {"value", string_type{}},
    },
  };
}

/// A type that describes a type within a schema.
auto field_type() -> type {
  return type{
    "tenzir.field_type",
    record_type{
      {"kind", string_type{}},
      {"category", string_type{}},
      {"lists", uint64_type()},
      {"name", string_type{}},
      {"attributes", list_type{attribute_type()}},
    },
  };
}

/// A type that describes all fields in a schema.
auto schema_type() -> type {
  return type{
    "tenzir.schema",
    record_type{
      {"schema", string_type{}},
      {"field", string_type{}},
      {"path", list_type{string_type{}}},
      {"index", list_type{uint64_type{}}},
      {"type", field_type()},
    },
  };
}

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
         {"map", list_type{list_type{record_type{
                   {"key", string_type{}},
                   {"value", string_type{}},
                 }}}},
         {"record",
          list_type{
            record_type{
              {"name", string_type{}},
              {"type", string_type()},
            },
          }},
       }},
      {"attributes", list_type{attribute_type()}},
    },
  };
}

struct field_context {
  std::string name{};
  std::vector<std::string> path{};
  offset index{};
};

struct type_context {
  std::string kind{};
  std::string category;
  size_t lists{0};
  std::string name{};
  std::vector<std::pair<std::string, std::string>> attributes{};
};

struct schema_context {
  field_context field;
  type_context type;
};

/// Yields all fields from a record type, with listness being a separate
/// attribute.
auto traverse(type t, field_context ctx = {}) -> generator<schema_context> {
  schema_context result;
  // Unpack lists. Note that we lose type metadata of lists.
  while (const auto* list = caf::get_if<list_type>(&t)) {
    ++result.type.lists;
    t = list->value_type();
  }
  result.type.name = t.name();
  for (auto [key, value] : t.attributes())
    result.type.attributes.emplace_back(key, value);
  result.type.kind = fmt::to_string(t);
  const auto* alphabet = "abcdefghijklmnopqrstuvwxyz";
  auto i = result.type.kind.find_first_not_of(alphabet);
  if (i != std::string::npos)
    result.type.kind.resize(i);
  // TODO: This categorization is somewhat arbitrary, and we probably want to
  // think about this more.
  if (result.type.kind == "list" || result.type.kind == "record")
    result.type.category = "container";
  else
    result.type.category = "atomic";
  TENZIR_ASSERT_CHEAP(not caf::holds_alternative<list_type>(t));
  TENZIR_ASSERT_CHEAP(not caf::holds_alternative<map_type>(t));
  if (const auto* record = caf::get_if<record_type>(&t)) {
    auto i = size_t{0};
    for (const auto& field : record->fields()) {
      result.field.name = field.name;
      result.field.path.emplace_back(field.name);
      result.field.index.emplace_back(i);
      for (const auto& inner : traverse(field.type, result.field)) {
        result.type = inner.type;
        auto nested = not inner.field.name.empty();
        if (nested) {
          result.field.name = inner.field.name;
          for (const auto& p : inner.field.path)
            result.field.path.push_back(p);
          for (const auto& i : inner.field.index)
            result.field.index.push_back(i);
        }
        co_yield result;
        if (nested) {
          auto delta = inner.field.path.size();
          result.field.path.resize(result.field.path.size() - delta);
          delta = inner.field.index.size();
          result.field.index.resize(result.field.index.size() - delta);
        }
      }
      result.field.index.pop_back();
      result.field.path.pop_back();
      ++i;
    }
  } else {
    co_yield result;
  }
}

// TODO: this feels like it should be a generic function that works on any
// inspectable type.
/// Adds a schema (= named record type) to a builder, with one row per field.
auto add_schema(auto& builder, const type& t) -> caf::error {
  for (const auto& ctx : traverse(t)) {
    auto row = builder.push_row();
    if (auto err = row.push_field("schema").add(t.name()))
      return err;
    if (auto err = row.push_field("field").add(ctx.field.name))
      return err;
    auto path = row.push_field("path").push_list();
    for (const auto& p : ctx.field.path)
      if (auto err = path.add(p))
        return err;
    auto index = row.push_field("index").push_list();
    for (auto i : ctx.field.index)
      if (auto err = index.add(uint64_t{i}))
        return err;
    auto type = row.push_field("type").push_record();
    if (auto err = type.push_field("kind").add(ctx.type.kind))
      return err;
    if (auto err = type.push_field("category").add(ctx.type.category))
      return err;
    if (auto err = type.push_field("lists").add(ctx.type.lists))
      return err;
    if (auto err = type.push_field("name").add(ctx.type.name))
      return err;
    auto attrs = type.push_field("attributes").push_list();
    for (const auto& [key, value] : ctx.type.attributes) {
      auto attr = attrs.push_record();
      if (auto err = attr.push_field("key").add(key))
        return err;
      if (auto err = attr.push_field("value").add(value))
        return err;
    }
  }
  return {};
}

// TODO: harmonize this with type::to_definition. The goal was to create a
// single fixed schema, but it's still clunky due to nested records.
/// Adds one type definition per row to a builder.
auto add_type(auto& builder, const type& t) -> caf::error {
  auto row = builder.push_row();
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
    [&](const map_type& m) -> caf::error {
      // TODO: recurse into nested records.
      auto map = layout.push_field("map");
      auto record = map.push_record();
      auto key_field = record.push_field("key");
      if (auto err = key_field.add(fmt::to_string(m.key_type())))
        return err;
      auto value_field = record.push_field("value");
      if (auto err = value_field.add(fmt::to_string(m.value_type())))
        return err;
      return {};
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

/// Base class for aspects that users can show.
class aspect {
public:
  virtual ~aspect() = default;

  /// The name of the aspect that enables `show a.spect`
  virtual auto name() const -> std::string = 0;

  /// The location of the show operator for this aspect.
  virtual auto location() const -> operator_location = 0;

  /// Produces the data to show.
  virtual auto show(operator_control_plane& ctrl) const
    -> generator<table_slice>
    = 0;
};

class connectors_aspect final : public aspect {
public:
  auto name() const -> std::string override {
    return "connectors";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto loaders = collect(plugins::get<loader_parser_plugin>());
    auto savers = collect(plugins::get<saver_parser_plugin>());
    auto connectors = std::set<std::string>{};
    for (const auto* plugin : loaders)
      connectors.insert(plugin->name());
    for (const auto* plugin : savers)
      connectors.insert(plugin->name());
    auto builder = table_slice_builder{connector_type()};
    for (const auto& connector : connectors) {
      if (not(builder.add(connector)
              && builder.add(contains(loaders, connector))
              && builder.add(contains(savers, connector)))) {
        diagnostic::error("failed to add connector")
          .note("from `show {}`", name())
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
    co_yield builder.finish();
  }
};

class formats_aspect final : public aspect {
public:
  auto name() const -> std::string override {
    return "formats";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto parsers = collect(plugins::get<parser_parser_plugin>());
    auto printers = collect(plugins::get<printer_parser_plugin>());
    auto formats = std::set<std::string>{};
    for (const auto* plugin : parsers)
      formats.insert(plugin->name());
    for (const auto* plugin : printers)
      formats.insert(plugin->name());
    auto builder = table_slice_builder{format_type()};
    for (const auto& format : formats) {
      if (not(builder.add(format) && builder.add(contains(parsers, format))
              && builder.add(contains(printers, format)))) {
        diagnostic::error("failed to add format")
          .note("from `show {}`", name())
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
    co_yield builder.finish();
  }
};

class operators_aspect final : public aspect {
public:
  auto name() const -> std::string override {
    return "operators";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto builder = table_slice_builder{operator_type()};
    for (const auto* plugin : plugins::get<operator_parser_plugin>()) {
      auto signature = plugin->signature();
      if (not(builder.add(plugin->name()) && builder.add(signature.source)
              && builder.add(signature.transformation)
              && builder.add(signature.sink))) {
        diagnostic::error("failed to add operator")
          .note("from `show {}`", name())
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
    co_yield builder.finish();
  }
};

class partitions_aspect final : public aspect {
public:
  auto name() const -> std::string override {
    return "partitions";
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
    auto builder = table_slice_builder{partition_type()};
    constexpr auto max_rows = size_t{8_Ki};
    for (auto i = 0u; i < synopses.size(); ++i) {
      auto& synopsis = synopses[i];
      if (not(builder.add(fmt::to_string(synopsis.uuid))
              && builder.add(uint64_t{synopsis.synopsis->memusage()})
              && builder.add(synopsis.synopsis->min_import_time)
              && builder.add(synopsis.synopsis->max_import_time)
              && builder.add(synopsis.synopsis->version)
              && builder.add(synopsis.synopsis->schema.name()))) {
        diagnostic::error("failed to add partition entry")
          .note("from `show {}`", name())
          .emit(ctrl.diagnostics());
        co_return;
      }
      if ((i + 1) % max_rows == 0) {
        co_yield builder.finish();
        builder = std::exchange(builder, table_slice_builder{partition_type()});
      }
    }
    if (synopses.size() % max_rows != 0) // no empty slices
      co_yield builder.finish();
  }
};

class tables_aspect final : public aspect {
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
    auto schemas = std::set<type>{};
    for (const auto& synopsis : synopses)
      schemas.insert(synopsis.synopsis->schema);
    auto builder = adaptive_table_slice_builder{schema_type()};
    for (const auto& schema : schemas) {
      if (auto err = add_schema(builder, schema))
        diagnostic::error("failed to add type to builder")
          .note("full type: {}", fmt::to_string(schema))
          .emit(ctrl.diagnostics());
    }
    co_yield builder.finish();
  }
};

class types_aspect final : public aspect {
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
      if (auto err = add_type(builder, type))
        diagnostic::error("failed to add type to builder")
          .note("full type: {}", fmt::to_string(type))
          .emit(ctrl.diagnostics());
    }
    co_yield builder.finish();
  }
};

namespace aspects {

auto get() -> const std::vector<std::shared_ptr<aspect>>& {
  static auto instance = std::vector<std::shared_ptr<aspect>>{
    std::make_shared<connectors_aspect>(),
    std::make_shared<formats_aspect>(),
    std::make_shared<operators_aspect>(),
    std::make_shared<partitions_aspect>(),
    std::make_shared<tables_aspect>(),
    std::make_shared<types_aspect>(),
  };
  return instance;
}

auto get(std::string_view name) -> std::shared_ptr<aspect> {
  auto pred = [&](auto& x) {
    return x->name() == name;
  };
  const auto& all = get();
  auto it = std::find_if(all.begin(), all.end(), pred);
  if (it == all.end())
    return nullptr;
  return *it;
}

} // namespace aspects

struct operator_args {
  located<std::string> aspect;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("aspect", x.aspect));
  }
};

class show_operator final : public crtp_operator<show_operator> {
public:
  show_operator() = default;

  explicit show_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    return get()->show(ctrl);
  }

  auto name() const -> std::string override {
    return "show";
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return get()->location();
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, show_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  auto get() const -> aspect* {
    const auto& aspect = aspects::get(args_.aspect.inner);
    TENZIR_ASSERT_CHEAP(aspect != nullptr);
    return aspect.get();
  }

  operator_args args_;
};

class plugin final : public virtual operator_plugin<show_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"show", "https://docs.tenzir.com/next/"
                                          "operators/sources/show"};
    operator_args args;
    parser.add(args.aspect, "<aspect>");
    parser.parse(p);
    if (not aspects::get(args.aspect.inner)) {
      auto available = std::vector<std::string>{};
      for (const auto& aspect : aspects::get())
        available.push_back(aspect->name());
      diagnostic::error("aspect `{}` could not be found", args.aspect.inner)
        .primary(args.aspect.source)
        .hint("must be one of {}", fmt::join(available, ", "))
        .throw_();
    }
    return std::make_unique<show_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::show

TENZIR_REGISTER_PLUGIN(tenzir::plugins::show::plugin)
