//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::fields {

namespace {

auto field_type() -> type {
  return type{
    "tenzir.field",
    record_type{
      {"schema", string_type{}},
      {"schema_id", string_type{}},
      {"field", string_type{}},
      {"path", list_type{string_type{}}},
      {"index", list_type{uint64_type{}}},
      {"type",
       record_type{
         {"kind", string_type{}},
         {"category", string_type{}},
         {"lists", uint64_type()},
         {"name", string_type{}},
         {"attributes", list_type{record_type{
                          {"key", string_type{}},
                          {"value", string_type{}},
                        }}},
       }},
    },
  };
}

struct field_context {
  std::string name{};
  std::vector<std::string> path{};
  offset index{};
};

struct type_context {
  type_kind kind{};
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
auto traverse(type t) -> generator<schema_context> {
  schema_context result;
  // Unpack lists. Note that we lose type metadata of lists.
  while (const auto* list = caf::get_if<list_type>(&t)) {
    ++result.type.lists;
    t = list->value_type();
  }
  result.type.name = t.name();
  for (auto [key, value] : t.attributes())
    result.type.attributes.emplace_back(key, value);
  result.type.kind = t.kind();
  // TODO: This categorization is somewhat arbitrary, and we probably want to
  // think about this more.
  if (result.type.kind.is<record_type>())
    result.type.category = "container";
  else
    result.type.category = "atomic";
  TENZIR_ASSERT(not caf::holds_alternative<list_type>(t));
  TENZIR_ASSERT(not caf::holds_alternative<map_type>(t));
  if (const auto* record = caf::get_if<record_type>(&t)) {
    auto i = size_t{0};
    for (const auto& field : record->fields()) {
      result.field.name = field.name;
      result.field.path.emplace_back(field.name);
      result.field.index.emplace_back(i);
      for (const auto& inner : traverse(field.type)) {
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
auto add_field(builder_ref builder, const type& t) {
  for (const auto& ctx : traverse(t)) {
    auto row = builder.record();
    row.field("schema").data(t.name());
    row.field("schema_id").data(t.make_fingerprint());
    row.field("field").data(ctx.field.name);
    auto path = row.field("path").list();
    for (const auto& p : ctx.field.path)
      path.data(p);
    auto index = row.field("index").list();
    for (auto i : ctx.field.index)
      index.data(uint64_t{i});
    auto type = row.field("type").record();
    type.field("kind").data(to_string(ctx.type.kind));
    type.field("category").data(ctx.type.category);
    type.field("lists").data(ctx.type.lists);
    type.field("name").data(ctx.type.name);
    auto attrs = type.field("attributes").list();
    for (const auto& [key, value] : ctx.type.attributes) {
      auto attr = attrs.record();
      attr.field("key").data(key);
      attr.field("value").data(value);
    }
  }
}

class fields_operator final : public crtp_operator<fields_operator> {
public:
  fields_operator() = default;

  auto operator()(exec_ctx ctx) const -> generator<table_slice> {
    auto catalog
      = ctrl.self().system().registry().get<catalog_actor>("tenzir.catalog");
    TENZIR_ASSERT(catalog);
    ctrl.set_waiting(true);
    auto synopses = std::vector<partition_synopsis_pair>{};
    ctrl.self()
      .request(catalog, caf::infinite, atom::get_v)
      .then(
        [&](std::vector<partition_synopsis_pair>& result) {
          synopses = std::move(result);
          ctrl.set_waiting(false);
        },
        [&ctrl](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to get partitions")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    auto fields = std::set<type>{};
    for (const auto& synopsis : synopses)
      fields.insert(synopsis.synopsis->schema);
    auto builder = series_builder{field_type()};
    for (const auto& schema : fields) {
      add_field(builder, schema);
    }
    for (auto&& slice : builder.finish_as_table_slice()) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "fields";
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  auto internal() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, fields_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<fields_operator>,
                     operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"fields", "https://docs.tenzir.com/"
                                            "operators/fields"};
    parser.parse(p);
    return std::make_unique<fields_operator>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("fields").parse(inv, ctx).ignore();
    return std::make_unique<fields_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::fields

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fields::plugin)
