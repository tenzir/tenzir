//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/cast.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>

#include <ranges>

namespace tenzir::plugins::chart {
namespace {

struct unset_default_tag {
  friend auto inspect(auto&, unset_default_tag&) -> bool {
    TENZIR_UNREACHABLE();
  }
};
struct nth_field {
  size_t index;

  friend auto inspect(auto& f, nth_field& x) -> bool {
    return f.object(x)
      .pretty_name("nth_field")
      .fields(f.field("index", x.index));
  }
};
struct schema_name {
  friend auto inspect(auto& f, schema_name& x) -> bool {
    return f.object(x).pretty_name("schema_name").fields();
  }
};
struct field_name {
  std::string field;

  friend auto inspect(auto& f, field_name& x) -> bool {
    return f.object(x)
      .pretty_name("field_name")
      .fields(f.field("field", x.field));
  }
};
struct attribute_value {
  std::string attr;

  friend auto inspect(auto& f, attribute_value& x) -> bool {
    return f.object(x)
      .pretty_name("attribute_value")
      .fields(f.field("attr", x.attr));
  }
};

using field_value_type = std::variant<unset_default_tag, nth_field, schema_name,
                                      field_name, attribute_value>;

} // namespace
} // namespace tenzir::plugins::chart

TENZIR_DIAGNOSTIC_PUSH
TENZIR_DIAGNOSTIC_IGNORE_UNUSED_CONST_VARIABLE

CAF_BEGIN_TYPE_ID_BLOCK(tenzir_chart_operator_config_types, 3200)
  CAF_ADD_TYPE_ID(tenzir_chart_operator_config_types,
                  (tenzir::plugins::chart::unset_default_tag));
  CAF_ADD_TYPE_ID(tenzir_chart_operator_config_types,
                  (tenzir::plugins::chart::nth_field));
  CAF_ADD_TYPE_ID(tenzir_chart_operator_config_types,
                  (tenzir::plugins::chart::schema_name));
  CAF_ADD_TYPE_ID(tenzir_chart_operator_config_types,
                  (tenzir::plugins::chart::field_name));
  CAF_ADD_TYPE_ID(tenzir_chart_operator_config_types,
                  (tenzir::plugins::chart::attribute_value));
CAF_END_TYPE_ID_BLOCK(tenzir_chart_operator_config_types)

TENZIR_DIAGNOSTIC_POP

namespace tenzir::plugins::chart {
namespace {

enum class requirement {
  none,
  // No duplicate values are allowed.
  unique,
  // Values must be strictly increasing
  // (i.e., be sorted in ascending order, without duplicates).
  // Implies `unique`.
  strictly_increasing,
};

auto inspect(auto& f, requirement& x) -> bool {
  return detail::inspect_enum_str(f, x,
                                  {"none", "unique", "strictly_increasing"});
}

struct configuration_item {
  std::string key;
  field_value_type field_value;
  requirement req{requirement::none};

  friend auto inspect(auto& f, configuration_item& x) -> bool {
    return f.object(x)
      .pretty_name("configuration_item")
      .fields(f.field("key", x.key), f.field("field_value", x.field_value),
              f.field("req", x.req));
  }
};

using configuration = std::vector<configuration_item>;

class chart_operator final : public crtp_operator<chart_operator> {
public:
  chart_operator() = default;

  explicit chart_operator(configuration&& cfg) : cfg_(std::move(cfg)) {
  }

  // Keys are keys into `cfg_`
  // Values are a set of all the previously encountered values in that field
  //
  // An entry is only added if the field's `req` value is not
  // `requirement::none`.
  using previous_values_type
    = detail::flat_map<std::string_view, std::unordered_set<data>>;

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // Cache attribute-enriched schemas, to avoid the potentially expensive
    // operation of building a list of attributes by visiting `cfg_` for every
    // iteration
    std::unordered_map<type, type> enriched_schemas_cache{};
    previous_values_type previous_values{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto original_schema = slice.schema();
      if (auto it = enriched_schemas_cache.find(original_schema);
          it != enriched_schemas_cache.end()) {
        if (not verify_values(slice, previous_values, ctrl)) {
          co_return;
        }
        const auto& [_, cached_schema] = *it;
        co_yield cast(std::move(slice), cached_schema);
        continue;
      }
      auto attributes = make_attributes(slice, previous_values, ctrl);
      if (not attributes) {
        co_return;
      }
      auto new_schema = type{original_schema, std::move(*attributes)};
      TENZIR_ASSERT(new_schema);
      co_yield cast(std::move(slice), new_schema);
      enriched_schemas_cache.emplace(std::move(original_schema),
                                     std::move(new_schema));
    }
  }

  auto name() const -> std::string override {
    return "chart";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, chart_operator& x) -> bool {
    return f.object(x).pretty_name("chart").fields(f.field("config", x.cfg_));
  }

private:
  auto
  verify_values(const table_slice& slice, previous_values_type& previous_values,
                operator_control_plane& ctrl) const -> bool {
    const auto* record_schema_ptr = caf::get_if<record_type>(&slice.schema());
    if (not record_schema_ptr) {
      diagnostic::error("chart operator expects input to be a record")
        .emit(ctrl.diagnostics());
      return false;
    }
    const auto& record_schema = *record_schema_ptr;
    auto struct_array = to_record_batch(slice)->ToStructArray().ValueOrDie();
    for (const auto& item : cfg_) {
      if (item.req == requirement::none) {
        continue;
      }
      auto field_name = resolve_attribute_value(item, record_schema,
                                                slice.schema().name(), ctrl);
      if (not field_name) {
        return false;
      }
      if (not verify_single_field(item, slice, *struct_array, record_schema,
                                  *field_name, previous_values, ctrl)) {
        return false;
      }
    }
    return true;
  }

  auto make_attributes(const table_slice& slice,
                       previous_values_type& previous_values,
                       operator_control_plane& ctrl) const
    -> std::optional<std::vector<type::attribute_view>> {
    const auto* record_schema_ptr = caf::get_if<record_type>(&slice.schema());
    if (not record_schema_ptr) {
      diagnostic::error("chart operator expects input to be a record")
        .emit(ctrl.diagnostics());
      return std::nullopt;
    }
    const auto& record_schema = *record_schema_ptr;
    auto struct_array = to_record_batch(slice)->ToStructArray().ValueOrDie();
    std::vector<type::attribute_view> result{};
    for (const auto& item : cfg_) {
      auto field_name = resolve_attribute_value(item, record_schema,
                                                slice.schema().name(), ctrl);
      if (not field_name) {
        return std::nullopt;
      }
      if (not verify_single_field(item, slice, *struct_array, record_schema,
                                  *field_name, previous_values, ctrl)) {
        return std::nullopt;
      }
      result.emplace_back(item.key, *field_name);
    }
    return result;
  }

  auto
  verify_single_field(const configuration_item& item, const table_slice& slice,
                      const arrow::StructArray& struct_array,
                      const record_type& record_schema,
                      std::string_view field_name,
                      previous_values_type& previous_values,
                      operator_control_plane& ctrl) const -> bool {
    if (item.req == requirement::none) {
      return true;
    }
    if (std::holds_alternative<schema_name>(item.field_value)
        || std::holds_alternative<attribute_value>(item.field_value)) {
      return true;
    }
    auto idx = slice.schema().resolve_key_or_concept(field_name);
    if (not idx) {
      diagnostic::error("could not find field `{}` in schema `{}`", field_name,
                        slice.schema().name())
        .note("from `{}`", name())
        .emit(ctrl.diagnostics());
      return false;
    }
    const auto& element_type = record_schema.field(*idx).type;
    auto element_array = idx->get(struct_array);
    TENZIR_ASSERT(element_array);
    auto& prev_values = previous_values[item.key];
    for (auto&& element : values(element_type, *element_array)) {
      auto data = materialize(element);
      switch (item.req) {
        case requirement::unique: {
          // For `unique`, we hold on to every value we've encountered,
          // and check if the new one is already in the set
          if (prev_values.contains(data)) {
            diagnostic::error("chart operator requires the value for `{}` (in "
                              "field `{}`) to hold unique values",
                              item.key, field_name)
              .note("duplicate value: `{}`", data)
              .emit(ctrl.diagnostics());
            return false;
          }
          break;
        }
        case requirement::strictly_increasing: {
          if (prev_values.empty()) {
            break;
          }
          // Optimization: with `strictly_increasing`, we'll only ever hold the
          // latest, largest value in the set
          TENZIR_ASSERT(prev_values.size() == 1);
          const auto& last = *prev_values.begin();
          if (not evaluate(last, relational_operator::less, data)) {
            diagnostic::error("chart operator requires the value for `{}` (in "
                              "field `{}`) to hold strictly increasing values",
                              item.key, field_name)
              .note("offending value: `{}`, highest value: `{}`", data, last)
              .emit(ctrl.diagnostics());
            return false;
          }
          prev_values.clear();
          break;
        }
        case requirement::none:
        default:
          TENZIR_UNREACHABLE();
      }
      prev_values.emplace(std::move(data));
    }
    return true;
  }

  auto resolve_attribute_value(const configuration_item& item,
                               const record_type& schema,
                               std::string_view schema_name_,
                               operator_control_plane& ctrl) const
    -> std::optional<std::string_view> {
    auto visitor = detail::overload{
      // `nth_field`:
      // Value of the attribute is the name of the `f.index`th field.
      // Default for --x-axis and --y-axis
      [&](nth_field f) -> std::optional<std::string_view> {
        if (schema.num_fields() <= f.index) {
          diagnostic::error("field at index {} not found in input "
                            "(schema `{}`), but the chart operator "
                            "expected it "
                            "for `{}`",
                            f.index, schema_name_, item.key)
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          return std::nullopt;
        }
        auto field = schema.field(f.index);
        if (is_container(field.type)) {
          diagnostic::error(
            "field at index {} (name: `{}`) in input (schema `{}`) has an "
            "incompatible type (`{}`) for use as `{}`",
            f.index, field.name, schema_name_, field.type.name(), item.key)
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          return std::nullopt;
        }
        return field.name;
      },
      // `schema_name`:
      // Value of the attribute is the name of the schema.
      // Default for --title
      [&](schema_name) -> std::optional<std::string_view> {
        return schema_name_;
      },
      // `field_name`:
      // Value of the attribute is `f.field`.
      // The schema is checked for such a field.
      // Used when the field name is explicitly specified as an argument,
      // through --x-axis/--y-axis/--name/--value
      [&](const field_name& f) -> std::optional<std::string_view> {
        if (not schema.resolve_key_or_concept(f.field, schema_name_)) {
          diagnostic::error("field `{}` not found in input (schema `{}`), "
                            "but the chart operator expected it for `{}`",
                            f.field, schema_name_, item.key)
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          return std::nullopt;
        }
        return f.field;
      },
      // `attribute_value`:
      // Value of the attribute is `a.attr`, as-is.
      // Used for the chart type attribute, and when the title is explicitly
      // specified as an argument through --title
      [](const attribute_value& a) -> std::optional<std::string_view> {
        return a.attr;
      },
      [](unset_default_tag) -> std::optional<std::string_view> {
        TENZIR_UNREACHABLE();
      },
    };
    return std::visit(visitor, item.field_value);
  }

  configuration cfg_{};
};

enum class flag_type {
  // --flag <field>,
  // where <field> specifies the field in the schema to use.
  field_name,
  // --flag <value>,
  // where <value> specified the actual value to use for the attribute.
  attribute_value,
};

struct chart_definition {
  struct required_argument_definition {
    std::string_view attr;
    std::string_view flag;
    flag_type type;
    requirement req{requirement::none};
  };
  struct optional_argument_definition {
    std::string_view attr;
    std::string_view flag;
    flag_type type;
    field_value_type default_;
    requirement req{requirement::none};
  };
  template <typename Val, typename Def>
  struct value_and_definition {
    Val value;
    const Def* definition;
  };

  std::string_view type;
  std::vector<required_argument_definition> required_flags;
  std::vector<optional_argument_definition> optional_flags;

  auto parse_arguments(parser_interface& p, std::string&& docs) const
    -> configuration {
    argument_parser parser{fmt::format("chart {}", type), std::move(docs)};
    // Build up lists of arguments to be given to the `argument_parser`,
    // based on the definitions
    auto required_arguments
      = build_argument_list<std::string>(parser, required_flags);
    auto optional_arguments
      = build_argument_list<std::optional<std::string>>(parser, optional_flags);
    parser.parse(p);
    configuration result;
    result.emplace_back("chart", attribute_value{std::string{type}});
    // Go through the arguments,
    // and populate `result` with the specified attributes
    for (auto&& [attr, arg] : required_arguments) {
      if (arg.definition->req != requirement::none
          && arg.definition->type != flag_type::field_name) {
        diagnostic::error("flag_type other than field_name is only compatible "
                          "with requirement::none")
          .note("internal configuration logic error in `chart`")
          .throw_();
      }
      if (arg.definition->type == flag_type::field_name) {
        result.emplace_back(std::string{attr}, field_name{std::move(arg.value)},
                            arg.definition->req);
      } else {
        result.emplace_back(std::string{attr},
                            attribute_value{std::move(arg.value)},
                            arg.definition->req);
      }
    }
    for (auto&& [attr, arg] : optional_arguments) {
      if (arg.definition->req != requirement::none
          && arg.definition->type != flag_type::field_name) {
        diagnostic::error("flag_type other than field_name is only compatible "
                          "with requirement::none")
          .note("internal configuration logic error in `chart`")
          .throw_();
      }
      if (!arg.value) {
        // Optional argument wasn't set, use the default value
        result.emplace_back(std::string{attr}, arg.definition->default_,
                            arg.definition->req);
      } else if (arg.definition->type == flag_type::field_name) {
        result.emplace_back(std::string{attr},
                            field_name{std::move(*arg.value)},
                            arg.definition->req);
      } else {
        result.emplace_back(std::string{attr},
                            attribute_value{std::move(*arg.value)},
                            arg.definition->req);
      }
    }
    return result;
  }

private:
  template <typename ArgumentType, typename Field>
  auto build_argument_list(argument_parser& parser,
                           const std::vector<Field>& defs) const
    -> detail::stable_map<std::string_view,
                          value_and_definition<ArgumentType, Field>> {
    detail::stable_map<std::string_view,
                       value_and_definition<ArgumentType, Field>>
      arguments;
    arguments.reserve(defs.size());
    for (const auto& def : defs) {
      auto [iter, inserted]
        = arguments.emplace(def.attr, value_and_definition<ArgumentType, Field>{
                                        ArgumentType{}, &def});
      TENZIR_DIAG_ASSERT(inserted);
      parser.add(def.flag, iter->second.value, [&]() {
        if (def.type == flag_type::field_name) {
          return std::string{"<field>"};
        }
        return fmt::format("<{}>", def.attr);
      }());
    }
    return arguments;
  }
};

// Definitions of all supported chart types
chart_definition chart_definitions[] = {
  // `line` chart has flags --x-axis, --y-axis and --title
  // --x-axis defaults to the first field, --y-axis to the second,
  // --title to the schema name
  {.type = "line",
   .required_flags = {},
   .optional_flags
   = {{.attr = "x", .flag = "-x,--x-axis", .type = flag_type::field_name, .default_ = nth_field{0}, .req = requirement::strictly_increasing,},
      {.attr = "y", .flag = "-y,--y-axis", .type = flag_type::field_name, .default_ = nth_field{1},},
      {.attr = "title", .flag = "--title", .type = flag_type::attribute_value, .default_ = schema_name{},},}},
  // `area` is equivalent to `line`
  {.type = "area",
   .required_flags = {},
   .optional_flags
   = {{.attr = "x", .flag = "-x,--x-axis", .type = flag_type::field_name, .default_ = nth_field{0}, .req = requirement::strictly_increasing,},
      {.attr = "y", .flag = "-y,--y-axis", .type = flag_type::field_name, .default_ = nth_field{1},},
      {.attr = "title", .flag = "--title", .type = flag_type::attribute_value, .default_ = schema_name{},},}},
  // `bar` is equivalent to `line`
  {.type = "bar",
   .required_flags = {},
   .optional_flags
   = {{.attr = "x", .flag = "-x,--x-axis", .type = flag_type::field_name, .default_ = nth_field{0}, .req = requirement::unique,},
      {.attr = "y", .flag = "-y,--y-axis", .type = flag_type::field_name, .default_ = nth_field{1},},
      {.attr = "title", .flag = "--title", .type = flag_type::attribute_value, .default_ = schema_name{},},}},
  // `pie` chart is equivalent to `line` and `bar`, except
  // --x-axis is called --name, and --y-axis is called --value
  {.type = "pie",
   .required_flags = {},
   .optional_flags
   = {{.attr = "x", .flag = "--name", .type = flag_type::field_name, .default_ = nth_field{0}, .req = requirement::unique,},
      {.attr = "y", .flag = "--value", .type = flag_type::field_name, .default_ = nth_field{1},},
      {.attr = "title", .flag = "--title", .type = flag_type::attribute_value, .default_ = schema_name{},},}},
};

class plugin final : public virtual operator_plugin<chart_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto docs = fmt::format("https://docs.tenzir/operators/{}", name());
    // The chart operator is of the form
    // `chart <type> [args...]`
    // Here, we'll parse the <type>
    auto type = p.accept_shell_arg();
    if (not type) {
      diagnostic::error("expected chart type as an argument")
        .primary(p.current_span())
        .docs(docs)
        .throw_();
    }
    const auto* chart_def_iterator = std::ranges::find(
      chart_definitions, type->inner, [](const chart_definition& def) {
        return def.type;
      });
    if (chart_def_iterator == std::ranges::end(chart_definitions)) {
      diagnostic::error("invalid chart type")
        .primary(type->source)
        .hint("valid chart types are: {}",
              fmt::join(chart_definitions
                          | std::views::transform(&chart_definition::type),
                        ", "))
        .docs(docs)
        .throw_();
    }
    // Forward the rest of the arguments to the
    // `parse_arguments` member of the chart definition
    const auto& chart_def = *chart_def_iterator;
    auto config = chart_def.parse_arguments(p, std::move(docs));
    return std::make_unique<chart_operator>(std::move(config));
  }
};

} // namespace

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::plugin)
