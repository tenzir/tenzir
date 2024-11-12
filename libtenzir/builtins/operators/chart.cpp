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
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/algorithms.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/variant.hpp>

#include <ranges>

namespace tenzir::plugins::chart {
namespace {

struct unset_default_tag {
  friend auto inspect(auto&, unset_default_tag&) -> bool {
    TENZIR_UNREACHABLE();
  }
};
struct nth_field {
  static constexpr size_t all_the_rest = std::numeric_limits<size_t>::max();

  size_t index;
  size_t count{1};

  friend auto inspect(auto& f, nth_field& x) -> bool {
    return f.object(x)
      .pretty_name("nth_field")
      .fields(f.field("index", x.index), f.field("count", x.count));
  }
};
struct schema_name {
  friend auto inspect(auto& f, schema_name& x) -> bool {
    return f.object(x).pretty_name("schema_name").fields();
  }
};
struct field_name {
  std::vector<std::string> fields;

  friend auto inspect(auto& f, field_name& x) -> bool {
    return f.object(x)
      .pretty_name("field_name")
      .fields(f.field("fields", x.fields));
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

using field_value_type = variant<unset_default_tag, nth_field, schema_name,
                                 field_name, attribute_value>;

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

  // Using a `deque` to guarantee reference validity after a growing `resize`,
  // which may happen in `get_attribute_key` if a type has more fields
  // than the previous one.
  mutable std::deque<std::string> indexed_attribute_keys{};

  /// Returns the number of fields this `configuration_item`
  /// refers to in the input.
  auto count_fields(const record_type& ty) const -> size_t {
    return std::visit(detail::overload{
                        [](const auto&) -> size_t {
                          return 1;
                        },
                        [&](const nth_field& x) -> size_t {
                          if (x.count == nth_field::all_the_rest) {
                            return ty.num_fields() - x.index;
                          }
                          return x.count;
                        },
                        [](const field_name& x) -> size_t {
                          return x.fields.size();
                        },
                      },
                      field_value);
  }

  /// Returns a `string_view`, valid for the lifetime of `*this`,
  /// which contains the name of the attribute `*this` describes.
  /// (Required by the constructor of `type`, which takes
  /// `attribute_view`s, which contain `string_view`s).
  ///
  /// Returns `key` for the first field, and `key<index>` for the remaining
  /// fields, with the index starting at 1.
  auto get_attribute_key(const record_type& ty, size_t index) const
    -> std::string_view {
    const auto field_count = count_fields(ty);
    if (index >= indexed_attribute_keys.size()) {
      indexed_attribute_keys.resize(field_count);
    }
    if (indexed_attribute_keys[index].empty()) {
      indexed_attribute_keys[index]
        = index == 0 ? key : fmt::format("{}{}", key, index);
    }
    return indexed_attribute_keys[index];
  }

  friend auto inspect(auto& f, configuration_item& x) -> bool {
    return f.object(x)
      .pretty_name("configuration_item")
      .fields(f.field("key", x.key), f.field("field_value", x.field_value),
              f.field("req", x.req));
  }
};

using configuration = std::vector<configuration_item>;

auto limit_as_number(const configuration& cfg) -> std::optional<uint64_t> {
  auto cfg_it = std::ranges::find(cfg, "limit", &configuration_item::key);
  TENZIR_ASSERT(cfg_it != cfg.end());
  auto& cfg_item = *cfg_it;
  TENZIR_ASSERT(std::holds_alternative<attribute_value>(cfg_item.field_value));
  auto& attr_value = std::get<attribute_value>(cfg_item.field_value);
  const auto attr_begin = attr_value.attr.c_str();
  const auto attr_end = attr_begin + attr_value.attr.size();
  auto res = uint64_t{};
  auto [ptr, ec] = std::from_chars(attr_begin, attr_end, res);
  if (ec != std::errc{}) {
    return std::nullopt;
  }
  return res;
}

class chart_operator final : public crtp_operator<chart_operator> {
public:
  chart_operator() = default;

  explicit chart_operator(struct location loc, configuration&& cfg)
    : loc_{loc}, cfg_(std::move(cfg)) {
  }

  // Keys are keys into `cfg_`, combined with an index
  // ((`y`, 0) refers to the first `y`-field, etc.).
  // Values are a set of all the previously encountered values in that field
  //
  // An entry is only added if the field's `req` value is not
  // `requirement::none`.
  using previous_values_type
    = detail::flat_map<std::pair<std::string, size_t>, std::unordered_set<data>>;

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // Cache attribute-enriched schemas, to avoid the potentially expensive
    // operation of building a list of attributes by visiting `cfg_` for every
    // iteration
    auto limit = uint64_t{0};
    {
      auto l = limit_as_number(cfg_);
      TENZIR_ASSERT(l);
      limit = *l;
    }
    auto remaining = limit;
    std::unordered_map<type, type> enriched_schemas_cache{};
    previous_values_type previous_values{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (remaining == 0) {
        co_yield {};
        continue;
      }
      if (slice.rows() > remaining) {
        slice = subslice(slice, 0, remaining);
        diagnostic::warning("chart reached event limit of `{}`", limit)
          .hint("You can use `--limit `value` to change this limit")
          .primary(loc_)
          .emit(ctrl.diagnostics());
        remaining = 0;
      } else {
        remaining -= slice.rows();
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
    return f.object(x).pretty_name("chart").fields(f.field("config", x.cfg_),
                                                   f.field("loc", x.loc_));
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
      const auto count = item.count_fields(record_schema);
      if (item.req == requirement::none) {
        continue;
      }
      for (size_t index = 0; index < count; ++index) {
        auto field_name = resolve_attribute_value(
          item, record_schema, slice.schema().name(), index, ctrl);
        if (not field_name) {
          return false;
        }
        if (not verify_single_field(item, slice, *struct_array, record_schema,
                                    *field_name, previous_values, index,
                                    ctrl)) {
          return false;
        }
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
      const auto count = item.count_fields(record_schema);
      for (size_t index = 0; index < count; ++index) {
        auto field_name = resolve_attribute_value(
          item, record_schema, slice.schema().name(), index, ctrl);
        if (not field_name) {
          return std::nullopt;
        }
        if (not verify_single_field(item, slice, *struct_array, record_schema,
                                    *field_name, previous_values, index,
                                    ctrl)) {
          return std::nullopt;
        }
        result.emplace_back(item.get_attribute_key(record_schema, index),
                            *field_name);
      }
    }
    return result;
  }

  auto
  verify_single_field(const configuration_item& item, const table_slice& slice,
                      const arrow::StructArray& struct_array,
                      const record_type& record_schema,
                      std::string_view field_name,
                      previous_values_type& previous_values, size_t index,
                      operator_control_plane& ctrl) const -> bool {
    if (item.req == requirement::none) {
      return true;
    }
    if (std::holds_alternative<schema_name>(item.field_value)
        || std::holds_alternative<attribute_value>(item.field_value)) {
      return true;
    }
    auto idx = slice.schema().resolve_key_or_concept_once(field_name);
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
    auto& prev_values = previous_values[std::pair{item.key, index}];
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
                               std::string_view schema_name_, size_t index,
                               operator_control_plane& ctrl) const
    -> std::optional<std::string_view> {
    auto visitor = detail::overload{
      // `nth_field`:
      // Value of the attribute is the name of the `f.index`th field.
      // Default for `x` and `y`.
      [&](const nth_field& f) -> std::optional<std::string_view> {
        if (schema.num_fields() <= f.index + index) {
          diagnostic::error("field at index {} not found in input "
                            "(schema `{}`), but the chart operator "
                            "expected it for `{}`",
                            f.index + index, schema_name_,
                            item.get_attribute_key(schema, index))
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          return std::nullopt;
        }
        auto field = schema.field(f.index + index);
        if (is_container(field.type)) {
          diagnostic::error("field at index {} (name: `{}`) in input (schema "
                            "`{}`) has an "
                            "incompatible type (`{}`) for use as `{}`",
                            f.index + index, field.name, schema_name_,
                            field.type.name(),
                            item.get_attribute_key(schema, index))
            .hint("to be charted, a value cannot be a list or a record")
            .hint("either explicitly specify fields to use for charting in "
                  "`chart`, or choose the fields with the `select` or `drop` "
                  "operator")
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          return std::nullopt;
        }
        return field.name;
      },
      // `schema_name`:
      // Value of the attribute is the name of the schema.
      // Used to be the default for `title` (such option no longer exists).
      [&](schema_name) -> std::optional<std::string_view> {
        TENZIR_ASSERT(index == 0);
        return schema_name_;
      },
      // `field_name`:
      // Value of the attribute is `f.field`.
      // The schema is checked for such a field.
      // Used when the field name is explicitly specified as an argument,
      // through `-x`/`-y`/`--name`/`--value`.
      [&](const field_name& f) -> std::optional<std::string_view> {
        auto offset
          = schema.resolve_key_or_concept_once(f.fields[index], schema_name_);
        if (not offset) {
          diagnostic::error("field `{}` not found in input (schema `{}`), "
                            "but the chart operator expected it for `{}`",
                            f.fields[index], schema_name_,
                            item.get_attribute_key(schema, index))
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          return {};
        }
        if (auto field = schema.field(*offset); is_container(field.type)) {
          diagnostic::error("field `{}` in input (schema `{}`) has an "
                            "incompatible type (`{}`) for use as `{}`",
                            f.fields[index], schema_name_, field.type.name(),
                            item.get_attribute_key(schema, index))
            .hint("to be charted, a value cannot be a list or a record")
            .hint("either explicitly specify fields to use for charting in "
                  "`chart`, or choose the fields with the `select` or `drop` "
                  "operator")
            .note("from `{}`", name())
            .emit(ctrl.diagnostics());
          return std::nullopt;
        }
        return f.fields[index];
      },
      // `attribute_value`:
      // Value of the attribute is `a.attr`, as-is.
      // Used for the chart type attribute, and when the title is explicitly
      // specified as an argument through `--title`.
      [&](const attribute_value& a) -> std::optional<std::string_view> {
        TENZIR_ASSERT(index == 0);
        return a.attr;
      },
      [](unset_default_tag) -> std::optional<std::string_view> {
        TENZIR_UNREACHABLE();
      },
    };
    return std::visit(visitor, item.field_value);
  }
  struct location loc_ {};
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
    bool allow_lists{false};
    requirement req{requirement::none};
  };
  struct optional_argument_definition {
    std::string_view attr;
    std::string_view flag;
    flag_type type;
    field_value_type default_;
    bool allow_lists{false};
    requirement req{requirement::none};
  };
  template <typename Val, typename Def>
  struct value_and_definition {
    Val value;
    const Def* definition;
  };

  using verification_callback
    = std::function<auto(configuration&)->std::optional<diagnostic>>;

  std::string_view type;
  std::vector<required_argument_definition> required_flags;
  std::vector<optional_argument_definition> optional_flags;
  std::vector<verification_callback> verifications{};

  auto parse_arguments(parser_interface& p, std::string&& docs) const
    -> configuration {
    argument_parser parser{fmt::format("chart {}", type), std::move(docs)};
    // Build up lists of arguments to be given to the `argument_parser`,
    // based on the definitions
    auto required_single_arguments = build_argument_list<std::string>(
      parser, required_flags | std::views::filter([](const auto& def) {
                return not def.allow_lists;
              }));
    auto required_list_arguments
      = build_argument_list<std::vector<std::string>>(
        parser, required_flags | std::views::filter([](const auto& def) {
                  return def.allow_lists;
                }));
    auto optional_single_arguments
      = build_argument_list<std::optional<std::string>>(
        parser, optional_flags | std::views::filter([](const auto& def) {
                  return not def.allow_lists;
                }));
    auto optional_list_arguments
      = build_argument_list<std::optional<std::vector<std::string>>>(
        parser, optional_flags | std::views::filter([](const auto& def) {
                  return def.allow_lists;
                }));
    // TODO: Before this was rebased to use the normal argument parser again,
    // the arguments added to the custom parser were put into the right order.
    // However, the normal argument parser does not support this. This causes
    // the arguments to appear in a different order than desired.
    parser.parse(p);
    configuration result;
    result.emplace_back("chart", attribute_value{std::string{type}});
    for (auto& arg : required_single_arguments) {
      if (detail::contains(arg.second.value, ',')) {
        diagnostic::error("option `{}` can only accept a single value, not a "
                          "list",
                          arg.second.definition->flag)
          .throw_();
      }
    }
    for (auto& arg : optional_single_arguments) {
      if (arg.second.value && detail::contains(*arg.second.value, ',')) {
        diagnostic::error("option `{}` can only accept a single value, not a "
                          "list",
                          arg.second.definition->flag)
          .throw_();
      }
    }
    // Go through the arguments,
    // and populate `result` with the specified attributes
    for (auto&& [attr, arg] : required_single_arguments) {
      if (arg.definition->req != requirement::none
          && arg.definition->type != flag_type::field_name) {
        diagnostic::error("flag_type other than field_name is only compatible "
                          "with requirement::none")
          .note("internal configuration logic error in `chart`")
          .throw_();
      }
      if (arg.definition->type == flag_type::field_name) {
        result.emplace_back(std::string{attr},
                            field_name{{std::move(arg.value)}},
                            arg.definition->req);
      } else {
        result.emplace_back(std::string{attr},
                            attribute_value{std::move(arg.value)},
                            arg.definition->req);
      }
    }
    for (auto&& [attr, arg] : required_list_arguments) {
      if (arg.definition->req != requirement::none
          && arg.definition->type != flag_type::field_name) {
        diagnostic::error("flag_type other than field_name is only compatible "
                          "with requirement::none")
          .note("internal configuration logic error in `chart`")
          .throw_();
      }
      if (arg.definition->type != flag_type::field_name) {
        diagnostic::error(
          "allow_lists=true is only compatible with flag_type::field_name")
          .note("internal configuration logic error in `chart`")
          .throw_();
      }
      result.emplace_back(std::string{attr}, field_name{std::move(arg.value)},
                          arg.definition->req);
    }
    for (auto&& [attr, arg] : optional_single_arguments) {
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
                            field_name{{std::move(*arg.value)}},
                            arg.definition->req);
      } else {
        result.emplace_back(std::string{attr},
                            attribute_value{std::move(*arg.value)},
                            arg.definition->req);
      }
    }
    for (auto&& [attr, arg] : optional_list_arguments) {
      if (arg.definition->req != requirement::none
          && arg.definition->type != flag_type::field_name) {
        diagnostic::error("flag_type other than field_name is only compatible "
                          "with requirement::none")
          .note("internal configuration logic error in `chart`")
          .throw_();
      }
      if (arg.definition->type != flag_type::field_name) {
        diagnostic::error(
          "allow_lists=true is only compatible with flag_type::field_name")
          .note("internal configuration logic error in `chart`")
          .throw_();
      }
      if (!arg.value) {
        result.emplace_back(std::string{attr}, arg.definition->default_,
                            arg.definition->req);
      } else {
        result.emplace_back(std::string{attr},
                            field_name{std::move(*arg.value)},
                            arg.definition->req);
      }
    }
    for (const auto& verify : verifications) {
      if (auto diag = verify(result)) {
        std::move(*diag).modify().usage(parser.usage()).docs(docs).throw_();
      }
    }
    return result;
  }

private:
  template <typename ArgumentType, std::ranges::forward_range Definitions,
            typename FieldType = std::ranges::range_value_t<Definitions>>
  auto build_argument_list(argument_parser& parser, Definitions&& defs) const
    -> detail::stable_map<std::string_view,
                          value_and_definition<ArgumentType, FieldType>> {
    detail::stable_map<std::string_view,
                       value_and_definition<ArgumentType, FieldType>>
      arguments;
    arguments.reserve(std::ranges::distance(defs));
    for (const auto& def : defs) {
      auto [iter, inserted] = arguments.emplace(
        def.attr,
        value_and_definition<ArgumentType, FieldType>{ArgumentType{}, &def});
      TENZIR_DIAG_ASSERT(inserted);
      parser.add(def.flag, iter->second.value, [&]() {
        if (def.type == flag_type::field_name) {
          if (def.allow_lists) {
            return std::string{"<fields>"};
          }
          return std::string{"<field>"};
        }
        return fmt::format("<{}>", def.attr);
      }());
    }
    return arguments;
  }
};

auto disallow_mixmatch_between_explicit_and_implicit_arguments(
  std::vector<std::string_view> args)
  -> chart_definition::verification_callback {
  return
    [args = std::move(args)](configuration& cfg) -> std::optional<diagnostic> {
      if (args.empty()) {
        return std::nullopt;
      }
      bool has_nth_field_args = false;
      bool has_field_name_args = false;
      for (const auto& item : cfg) {
        if (std::holds_alternative<nth_field>(item.field_value)) {
          has_nth_field_args = true;
        } else if (std::holds_alternative<field_name>(item.field_value)) {
          has_field_name_args = true;
        }
      }
      if (has_nth_field_args && has_field_name_args) {
        const auto args_spelled_out = [&]() -> std::string {
          if (args.size() == 1) {
            return fmt::format("`{}`", args.front());
          }
          if (args.size() == 2) {
            return fmt::format("`{}` and `{}`", args[0], args[1]);
          }
          return fmt::format(
            "{}, and `{}`",
            fmt::join(args | std::views::take(args.size() - 1)
                        | std::views::transform([](const auto& val) {
                            return fmt::format("`{}`", val);
                          }),
                      ", "),
            args.back());
        }();
        return diagnostic::error("failed to infer fields to use for charting")
          .hint("either specify {} as {}, and "
                "utilize the `select` operator",
                args_spelled_out,
                args.size() == 1 ? "an argument explicitly, or don't"
                                 : "arguments explicitly, or none of them")
          .done();
      }
      return std::nullopt;
    };
}

auto require_attribute_value_one_of(std::string_view attr,
                                    std::vector<std::string_view> values)
  -> chart_definition::verification_callback {
  return [attr, values = std::move(values)](
           configuration& cfg) -> std::optional<diagnostic> {
    auto cfg_it = std::ranges::find(cfg, attr, &configuration_item::key);
    TENZIR_ASSERT(cfg_it != cfg.end());
    auto& cfg_item = *cfg_it;
    TENZIR_ASSERT(
      std::holds_alternative<attribute_value>(cfg_item.field_value));
    auto& attr_value = std::get<attribute_value>(cfg_item.field_value);
    if (not std::ranges::any_of(values, [&](std::string_view sv) {
          return sv == attr_value.attr;
        })) {
      return diagnostic::error("invalid value for option `{}`", attr)
        .hint("value must be one of the following: {}", fmt::join(values, ", "))
        .done();
    }
    return std::nullopt;
  };
}

auto require_limit_is_valid_number() -> chart_definition::verification_callback {
  return [](configuration& cfg) -> std::optional<diagnostic> {
    auto res = limit_as_number(cfg);
    if (not res) {
      return diagnostic::error("invalid value for option `limit`")
        .hint("argument must a positive integer")
        .done();
    }
    return std::nullopt;
  };
}

constexpr std::string_view default_limit = "10000";
// Definitions of all supported chart types
chart_definition chart_definitions[] = {
  // `line` chart has flags `x`, `y`, and `position`.
  // `x` defaults to the first field, `y` to all the rest.
  // If `x` or `y` is specified, the other must be, too
  // (specified by the `disallow_mixmatch...` verification).
  {
    .type = "line",
    .required_flags = {},
    .optional_flags = {
      {.attr = "x", .flag = "-x,--x-axis", .type = flag_type::field_name, .default_ = nth_field{0}, .allow_lists = false, .req = requirement::strictly_increasing,},
      {.attr = "y", .flag = "-y,--y-axis", .type = flag_type::field_name, .default_ = nth_field{1, nth_field::all_the_rest}, .allow_lists = true,},
      {.attr = "position", .flag = "--position", .type = flag_type::attribute_value, .default_ = attribute_value{"grouped"}, .allow_lists = false,},
      {.attr = "x_axis_type", .flag = "--x-axis-type", .type = flag_type::attribute_value, .default_ = attribute_value{"linear"}, .allow_lists = false,},
      {.attr = "y_axis_type", .flag = "--y-axis-type", .type = flag_type::attribute_value, .default_ = attribute_value{"linear"}, .allow_lists = false,},
      {.attr = "limit", .flag = "--limit", .type = flag_type::attribute_value, .default_ = attribute_value{std::string{default_limit}}, .allow_lists = false,},
    },
    .verifications = {
      disallow_mixmatch_between_explicit_and_implicit_arguments({"x", "y"}),
      require_attribute_value_one_of("position", {"grouped", "stacked"}),
      require_attribute_value_one_of("x_axis_type", {"log", "linear"}),
      require_attribute_value_one_of("y_axis_type", {"log", "linear"}),
      require_limit_is_valid_number(),
    },
  },
  // `area` is equivalent to `line`.
  {
    .type = "area",
    .required_flags = {},
    .optional_flags = {
      {.attr = "x", .flag = "-x,--x-axis", .type = flag_type::field_name, .default_ = nth_field{0}, .allow_lists = false, .req = requirement::strictly_increasing,},
      {.attr = "y", .flag = "-y,--y-axis", .type = flag_type::field_name, .default_ = nth_field{1, nth_field::all_the_rest}, .allow_lists = true,},
      {.attr = "position", .flag = "--position", .type = flag_type::attribute_value, .default_ = attribute_value{"grouped"}, .allow_lists = false,},
      {.attr = "x_axis_type", .flag = "--x-axis-type", .type = flag_type::attribute_value, .default_ = attribute_value{"linear"}, .allow_lists = false,},
      {.attr = "y_axis_type", .flag = "--y-axis-type", .type = flag_type::attribute_value, .default_ = attribute_value{"linear"}, .allow_lists = false,},
      {.attr = "limit", .flag = "--limit", .type = flag_type::attribute_value, .default_ = attribute_value{std::string{default_limit}}, .allow_lists = false,},
    },
    .verifications = {
      disallow_mixmatch_between_explicit_and_implicit_arguments({"x", "y"}),
      require_attribute_value_one_of("position", {"grouped", "stacked"}),
      require_attribute_value_one_of("x_axis_type", {"log", "linear"}),
      require_attribute_value_one_of("y_axis_type", {"log", "linear"}),
      require_limit_is_valid_number(),
    },
  },
  // `bar` is equivalent to `line`, except the requirement on `x` is for the values to be unique.
  {
    .type = "bar",
    .required_flags = {},
    .optional_flags = {
      {.attr = "x", .flag = "-x,--x-axis", .type = flag_type::field_name, .default_ = nth_field{0}, .allow_lists = false, .req = requirement::unique,},
      {.attr = "y", .flag = "-y,--y-axis", .type = flag_type::field_name, .default_ = nth_field{1, nth_field::all_the_rest}, .allow_lists = true,},
      {.attr = "position", .flag = "--position", .type = flag_type::attribute_value, .default_ = attribute_value{"grouped"}, .allow_lists = false,},
      {.attr = "x_axis_type", .flag = "--x-axis-type", .type = flag_type::attribute_value, .default_ = attribute_value{"linear"}, .allow_lists = false,},
      {.attr = "y_axis_type", .flag = "--y-axis-type", .type = flag_type::attribute_value, .default_ = attribute_value{"linear"}, .allow_lists = false,},
      {.attr = "limit", .flag = "--limit", .type = flag_type::attribute_value, .default_ = attribute_value{std::string{default_limit}}, .allow_lists = false,},
    },
    .verifications = {
      disallow_mixmatch_between_explicit_and_implicit_arguments({"x", "y"}),
      require_attribute_value_one_of("position", {"grouped", "stacked"}),
      require_attribute_value_one_of("x_axis_type", {"log", "linear"}),
      require_attribute_value_one_of("y_axis_type", {"log", "linear"}),
      require_limit_is_valid_number(),
    },
  },
  // `pie` chart is equivalent to `line` and `bar`, except
  // `x` is called `name`, and `y` is called `value`.
  {
    .type = "pie",
    .required_flags = {},
    .optional_flags = {
      {.attr = "x", .flag = "--name", .type = flag_type::field_name, .default_ = nth_field{0}, .allow_lists = false, .req = requirement::unique,},
      {.attr = "y", .flag = "--value", .type = flag_type::field_name, .default_ = nth_field{1, nth_field::all_the_rest}, .allow_lists = true,},
      {.attr = "limit", .flag = "--limit", .type = flag_type::attribute_value, .default_ = attribute_value{std::string{default_limit}}, .allow_lists = false,},
    },
    .verifications = {
      disallow_mixmatch_between_explicit_and_implicit_arguments({"x", "y"}),
      require_limit_is_valid_number(),
    },
  },
};

class plugin final : public virtual operator_plugin<chart_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto docs = fmt::format("https://docs.tenzir.com/operators/{}", name());
    // The chart operator is of the form
    // `chart <type> [args...]`
    // Here, we'll parse the <type>
    auto loc = p.current_span();
    loc.begin -= 5;
    loc.end -= 1;
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
    return std::make_unique<chart_operator>(loc, std::move(config));
  }
};

} // namespace

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::plugin)
