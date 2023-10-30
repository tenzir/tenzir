//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/cast.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::chart {

namespace {

struct chart_options {
  std::string type;
  std::optional<std::string> label;
  std::optional<std::string> x;
  std::optional<std::string> x_unit;
  std::optional<std::string> y;
  std::optional<std::string> y_unit;
  std::optional<std::string> value;
  std::optional<std::string> description;

  auto get_attribute_list() const -> const std::vector<type::attribute_view>& {
    if (not cached_attrs_.empty())
      return cached_attrs_;
    auto add_if_set
      = [&](std::string_view key, const std::optional<std::string>& val) {
          if (val)
            cached_attrs_.emplace_back(key, *val);
        };
    cached_attrs_.emplace_back("chart", type);
    add_if_set("label", label);
    add_if_set("x", x);
    add_if_set("x_unit", x_unit);
    add_if_set("y", x);
    add_if_set("y_unit", x_unit);
    add_if_set("value", value);
    add_if_set("description", description);
    return cached_attrs_;
  }

  friend auto inspect(auto& f, chart_options& opt) -> bool {
    return f.object(opt).fields(
      f.field("chart", opt.type), f.field("label", opt.label),
      f.field("x", opt.x), f.field("x_unit", opt.x_unit), f.field("y", opt.y),
      f.field("y_unit", opt.y_unit), f.field("value", opt.value),
      f.field("description", opt.description));
  }

private:
  mutable std::vector<type::attribute_view> cached_attrs_{};
};

class chart_operator final : public crtp_operator<chart_operator> {
public:
  chart_operator() = default;

  explicit chart_operator(chart_options&& opt) : options_(std::move(opt)) {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto& nested_schema = slice.schema();
      if (not check_schema_fields(nested_schema, ctrl)) {
        co_yield {};
        continue;
      }
      auto attrs = options_.get_attribute_list();
      auto new_schema = type{"tenzir.chart", nested_schema, std::move(attrs)};
      TENZIR_ASSERT(new_schema);
      co_yield cast(std::move(slice), new_schema);
    }
  }

  auto name() const -> std::string override {
    return "chart";
  }

  auto to_string() const -> std::string override {
    const auto format_attr_view = [](const type::attribute_view& attr) {
      if (attr.value.find_first_of(" \n\r\t\v\f") != std::string_view::npos)
        // Add quotes if `value` has whitespace
        return fmt::format("{}=\"{}\"", attr.key, attr.value);
      return fmt::format("{}={}", attr.key, attr.value);
    };
    return fmt::format("chart {}",
                       fmt::join(options_.get_attribute_list()
                                   | std::views::transform(format_attr_view),
                                 " "));
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, chart_operator& x) -> bool {
    return f.apply(x.options_);
  }

private:
  auto check_schema_fields(const type& schema,
                           operator_control_plane& ctrl) const -> bool {
    if (schema.type_index() != record_type::type_index) {
      ctrl.warn(caf::make_error(
        ec::unspecified,
        fmt::format("{} operator expects input to be a record", name())));
      return false;
    }
    const auto& record = caf::get<record_type>(schema);
    auto check_field_presence = [&](const auto& field,
                                    std::string_view field_name) {
      if (not field)
        return true;
      if (record.resolve_key(*field))
        return true;
      ctrl.warn(caf::make_error(ec::unspecified,
                                fmt::format("field '{}' not found in input, "
                                            "but the chart operator expected "
                                            "it for '{}'",
                                            *field, field_name)));
      return false;
    };
    return check_field_presence(options_.label, "label")
           && check_field_presence(options_.x, "x")
           && check_field_presence(options_.x_unit, "x_unit")
           && check_field_presence(options_.y, "y")
           && check_field_presence(options_.y_unit, "y_unit")
           && check_field_presence(options_.value, "value");
  }

  chart_options options_{};
};

class plugin final : public virtual operator_plugin<chart_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::alnum, parsers::chr,
      parsers::qqstr;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto name_char = (alnum | chr{'-'} | chr{'_'});
    const auto field_name_parser = +name_char;
    const auto object_name_parser
      = (field_name_parser % '.').then([](std::vector<std::string> in) {
          return fmt::to_string(fmt::join(in.begin(), in.end(), "."));
        });
    const auto type_argument_parser
      = field_name_parser.then([](std::string in) {
          return parsed_argument_list{{std::string{"type"}, std::move(in)}};
        });
    // Arguments are in the format of key=value,
    // where value can also be a "quoted string with spaces"
    const auto arguments_parser
      = (field_name_parser >> optional_ws_or_comment >> '='
         >> optional_ws_or_comment >> (qqstr | object_name_parser))
        % required_ws_or_comment;
    const auto p = required_ws_or_comment >> type_argument_parser
                   >> ~(required_ws_or_comment >> arguments_parser)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    parsed_argument_list parsed_arguments;
    if (!p(f, l, parsed_arguments))
      return {std::string_view{f, l},
              caf::make_error(
                ec::syntax_error,
                fmt::format("failed to parse chart operator: '{}'", pipeline))};
    auto options = make_options(parsed_arguments);
    if (not options)
      return {std::string_view{f, l}, options.error()};
    return {std::string_view{f, l},
            std::make_unique<chart_operator>(std::move(*options))};
  }

private:
  using parsed_argument_list
    = std::vector<std::tuple<std::string, std::string>>;

  static auto make_options(parsed_argument_list& args)
    -> caf::expected<chart_options> {
    const auto keys_projection = [](const auto& x) {
      return std::get<0>(x);
    };
    // Removes arg with key `key` from `args`, and returns the value
    const auto extract_arg_by_key
      = [&](std::string_view key) -> std::optional<std::string> {
      const auto it = std::ranges::find(args, key, keys_projection);
      if (it == args.end())
        return std::nullopt;
      auto elem = std::move(*it);
      args.erase(it);
      return std::string{std::move(std::get<1>(elem))};
    };
    chart_options options{};
    options.type = *extract_arg_by_key("type");
    const auto require_field
      = [&](std::optional<std::string>& field, std::string_view name) {
          auto value = extract_arg_by_key(name);
          if (not value)
            throw caf::make_error(ec::syntax_error,
                                  fmt::format("chart type '{}' requires field "
                                              "'{}'",
                                              options.type, name));
          field = std::move(*value);
        };
    const auto allow_field
      = [&](std::optional<std::string>& field, std::string_view name) {
          auto value = extract_arg_by_key(name);
          if (not value)
            return;
          field = std::move(*value);
        };
    const auto disallow_field = [&](std::string_view name) {
      auto value = extract_arg_by_key(name);
      if (value)
        throw caf::make_error(
          ec::syntax_error, fmt::format("chart type '{}' disallows field '{}'",
                                        options.type, name));
    };
    try {
      if (options.type == "stacked-area") {
        require_field(options.x, "x");
        require_field(options.y, "y");
        allow_field(options.x_unit, "x_unit");
        allow_field(options.y_unit, "y_unit");
        allow_field(options.label, "label");
        allow_field(options.description, "description");
        disallow_field("value");
      } else if (options.type == "donut" || options.type == "bar") {
        require_field(options.value, "value");
        allow_field(options.label, "label");
        allow_field(options.description, "description");
        disallow_field("x");
        disallow_field("x_unit");
        disallow_field("y");
        disallow_field("y_unit");
      } else {
        return caf::make_error(ec::syntax_error,
                               fmt::format("invalid chart type '{}', allowed "
                                           "types are: 'stacked-area', "
                                           "'donut', and 'bar'",
                                           options.type));
      }
      if (not args.empty()) {
        return caf::make_error(
          ec::syntax_error,
          fmt::format("invalid argument given to chart: '{}' "
                      "with value '{}'",
                      std::get<0>(args.front()), std::get<1>(args.front())));
      }
    } catch (const caf::error& err) {
      return err;
    }
    return options;
  }
};

} // namespace

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::plugin)
