//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/cast.hpp>
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

  // not private to enable aggregate initialization
  mutable std::vector<type::attribute_view> cached_attrs_{};
};

struct chart_arguments {
  located<std::string> type;
  std::optional<located<std::string>> label;
  std::optional<located<std::string>> x;
  std::optional<located<std::string>> x_unit;
  std::optional<located<std::string>> y;
  std::optional<located<std::string>> y_unit;
  std::optional<located<std::string>> value;
  std::optional<located<std::string>> description;

  auto make_chart_options() && -> chart_options {
    // TODO: replace with optional::transform in C++23
    auto unwrap_located = [](std::optional<located<std::string>>&& val)
      -> std::optional<std::string> {
      if (val)
        return {std::move(val->inner)};
      return std::nullopt;
    };
    return chart_options{.type = std::move(type.inner),
                         .label = unwrap_located(std::move(label)),
                         .x = unwrap_located(std::move(x)),
                         .x_unit = unwrap_located(std::move(x_unit)),
                         .y = unwrap_located(std::move(y)),
                         .y_unit = unwrap_located(std::move(y_unit)),
                         .value = unwrap_located(std::move(value)),
                         .description = unwrap_located(std::move(description))};
  }
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
    return fmt::format("chart {}",
                       fmt::join(options_.get_attribute_list(), " "));
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
      ctrl.warn(caf::make_error(
        ec::unspecified, fmt::format("field '{}' not found in input, "
                                     "but the {} operator expected it for '{}'",
                                     *field, name(), field_name)));
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

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"chart", ""};
    chart_arguments args;
    parser.add(args.type, "<type>");
    parser.add("-l,--label", args.label, "<label>");
    parser.add("-x,--x,--x-value", args.x, "<x-value>");
    parser.add("-xu,--x-unit", args.x_unit, "<x-unit>");
    parser.add("-y,--y,--y-value", args.y, "<y-value>");
    parser.add("-yu,--y-unit", args.y_unit, "<y-unit>");
    parser.add("-v,--value", args.value, "<value>");
    parser.add("-d,--description", args.description, "<description>");
    parser.parse(p);
    check_args(args, p, parser);
    return std::make_unique<chart_operator>(
      std::move(args).make_chart_options());
  }

private:
  static void check_args(const chart_arguments& args, parser_interface& p,
                         const argument_parser& parser) {
    const auto require_field = [&](const auto& field, std::string_view name) {
      if (field)
        return;
      diagnostic::error("chart type '{}' requires field '{}'", args.type.inner,
                        name)
        .primary(p.current_span())
        .usage(parser.usage())
        .throw_();
    };
    const auto disallow_field = [&](const auto& field, std::string_view name) {
      if (not field)
        return;
      diagnostic::error("chart type '{}' disallows field '{}'", args.type.inner,
                        name)
        .primary(field->source)
        .usage(parser.usage())
        .throw_();
    };
    if (args.type.inner == "stacked-area") {
      require_field(args.x, "x");
      require_field(args.y, "y");
      disallow_field(args.value, "value");
      return;
    }
    if (args.type.inner == "donut" || args.type.inner == "bar") {
      require_field(args.value, "value");
      disallow_field(args.x, "x");
      disallow_field(args.x_unit, "x_unit");
      disallow_field(args.y, "y");
      disallow_field(args.y_unit, "y_unit");
      return;
    }
    diagnostic::error("invalid chart type")
      .primary(args.type.source)
      .note("Allowed values are: 'stacked-area', 'donut', and 'bar'")
      .usage(parser.usage())
      .throw_();
  }
};

} // namespace

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::plugin)
