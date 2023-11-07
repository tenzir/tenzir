//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/set_attributes_operator_helper.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::chart {

namespace {

using detail::set_attributes_operator_helper;
using configuration = detail::set_attributes_operator_helper::configuration;

class chart_operator final : public crtp_operator<chart_operator> {
public:
  chart_operator() = default;

  explicit chart_operator(set_attributes_operator_helper&& helper)
    : helper_(std::move(helper)) {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      auto [result, err]
        = helper_.process(std::move(slice), check_schema_fields);
      if (err) {
        ctrl.warn(std::move(err));
      }
      co_yield result;
    }
  }

  auto name() const -> std::string override {
    return "chart";
  }

  auto to_string() const -> std::string override {
    return fmt::format("{} {}", name(), helper_.get_config().to_string());
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, chart_operator& x) -> bool {
    return f.apply(x.helper_.get_config());
  }

private:
  static auto check_schema_fields(const type& schema, const configuration& cfg)
    -> caf::error {
    if (schema.type_index() != record_type::type_index) {
      return caf::make_error(ec::unspecified,
                             "chart operator expects input to be a record");
    }
    const auto& record = caf::get<record_type>(schema);
    for (auto&& attr : cfg.get_attributes()) {
      // "type" and "description" fields aren't expected to be found in the input
      if (attr.key == "type" || attr.key == "description")
        continue;
      if (record.resolve_key(attr.value))
        continue;
      return caf::make_error(ec::unspecified,
                             fmt::format("field `{}` not found in input, "
                                         "but the chart operator expected "
                                         "it for `{}`",
                                         attr.value, attr.key));
    }
    return {};
  }

  set_attributes_operator_helper helper_{};
};

class plugin final : public virtual operator_plugin<chart_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    // chart operator is of the form
    // `chart <type> [key=value]...`
    // Here, we'll parse <type>, and forward the rest to `set-attributes`
    using parsers::required_ws_or_comment, parsers::alnum, parsers::chr;
    const auto type_parser = +(alnum | chr{'-'} | chr{'_'});
    const auto* first = pipeline.begin();
    const auto* const last = pipeline.end();
    const auto p = required_ws_or_comment >> type_parser;
    std::string chart_type;
    if (not p(first, last, chart_type))
      return {std::string_view{first, last},
              caf::make_error(ec::syntax_error,
                              fmt::format("failed to parse chart type: `{}`",
                                          pipeline))};
    set_attributes_operator_helper helper{
      configuration{{std::pair{"type", std::move(chart_type)}}}};
    auto [sv, err] = helper.parse(std::string_view{first, last}, check_options);
    if (err)
      return {sv, err};
    return {sv, std::make_unique<chart_operator>(std::move(helper))};
  }

private:
  static auto check_options(const configuration& cfg) -> caf::error {
    auto attributes = collect(cfg.get_attributes(), cfg.count_attributes());
    // Removes arg with key `key` from `args`, and returns the value
    const auto remove_arg_if_present
      = [&](std::string_view key) -> std::optional<std::string_view> {
      const auto it
        = std::ranges::find(attributes, key, [](const type::attribute_view& x) {
            return x.key;
          });
      if (it == attributes.end())
        return std::nullopt;
      auto elem = *it;
      attributes.erase(it);
      return elem.value;
    };
    const auto type = *remove_arg_if_present("type");
    const auto require_field = [&](std::string_view name) {
      if (not remove_arg_if_present(name))
        throw caf::make_error(ec::syntax_error,
                              fmt::format("chart type `{}` requires field "
                                          "`{}`",
                                          type, name));
    };
    const auto allow_field = [&](std::string_view name) {
      remove_arg_if_present(name);
    };
    const auto disallow_field = [&](std::string_view name) {
      if (remove_arg_if_present(name))
        throw caf::make_error(
          ec::syntax_error,
          fmt::format("chart type `{}` disallows field `{}`", type, name));
    };
    try {
      if (type == "stacked-area") {
        require_field("x");
        require_field("y");
        allow_field("x_unit");
        allow_field("y_unit");
        allow_field("label");
        allow_field("description");
        disallow_field("value");
      } else if (type == "donut" || type == "bar") {
        require_field("value");
        allow_field("label");
        allow_field("description");
        disallow_field("x");
        disallow_field("x_unit");
        disallow_field("y");
        disallow_field("y_unit");
      } else {
        return caf::make_error(ec::syntax_error,
                               fmt::format("invalid chart type `{}`, allowed "
                                           "types are: `stacked-area`, "
                                           "`donut`, and `bar`",
                                           type));
      }
      // Above, every time we found an attribute in `attributes` which we
      // expected to find, we also removed it from `attributes`.
      // After checking for every valid attribute, `attributes` should thus be
      // empty.
      if (not attributes.empty()) {
        return caf::make_error(
          ec::syntax_error,
          fmt::format("invalid argument given to chart: `{}` "
                      "with value `{}`",
                      attributes.front().key, attributes.front().value));
      }
    } catch (const caf::error& err) {
      return err;
    }
    return {};
  }
};

} // namespace

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::plugin)
