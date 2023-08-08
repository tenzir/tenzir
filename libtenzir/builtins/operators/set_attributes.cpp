//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/set_attributes_operator_helper.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::set_attributes {

namespace {

using detail::set_attributes_operator_helper;
using configuration = detail::set_attributes_operator_helper::configuration;

class set_attributes_operator final
  : public crtp_operator<set_attributes_operator> {
public:
  set_attributes_operator() = default;

  explicit set_attributes_operator(set_attributes_operator_helper&& helper)
    : helper_(std::move(helper)) {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      auto [result, err] = helper_.process(std::move(slice));
      if (err) {
        ctrl.warn(std::move(err));
      }
      co_yield result;
    }
  }

  auto name() const -> std::string override {
    return "set-attributes";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, set_attributes_operator& x) -> bool {
    return f.apply(x.helper_.get_config());
  }

private:
  set_attributes_operator_helper helper_{};
};

class plugin final : public virtual operator_plugin<set_attributes_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    set_attributes_operator_helper helper{};
    auto [sv, err] = helper.parse(pipeline);
    if (err)
      return {sv, err};
    return {sv, std::make_unique<set_attributes_operator>(std::move(helper))};
  }
};

} // namespace

} // namespace tenzir::plugins::set_attributes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_attributes::plugin)
