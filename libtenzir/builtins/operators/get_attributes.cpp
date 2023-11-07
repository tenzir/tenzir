//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

namespace tenzir::plugins::get_attributes {

namespace {

class get_attributes_operator final
  : public crtp_operator<get_attributes_operator> {
public:
  auto name() const -> std::string override {
    return "get-attributes";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto b = series_builder{};
      auto r = b.record();
      for (auto&& [name, value] : slice.schema().attributes()) {
        r.field(name).data(value);
      }
      for (auto&& out : b.finish_as_table_slice("tenzir.attributes")) {
        co_yield std::move(out);
      }
    }
  }

  friend auto inspect(auto& f, get_attributes_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<get_attributes_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"get-attributes"};
    parser.parse(p);
    return std::make_unique<get_attributes_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::get_attributes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::get_attributes::plugin)
