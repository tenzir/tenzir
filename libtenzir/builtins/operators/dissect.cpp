//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>

#include <string>

namespace tenzir::plugins::dissect {

namespace {

struct operator_args {
  std::string field;
  std::string expr;
};

auto inspect(auto& f, operator_args& x) -> bool {
  return f.object(x)
    .pretty_name("tenzir.plugins.dissect.operator_args")
    .fields(f.field("field", x.field), f.field("expr", x.expr));
}

class dissect_operator final : public crtp_operator<dissect_operator> {
public:
  dissect_operator() = default;

  explicit dissect_operator(operator_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "dissect";
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      // TODO: Apply function on every field.
      co_yield slice;
    }
  }

  // TODO: remove after deprecation.
  auto to_string() const -> std::string override {
    return {};
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

private:
  friend auto inspect(auto& f, dissect_operator& x) -> bool {
    return f.apply(x.args_);
  }

  operator_args args_;
};

class plugin final : public virtual operator_plugin<dissect_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{name(), "https://docs.tenzir.com/"
                                          "operators/transformations/dissect"};
    auto args = operator_args{};
    parser.add(args.field, "<field>");
    parser.add(args.expr, "<expr>");
    parser.parse(p);
    return std::make_unique<dissect_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::dissect

TENZIR_REGISTER_PLUGIN(tenzir::plugins::dissect::plugin)
