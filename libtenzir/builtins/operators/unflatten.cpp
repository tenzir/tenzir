//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::unflatten {

namespace {

constexpr auto default_unflatten_separator = ".";

class unflatten_operator final : public crtp_operator<unflatten_operator> {
public:
  unflatten_operator() = default;

  unflatten_operator(std::string separator) : separator_{std::move(separator)} {
  }

  auto
  operator()(generator<table_slice> input, [[maybe_unused]] exec_ctx ctx) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      auto result = tenzir::unflatten(slice, separator_);
      co_yield std::move(result);
    }
  }

  auto name() const -> std::string override {
    return "unflatten";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, unflatten_operator& x) -> bool {
    return f.apply(x.separator_);
  }

private:
  std::string separator_ = default_unflatten_separator;
};

class plugin final : public virtual operator_plugin<unflatten_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"unflatten", "https://docs.tenzir.com/"
                                               "operators/unflatten"};
    auto sep = std::optional<located<std::string>>{};
    parser.add(sep, "<separator>");
    parser.parse(p);
    auto separator = (sep) ? sep->inner : default_unflatten_separator;
    return std::make_unique<unflatten_operator>(separator);
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto sep = std::optional<located<std::string>>{};
    argument_parser2::operator_(name())
      .add(sep, "<separator>")
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<unflatten_operator>(
      sep ? std::move(sep->inner) : default_unflatten_separator);
  }
};

} // namespace

} // namespace tenzir::plugins::unflatten

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unflatten::plugin)
