//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::strict {

namespace {

class strict_operator final : public operator_base {
public:
  strict_operator() = default;

  strict_operator(operator_ptr op) : op_{std::move(op)} {
    if (auto* op = dynamic_cast<strict_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    TENZIR_ASSERT(not dynamic_cast<const strict_operator*>(op_.get()));
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    auto result = op_->optimize(filter, order);
    if (not result.replacement) {
      return result;
    }
    if (auto* pipe = dynamic_cast<pipeline*>(result.replacement.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<strict_operator>(std::move(result.replacement));
      }
      result.replacement = std::make_unique<pipeline>(std::move(ops));
      return result;
    }
    result.replacement
      = std::make_unique<strict_operator>(std::move(result.replacement));
    return result;
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    return op_->instantiate(std::move(input), ctrl);
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<strict_operator>(op_->copy());
  };

  auto location() const -> operator_location override {
    return op_->location();
  }

  auto detached() const -> bool override {
    return op_->detached();
  }

  auto internal() const -> bool override {
    return op_->internal();
  }

  auto idle_after() const -> duration override {
    return op_->idle_after();
  }

  auto demand() const -> demand_settings override {
    return op_->demand();
  }

  auto strictness() const -> strictness_level override {
    return strictness_level::strict;
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    return op_->infer_type(input);
  }

  auto name() const -> std::string override {
    return "strict";
  }

  friend auto inspect(auto& f, strict_operator& x) -> bool {
    return f.apply(x.op_);
  }

private:
  operator_ptr op_;
};

struct strict : public virtual operator_plugin2<strict_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipe = pipeline{};
    auto parser = argument_parser2::operator_(name()).positional("{ … }", pipe);
    TRY(parser.parse(inv, ctx));
    auto ops = std::move(pipe).unwrap();
    for (auto& op : ops) {
      op = std::make_unique<strict_operator>(std::move(op));
    }
    return std::make_unique<pipeline>(std::move(ops));
  }
};

} // namespace

} // namespace tenzir::plugins::strict

TENZIR_REGISTER_PLUGIN(tenzir::plugins::strict::strict)
