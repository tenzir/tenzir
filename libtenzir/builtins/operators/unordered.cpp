//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::unordered {

namespace {

class unordered_operator final : public operator_base {
public:
  unordered_operator() = default;

  explicit unordered_operator(operator_ptr op) : op_{std::move(op)} {
    if (auto* op = dynamic_cast<unordered_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    TENZIR_ASSERT(not dynamic_cast<const unordered_operator*>(op_.get()));
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    return op_->optimize(filter, event_order::unordered);
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    return op_->instantiate(std::move(input), ctrl);
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<unordered_operator>(op_->copy());
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

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    return op_->infer_type(input);
  }

  auto name() const -> std::string override {
    return "unordered";
  }

  friend auto inspect(auto& f, unordered_operator& x) -> bool {
    return f.object(x).fields(f.field("op", x.op_));
  }

private:
  operator_ptr op_;
};

class plugin final : public virtual operator_plugin<unordered_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto result = p.parse_operator();
    if (not result.inner) {
      diagnostic::error("failed to parse operator")
        .primary(result.source)
        .throw_();
    }
    return std::make_unique<unordered_operator>(std::move(result.inner));
  }
};

} // namespace

} // namespace tenzir::plugins::unordered

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unordered::plugin)
