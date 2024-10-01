//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/cast.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <algorithm>

namespace tenzir::plugins::cast {

namespace {

class cast_operator final : public crtp_operator<cast_operator> {
public:
  cast_operator() = default;

  cast_operator(tenzir::location op, type ty) : op_{op}, ty_{std::move(ty)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto can = can_cast(slice.schema(), ty_);
      if (not can) {
        diagnostic::warning("could not cast: {}", can.error())
          .primary(op_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      co_yield tenzir::cast(slice, ty_);
    }
  }

  auto name() const -> std::string override {
    return "cast";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, cast_operator& x) -> bool {
    return f.object(x).fields(f.field("op", x.op_), f.field("ty", x.ty_));
  }

private:
  tenzir::location op_;
  type ty_;
};

class plugin final : public virtual operator_plugin2<cast_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto schema = located<std::string>{};
    TRY(argument_parser2::operator_(name())
          .add(schema, "<schema>")
          .parse(inv, ctx));
    auto& schemas = modules::schemas();
    auto it = std::ranges::find(schemas, schema.inner, [](auto& type) {
      return type.name();
    });
    if (it == schemas.end()) {
      diagnostic::error("schema `{}` was not found", schema.inner)
        .primary(schema)
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<cast_operator>(inv.self.get_location(), *it);
  }
};

} // namespace

} // namespace tenzir::plugins::cast

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cast::plugin)
