//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::tune {

namespace {

struct tune_args {
  std::optional<located<duration>> idle_after = {};
  std::optional<located<uint64_t>> min_demand_elements = {};
  std::optional<located<uint64_t>> max_demand_elements = {};
  std::optional<located<uint64_t>> max_demand_batches = {};
  std::optional<located<duration>> min_backoff = {};
  std::optional<located<duration>> max_backoff = {};
  std::optional<located<double>> backoff_rate = {};
  std::optional<located<bool>> detached = {};

  friend auto inspect(auto& f, tune_args& x) {
    return f.object(x).fields(
      f.field("idle_after", x.idle_after),
      f.field("min_demand_elements", x.min_demand_elements),
      f.field("max_demand_elements", x.max_demand_elements),
      f.field("max_demand_batches", x.max_demand_batches),
      f.field("min_backoff", x.min_backoff),
      f.field("max_backoff", x.max_backoff),
      f.field("backoff_rate", x.backoff_rate), f.field("detached", x.detached));
  }
};

class tune_operator final : public operator_base {
public:
  tune_operator() = default;

  explicit tune_operator(operator_ptr op, tune_args args)
    : op_{std::move(op)}, args_{std::move(args)} {
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    return op_->optimize(filter, order);
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    return op_->instantiate(std::move(input), ctrl);
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<tune_operator>(op_->copy(), args_);
  };

  auto location() const -> operator_location override {
    return op_->location();
  }

  auto detached() const -> bool override {
    if (args_.detached) {
      return args_.detached->inner;
    }
    return op_->detached();
  }

  auto internal() const -> bool override {
    return op_->internal();
  }

  auto idle_after() const -> duration override {
    if (args_.idle_after) {
      return args_.idle_after->inner;
    }
    return op_->idle_after();
  }

  auto demand() const -> demand_settings override {
    auto result = op_->demand();
    if (args_.min_demand_elements) {
      result.min_elements = args_.min_demand_elements->inner;
    }
    if (args_.max_demand_elements) {
      result.max_elements = args_.max_demand_elements->inner;
    }
    if (args_.max_demand_batches) {
      result.max_batches = args_.max_demand_batches->inner;
    }
    if (args_.min_backoff) {
      result.min_backoff = args_.min_backoff->inner;
    }
    if (args_.max_backoff) {
      result.max_backoff = args_.max_backoff->inner;
    }
    if (args_.backoff_rate) {
      result.backoff_rate = args_.backoff_rate->inner;
    }
    return result;
  }

  auto strictness() const -> strictness_level override {
    return op_->strictness();
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    return op_->infer_type(input);
  }

  auto name() const -> std::string override {
    return "_tune";
  }

  friend auto inspect(auto& f, tune_operator& x) -> bool {
    return f.object(x).fields(f.field("op", x.op_), f.field("args", x.args_));
  }

private:
  operator_ptr op_ = {};
  tune_args args_ = {};
};

class plugin final : public virtual operator_plugin2<tune_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = tune_args{};
    // TODO: This is only optional because of a bug in the argument parser,
    // which fails for operators that have no positional arguments except for a
    // required pipeline, and have at least one named argument.
    auto pipe = std::optional<pipeline>{};
    auto parser = argument_parser2::operator_(name());
    parser.named("idle_after", args.idle_after);
    parser.named("min_demand_elements", args.min_demand_elements);
    parser.named("max_demand_elements", args.max_demand_elements);
    parser.named("max_demand_batches", args.max_demand_batches);
    parser.named("min_backoff", args.min_backoff);
    parser.named("max_backoff", args.max_backoff);
    parser.named("backoff_rate", args.backoff_rate);
    parser.named("detached", args.detached);
    parser.positional("{ … }", pipe);
    TRY(parser.parse(inv, ctx));
    auto failed = false;
    if (args.idle_after and args.idle_after->inner < duration::zero()) {
      diagnostic::error("`idle_after` must be a positive duration")
        .primary(args.idle_after->source)
        .emit(ctx);
      failed = true;
    }
    if (args.min_demand_elements and args.min_demand_elements->inner == 0) {
      diagnostic::error("`min_demand_elements` must be greater than zero")
        .primary(args.min_demand_elements->source)
        .emit(ctx);
      failed = true;
    }
    if (args.max_demand_elements and args.max_demand_elements->inner == 0) {
      diagnostic::error("`max_demand_elements` must be greater than zero")
        .primary(args.max_demand_elements->source)
        .emit(ctx);
      failed = true;
    }
    if (args.min_demand_elements and args.max_demand_elements
        and args.min_demand_elements->inner > args.max_demand_elements->inner) {
      diagnostic::error("`max_demand_elements` must be greater or equal than "
                        "`min_demand_elements`")
        .primary(args.max_demand_elements->source)
        .primary(args.min_demand_elements->source)
        .emit(ctx);
      failed = true;
    }
    if (args.max_demand_batches and args.max_demand_batches->inner == 0) {
      diagnostic::error("`max_demand_batches` must be greater than zero")
        .primary(args.max_demand_batches->source)
        .emit(ctx);
      failed = true;
    }
    if (args.min_backoff
        and args.min_backoff->inner < std::chrono::milliseconds{10}) {
      diagnostic::error("`min_backoff` must be greater than or equal to 10ms")
        .primary(args.min_backoff->source)
        .emit(ctx);
      failed = true;
    }
    if (args.max_backoff
        and args.max_backoff->inner <= std::chrono::milliseconds{10}) {
      diagnostic::error("`max_backoff` must be greater than or equal to 10ms")
        .primary(args.max_backoff->source)
        .emit(ctx);
      failed = true;
    }
    if (args.min_backoff and args.max_backoff
        and args.min_backoff->inner > args.max_backoff->inner) {
      diagnostic::error(
        "`max_backoff` must be greater or equal than `min_backoff`")
        .primary(args.max_backoff->source)
        .primary(args.min_backoff->source)
        .emit(ctx);
      failed = true;
    }
    if (args.backoff_rate and args.backoff_rate->inner < 1.0) {
      diagnostic::error("`backoff_rate` must be greater than or equal to 1.0")
        .primary(args.backoff_rate->source)
        .emit(ctx);
      failed = true;
    }
    if (args.backoff_rate and args.backoff_rate->inner == 1.0
        and args.max_backoff) {
      diagnostic::warning("`backoff_rate` is equal to 1.0, which causes "
                          "`max_backoff` to be ignored")
        .primary(args.backoff_rate->source)
        .secondary(args.max_backoff->source)
        .emit(ctx);
    }
    if (not pipe) {
      diagnostic::error("missing pipeline argument").primary(inv.self).emit(ctx);
      failed = true;
    }
    if (failed) {
      return failure::promise();
    }
    auto ops = std::move(*pipe).unwrap();
    for (auto& op : ops) {
      op = std::make_unique<tune_operator>(std::move(op), args);
    }
    return std::make_unique<pipeline>(std::move(ops));
  }
};

} // namespace

} // namespace tenzir::plugins::tune

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tune::plugin)
