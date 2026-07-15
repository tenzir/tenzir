//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/panic.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::unordered {

namespace {

class UnorderedIr final : public ir::Operator {
public:
  UnorderedIr() = default;

  explicit UnorderedIr(ir::pipeline pipeline, location loc)
    : pipeline_{std::move(pipeline)}, loc_{loc} {
  }

  auto name() const -> std::string override {
    return "UnorderedIr";
  }

  auto main_location() const -> location override {
    return loc_;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return pipeline_.substitute(ctx, instantiate);
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    return pipeline_.infer_type(input, dh);
  }

  auto optimize(ir::optimize_filter filter,
                event_order /* order */) && -> ir::optimize_result override {
    // Optimize each sub-operator individually, always passing unordered.
    auto replacement = ir::pipeline{std::move(pipeline_.lets), {}};
    for (auto& op : std::ranges::reverse_view(pipeline_.operators)) {
      auto opt
        = std::move(*op).optimize(std::move(filter), event_order::unordered);
      filter = std::move(opt.filter);
      replacement.operators.insert(
        replacement.operators.begin(),
        std::move_iterator{opt.replacement.operators.begin()},
        std::move_iterator{opt.replacement.operators.end()});
    }
    return {
      std::move(filter),
      event_order::unordered,
      replacement,
    };
  }

  auto spawn(element_type_tag) && -> Option<AnyOperator> override {
    panic("Cannot spawn unordered. It should have been optimized away.");
  }

  friend auto inspect(auto& f, UnorderedIr& x) -> bool {
    return f.object(x).fields(f.field("pipeline", x.pipeline_),
                              f.field("loc", x.loc_));
  }

private:
  ir::pipeline pipeline_;
  location loc_;
};

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

  auto demand() const -> demand_settings override {
    return op_->demand();
  }

  auto strictness() const -> strictness_level override {
    return op_->strictness();
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

class plugin final : public virtual operator_plugin<unordered_operator>,
                     public virtual operator_factory_plugin,
                     public virtual operator_compiler_plugin {
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

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipe = located<pipeline>{};
    auto parser = argument_parser2::operator_(name()).positional("{ … }", pipe);
    TRY(parser.parse(inv, ctx));
    auto ops = std::move(pipe.inner).unwrap();
    for (auto& op : ops) {
      op = std::make_unique<unordered_operator>(std::move(op));
    }
    return std::make_unique<pipeline>(std::move(ops));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto loc = inv.op.get_location();
    if (inv.args.size() != 1) {
      diagnostic::error("`unordered` expects a single pipeline argument")
        .primary(loc)
        .emit(ctx);
      return failure::promise();
    }
    auto* pipe_expr = try_as<ast::pipeline_expr>(inv.args[0]);
    if (not pipe_expr) {
      diagnostic::error("`unordered` expects a pipeline argument `{{ … }}`")
        .primary(inv.args[0])
        .emit(ctx);
      return failure::promise();
    }
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    return UnorderedIr{std::move(pipe_ir), loc};
  }
};

} // namespace

} // namespace tenzir::plugins::unordered

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unordered::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::unordered::UnorderedIr>)
