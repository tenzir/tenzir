//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
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

// --- New-executor implementation ---

struct StrictArgs {
  located<ir::pipeline> pipe;
};

// Primary template: non-void input, non-void output.
template <class Input, class Output>
class StrictOp final : public Operator<Input, Output> {
public:
  explicit StrictOp(StrictArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await ctx.spawn_sub<Input>(int64_t{0}, args_.pipe.inner,
                                  DiagnosticBehavior::WarningToError);
  }

  auto process(Input input, Push<Output>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    std::ignore = co_await as<SubHandle<Input>>(*sub).push(std::move(input));
  }

  auto process_sub(SubKeyView key, chunk_ptr chunk, Push<Output>& push,
                   OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, ctx);
    if constexpr (std::same_as<Output, chunk_ptr>) {
      co_await push(std::move(chunk));
    } else {
      TENZIR_UNREACHABLE();
    }
  }

private:
  StrictArgs args_;
};

// Partial specialisation: void output, non-void input.
template <class Input>
class StrictOp<Input, void> final : public Operator<Input, void> {
public:
  explicit StrictOp(StrictArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await ctx.spawn_sub<Input>(int64_t{0}, args_.pipe.inner,
                                  DiagnosticBehavior::WarningToError);
  }

  auto process(Input input, OpCtx& ctx) -> Task<void> override {
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    std::ignore = co_await as<SubHandle<Input>>(*sub).push(std::move(input));
  }

private:
  StrictArgs args_;
};

// Partial specialisation: void input, non-void output.
template <class Output>
class StrictOp<void, Output> final : public Operator<void, Output> {
public:
  explicit StrictOp(StrictArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await ctx.spawn_sub<void>(int64_t{0}, args_.pipe.inner,
                                 DiagnosticBehavior::WarningToError);
  }

  auto process_sub(SubKeyView key, chunk_ptr chunk, Push<Output>& push,
                   OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, ctx);
    if constexpr (std::same_as<Output, chunk_ptr>) {
      co_await push(std::move(chunk));
    } else {
      TENZIR_UNREACHABLE();
    }
  }

private:
  StrictArgs args_;
};

// Full specialisation: void input, void output.
template <>
class StrictOp<void, void> final : public Operator<void, void> {
public:
  explicit StrictOp(StrictArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await ctx.spawn_sub<void>(int64_t{0}, args_.pipe.inner,
                                 DiagnosticBehavior::WarningToError);
  }

private:
  StrictArgs args_;
};

// --- Plugin ---

struct strict : public virtual operator_plugin2<strict_operator>,
                public virtual OperatorPlugin {
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipe = located<pipeline>{};
    auto parser = argument_parser2::operator_(name()).positional("{ … }", pipe);
    TRY(parser.parse(inv, ctx));
    auto ops = std::move(pipe.inner).unwrap();
    for (auto& op : ops) {
      op = std::make_unique<strict_operator>(std::move(op));
    }
    return std::make_unique<pipeline>(std::move(ops));
  }

  auto describe() const -> Description override {
    auto d = Describer<StrictArgs>{};
    auto pipe = d.pipeline(&StrictArgs::pipe);
    d.spawner([pipe]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<StrictArgs, Input>>> {
      TRY(auto p, ctx.get(pipe));
      TRY(auto output, p.inner.infer_type(tag_v<Input>, ctx));
      if (not output) {
        return {};
      }
      return match(
        *output,
        [](tag<table_slice>)
          -> failure_or<Option<SpawnWith<StrictArgs, Input>>> {
          return [](StrictArgs args) {
            return StrictOp<Input, table_slice>{std::move(args)};
          };
        },
        [](tag<chunk_ptr>) -> failure_or<Option<SpawnWith<StrictArgs, Input>>> {
          return [](StrictArgs args) {
            return StrictOp<Input, chunk_ptr>{std::move(args)};
          };
        },
        [](tag<void>) -> failure_or<Option<SpawnWith<StrictArgs, Input>>> {
          return [](StrictArgs args) {
            return StrictOp<Input, void>{std::move(args)};
          };
        });
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::strict

TENZIR_REGISTER_PLUGIN(tenzir::plugins::strict::strict)
