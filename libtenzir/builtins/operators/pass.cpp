//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::pass {

namespace {

struct PassArgs {
  // No arguments needed for pass operator
};

class PassTableSlice final : public Operator<table_slice, table_slice> {
public:
  explicit PassTableSlice(PassArgs /*args*/) {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

class PassChunk final : public Operator<chunk_ptr, chunk_ptr> {
public:
  explicit PassChunk(PassArgs /*args*/) {
  }

  auto process(chunk_ptr input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

// Does nothing with the input.
class pass_operator final : public crtp_operator<pass_operator> {
public:
  template <operator_input_batch T>
  auto operator()(T x) const -> T {
    return x;
  }

  auto name() const -> std::string override {
    return "pass";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, nullptr};
  }

  friend auto inspect(auto& f, pass_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<pass_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    argument_parser{"pass", "https://docs.tenzir.com/operators/pass"}.parse(p);
    return std::make_unique<pass_operator>();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("pass").parse(inv, ctx).ignore();
    return std::make_unique<pass_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<PassArgs, PassTableSlice, PassChunk>{};
    return d.optimize([](DescribeCtx&, event_order order) -> Optimization {
      return {.order = order, .drop = true};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::pass

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pass::plugin)
