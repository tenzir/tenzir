//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/view3.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::discard {

namespace {

class discard_operator final : public crtp_operator<discard_operator> {
public:
  discard_operator() = default;

  auto name() const -> std::string override {
    return "discard";
  }

  template <operator_input_batch Batch>
  auto operator()(generator<Batch> input) const -> generator<std::monostate> {
    for (auto&& slice : input) {
      (void)slice;
      co_yield {};
    }
  }

  auto internal() const -> bool override {
    return true;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, discard_operator& x) -> bool {
    return f.object(x).fields();
  }
};

template <class Input>
class Discard final : public Operator<Input, void> {
public:
  auto start(OpCtx& ctx) -> Task<void> override {
    write_bytes_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "discard",
      },
      MetricsDirection::write, MetricsVisibility::internal_);
    co_return;
  }

  auto process(Input input, OpCtx&) -> Task<void> override {
    auto bytes = std::size_t{};
    if constexpr (std::same_as<Input, table_slice>) {
      bytes = input.approx_bytes();
    } else {
      if (input) {
        bytes = input->size();
      }
    }
    write_bytes_counter_.add(static_cast<uint64_t>(bytes));
    co_return;
  }

private:
  MetricsCounter write_bytes_counter_;
};

class discard_ir final : public ir::Operator {
public:
  discard_ir() = default;

  auto name() const -> std::string override {
    return "discard_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(ctx, instantiate);
    return {};
  }

  auto spawn(element_type_tag input) and -> AnyOperator override {
    return input.match([]<class T>(tag<T>) -> AnyOperator {
      if constexpr (std::is_void_v<T>) {
        TENZIR_UNREACHABLE();
      } else {
        return Discard<T>{};
      }
    });
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is<void>()) {
      diagnostic::error("`discard` cannot be used as a source")
        .primary(main_location())
        .emit(dh);
      return failure::promise();
    }
    return tag_v<void>;
  }

  friend auto inspect(auto& f, discard_ir& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<discard_operator>,
                     public virtual operator_factory_plugin,
                     public virtual operator_compiler_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.sink = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"discard", "https://docs.tenzir.com/"
                                             "operators/discard"};
    parser.parse(p);
    return std::make_unique<discard_operator>();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("discard").parse(inv, ctx).ignore();
    return std::make_unique<discard_operator>();
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    // TODO
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(inv.args.empty());
    return discard_ir{};
  }
};

} // namespace

} // namespace tenzir::plugins::discard

TENZIR_REGISTER_PLUGIN(tenzir::plugins::discard::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::discard::discard_ir>);
