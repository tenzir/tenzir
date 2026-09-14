//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/box.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/error.hpp>
#include <tenzir/format_utils.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>

namespace tenzir::plugins::top_rare {

namespace {

enum class mode {
  top,
  rare,
};

struct TopRareArgs {
  location keyword;
  /// The field to count occurrences of. Kept as an expression because the
  /// operator compiles into `summarize` and `sort`, which happens before
  /// substitution.
  ast::expression x;
};

template <mode Mode>
auto describe_top_rare() -> Description {
  auto d = Describer<TopRareArgs>{};
  d.name(Mode == mode::top ? "top" : "rare");
  d.operator_location(&TopRareArgs::keyword);
  d.positional("x", &TopRareArgs::x, "field");
  return d.only_arguments();
}

auto make_replacement_ir(mode which, ast::field_path selector, location self,
                         compile_ctx ctx) -> failure_or<ir::pipeline> {
  auto provider = session_provider::make(ctx);
  const auto* summarize = plugins::find<operator_compiler_plugin>("summarize");
  const auto* sort = plugins::find<operator_compiler_plugin>("sort");
  TENZIR_ASSERT(summarize);
  TENZIR_ASSERT(sort);
  auto count_call = ast::function_call{
    ast::entity{{ast::identifier{"count", self}}}, {}, self, false};
  auto count_field = ast::field_path::try_from(
    ast::root_field{ast::identifier{"count", self}});
  TENZIR_ASSERT(count_field);
  auto summarize_arg
    = ast::assignment{count_field->inner(), self, std::move(count_call)};
  TENZIR_ASSERT(resolve_entities(summarize_arg.right, provider.as_session()));
  auto summarize_inv = invocation_for_plugin(*summarize, self);
  summarize_inv.args.push_back(std::move(selector).unwrap());
  summarize_inv.args.push_back(std::move(summarize_arg));
  TRY(auto summarize_ir, summarize->compile(std::move(summarize_inv), ctx));
  auto sort_arg = [&]() -> ast::expression {
    if (which == mode::top) {
      return ast::unary_expr{{ast::unary_op::neg, self},
                             std::move(count_field).value().unwrap()};
    }
    TENZIR_ASSERT(which == mode::rare);
    return std::move(count_field).value().unwrap();
  }();
  auto sort_inv = invocation_for_plugin(*sort, self);
  sort_inv.args.push_back(std::move(sort_arg));
  TRY(auto sort_ir, sort->compile(std::move(sort_inv), ctx));
  auto operators = std::move(summarize_ir).unwrap().operators;
  for (auto& op : std::move(sort_ir).unwrap().operators) {
    operators.push_back(std::move(op));
  }
  return ir::pipeline{{}, std::move(operators)};
}

template <mode Mode>
class top_rare_plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return Mode == mode::top ? "top" : "rare";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    using Arguments = OperatorArguments<TopRareArgs, describe_top_rare<Mode>>;
    TRY(auto parsed, Arguments::parse(std::move(inv), ctx));
    auto args = parsed.get();
    auto loc = args.x.get_location();
    auto selector = ast::field_path::try_from(std::move(args.x));
    if (not selector) {
      diagnostic::error("expected a field path").primary(loc).emit(ctx);
      return failure::promise();
    }
    TRY(auto replacement,
        make_replacement_ir(Mode, std::move(*selector), args.keyword, ctx));
    return replacement;
  }
};

using top_plugin = top_rare_plugin<mode::top>;
using rare_plugin = top_rare_plugin<mode::rare>;

} // namespace

} // namespace tenzir::plugins::top_rare

TENZIR_REGISTER_PLUGIN(tenzir::plugins::top_rare::top_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::top_rare::rare_plugin)
