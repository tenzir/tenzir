//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_expr_props.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/nova/secret.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

auto parse(std::string_view source,
           registry const& reg = test::builtin_registry()) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, reg);
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

auto parse_unresolved(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  return std::move(*expr);
}

auto make_secret_constant(std::string_view value = "top-secret")
  -> ast::expression {
  return ast::constant{secret::make_literal(value), location::unknown};
}

/// Replaces every root field named `s` with a legacy `tenzir::secret` constant.
auto parse_with_secret(std::string_view source) -> ast::expression {
  auto expr = parse(source);
  auto replaced = ast::substitute_refs(
    expr, test::builtin_registry(),
    [](ast::field_path const& field) -> Option<ast::expression> {
      if (field.path().size() == 1 and field.path().front().id.name == "s") {
        return make_secret_constant();
      }
      return field.inner();
    });
  REQUIRE(replaced);
  return std::move(*replaced);
}

struct UnaryValueArgs {
  nova::ValueArgument x;
};

struct IdentityImpl {
  static auto eval(UnaryValueArgs const& args, nova::EvalFrame)
    -> nova::Array<nova::Data> {
    return args.x.data;
  }
};

/// Synthetic function plugin whose metadata tiers are configured explicitly.
class ConfiguredPlugin final : public virtual nova::FunctionPlugin {
public:
  ConfiguredPlugin(std::string name, Volatility vol, Fallibility fall,
                   Cost cost)
    : name_{std::move(name)}, vol_{vol}, fall_{fall}, cost_{cost} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return vol_ == Volatility::immutable;
  }

  auto volatility() const -> Volatility override {
    return vol_;
  }

  auto fallibility() const -> Fallibility override {
    return fall_;
  }

  auto cost() const -> Cost override {
    return cost_;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<UnaryValueArgs, IdentityImpl>{};
    d.positional("x", &UnaryValueArgs::x);
    return std::move(d).finish();
  }

  auto make_function(function_invocation, session) const
    -> failure_or<function_ptr> override {
    panic("legacy path unused in test");
  }

private:
  std::string name_;
  Volatility vol_;
  Fallibility fall_;
  Cost cost_;
};

/// Legacy-style function plugin that overrides only `is_deterministic()` and
/// `is_cheap()` to verify backward-compatible defaults.
class LegacyDefaultsPlugin final : public virtual function_plugin {
public:
  LegacyDefaultsPlugin(std::string name, bool deterministic, bool cheap)
    : name_{std::move(name)}, deterministic_{deterministic}, cheap_{cheap} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return deterministic_;
  }

  auto is_cheap() const -> bool override {
    return cheap_;
  }

  auto make_function(function_invocation, session) const
    -> failure_or<function_ptr> override {
    panic("legacy path unused in test");
  }

private:
  std::string name_;
  bool deterministic_;
  bool cheap_;
};

struct RewritableArgs {
  nova::ValueArgument x;
  int64_t opt = 0;
};

struct RewritableImpl {
  static auto eval(RewritableArgs const& args, nova::EvalFrame)
    -> nova::Array<nova::Data> {
    return args.x.data;
  }
};

/// Synthetic function plugin with field-rewrite hooks that record invocations.
class RewritablePlugin final : public virtual nova::FunctionPlugin {
public:
  explicit RewritablePlugin(std::string name) : name_{std::move(name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<RewritableArgs, RewritableImpl>{};
    d.positional("x", &RewritableArgs::x);
    d.named_optional("opt", &RewritableArgs::opt);
    return std::move(d).finish();
  }

  auto rewrite_field_access(ast::function_call const& call,
                            std::span<ast::field_path::segment const> path,
                            nova::FieldRewriteCtx) const
    -> Option<std::vector<nova::FieldCase>> override {
    ++access_calls;
    if (call.args.empty() or path.size() != 1 or path[0].id.name != "a") {
      return None{};
    }
    return std::vector<nova::FieldCase>{{
      .guard = ast::constant{true, location::unknown},
      .value = call.args.front(),
    }};
  }

  auto rewrite_field_comparison(ast::function_call const& call,
                                std::span<ast::field_path::segment const> path,
                                ast::binary_op op, ast::expression const& rhs,
                                nova::FieldRewriteCtx) const
    -> Option<ast::expression> override {
    ++comparison_calls;
    if (call.args.empty() or path.size() != 1 or path[0].id.name != "a") {
      return None{};
    }
    return ast::expression{ast::binary_expr{call.args.front(), op, rhs}};
  }

  auto make_function(function_invocation, session) const
    -> failure_or<function_ptr> override {
    panic("legacy path unused in test");
  }

  mutable size_t access_calls = 0;
  mutable size_t comparison_calls = 0;

private:
  std::string name_;
};

auto try_instantiate(ast::expression const& expr, registry const& reg) -> bool {
  auto dh = collecting_diagnostic_handler{};
  auto evaluator = nova::Evaluator::make(expr, nova::InstantiateCtx{dh, reg});
  return evaluator and dh.empty();
}

auto instantiate_pipeline_error(std::string_view source) -> std::string {
  auto dh = collecting_diagnostic_handler{};
  auto const& reg = test::builtin_registry();
  auto provider = session_provider::make(dh, reg);
  auto ctx = base_ctx{dh, reg};
  auto ast = parse_pipeline_with_location_override(source, location::unknown,
                                                   provider.as_session());
  REQUIRE(ast);
  REQUIRE(resolve_entities(*ast, ExternPrelude::empty(), ctx));
  auto compiled = std::move(*ast).compile(compile_ctx::make_root(ctx));
  if (compiled) {
    std::ignore = ir::instantiate(std::move(*compiled), ctx);
  }
  auto diags = std::move(dh).collect();
  for (auto const& diag : diags) {
    if (diag.severity == severity::error) {
      return diag.message;
    }
  }
  return {};
}

} // namespace

TEST("ExprProps monoid and lattice join laws") {
  auto const id = ir::ExprProps{};
  auto const a = ir::ExprProps{
    .volatility = Volatility::stable,
    .fallibility = Fallibility::warns,
    .cost = Cost::cheap,
    .expensive_ops = 0,
    .node_count = 3,
    .has_move = true,
    .has_unbound_var = false,
    .has_bare_assignment = false,
    .has_non_value = false,
    .has_function_call = true,
    .has_secret = false,
    .reads_event = true,
    .evaluable = true,
    .pure_constant = false,
  };
  auto const b = ir::ExprProps{
    .volatility = Volatility::volatile_,
    .fallibility = Fallibility::fails,
    .cost = Cost::expensive,
    .expensive_ops = 2,
    .node_count = 5,
    .has_move = false,
    .has_unbound_var = true,
    .has_bare_assignment = true,
    .has_non_value = true,
    .has_function_call = false,
    .has_secret = true,
    .reads_event = false,
    .evaluable = false,
    .pure_constant = true,
  };
  auto const c = ir::ExprProps{
    .volatility = Volatility::immutable,
    .fallibility = Fallibility::total,
    .cost = Cost::plain,
    .expensive_ops = 1,
    .node_count = 7,
    .has_move = false,
    .has_unbound_var = false,
    .has_bare_assignment = false,
    .has_non_value = false,
    .has_function_call = true,
    .has_secret = false,
    .reads_event = true,
    .evaluable = true,
    .pure_constant = false,
  };
  // Left and right identity.
  CHECK_EQUAL(id | a, a);
  CHECK_EQUAL(a | id, a);
  CHECK_EQUAL(id | b, b);
  CHECK_EQUAL(b | id, b);
  // Commutativity.
  CHECK_EQUAL(a | b, b | a);
  CHECK_EQUAL(a | c, c | a);
  CHECK_EQUAL(b | c, c | b);
  // Associativity.
  CHECK_EQUAL((a | b) | c, a | (b | c));
  // In-place join matches binary join.
  auto acc = a;
  acc |= b;
  CHECK_EQUAL(acc, a | b);
  // Idempotence on qualitative fields; counters add.
  auto const aa = a | a;
  CHECK_EQUAL(aa.volatility, a.volatility);
  CHECK_EQUAL(aa.fallibility, a.fallibility);
  CHECK_EQUAL(aa.cost, a.cost);
  CHECK_EQUAL(aa.has_move, a.has_move);
  CHECK_EQUAL(aa.has_function_call, a.has_function_call);
  CHECK_EQUAL(aa.reads_event, a.reads_event);
  CHECK_EQUAL(aa.evaluable, a.evaluable);
  CHECK_EQUAL(aa.pure_constant, a.pure_constant);
  CHECK_EQUAL(aa.node_count, uint32_t{6});
  CHECK_EQUAL(aa.expensive_ops, uint32_t{0});
  // Saturating arithmetic at UINT32_MAX.
  auto const max_u32 = std::numeric_limits<uint32_t>::max();
  auto const near_max = ir::ExprProps{
    .expensive_ops = max_u32 - 1,
    .node_count = max_u32 - 1,
  };
  auto const joined_max = near_max | b;
  CHECK_EQUAL(joined_max.expensive_ops, max_u32);
  CHECK_EQUAL(joined_max.node_count, max_u32);
}

TEST("function_plugin backward-compatible defaults and explicit overrides") {
  // Backward-compatible defaults on legacy plugins.
  auto const det_cheap = LegacyDefaultsPlugin{"det_cheap", true, true};
  CHECK_EQUAL(det_cheap.volatility(), Volatility::immutable);
  CHECK_EQUAL(det_cheap.fallibility(), Fallibility::fails);
  CHECK_EQUAL(det_cheap.cost(), Cost::cheap);
  auto const det_exp = LegacyDefaultsPlugin{"det_exp", true, false};
  CHECK_EQUAL(det_exp.volatility(), Volatility::immutable);
  CHECK_EQUAL(det_exp.fallibility(), Fallibility::fails);
  CHECK_EQUAL(det_exp.cost(), Cost::expensive);
  auto const nondet_exp = LegacyDefaultsPlugin{"nondet_exp", false, false};
  CHECK_EQUAL(nondet_exp.volatility(), Volatility::volatile_);
  CHECK_EQUAL(nondet_exp.fallibility(), Fallibility::fails);
  CHECK_EQUAL(nondet_exp.cost(), Cost::expensive);
  // Built-in volatility overrides and defaults.
  auto const& reg = test::builtin_registry();
  auto const get_plugin = [&](std::string_view name) -> function_plugin const& {
    auto const ref = reg.get(entity_path{
      std::string{entity_pkg_std}, {std::string{name}}, entity_ns::fn});
    return as<std::reference_wrapper<function_plugin const>>(ref).get();
  };
  CHECK_EQUAL(get_plugin("now").volatility(), Volatility::stable);
  CHECK_EQUAL(get_plugin("file_contents").volatility(), Volatility::stable);
  CHECK_EQUAL(get_plugin("secret").volatility(), Volatility::stable);
  CHECK_EQUAL(get_plugin("random").volatility(), Volatility::volatile_);
  CHECK_EQUAL(get_plugin("uuid").volatility(), Volatility::volatile_);
  CHECK_EQUAL(get_plugin("encrypt_hpke").volatility(), Volatility::volatile_);
  CHECK_EQUAL(get_plugin("abs").volatility(), Volatility::immutable);
  CHECK_EQUAL(get_plugin("abs").fallibility(), Fallibility::fails);
  CHECK_EQUAL(get_plugin("abs").cost(), Cost::cheap);
  CHECK_EQUAL(get_plugin("parse_json").volatility(), Volatility::immutable);
  CHECK_EQUAL(get_plugin("parse_json").fallibility(), Fallibility::fails);
  CHECK_EQUAL(get_plugin("parse_json").cost(), Cost::expensive);
}

TEST("call preparation is kept separate from per-row function fallibility") {
  auto total_fn = ConfiguredPlugin{"_test_total", Volatility::immutable,
                                   Fallibility::total, Cost::cheap};
  auto warns_fn = ConfiguredPlugin{"_test_warns", Volatility::immutable,
                                   Fallibility::warns, Cost::cheap};
  auto fails_fn = ConfiguredPlugin{"_test_fails", Volatility::immutable,
                                   Fallibility::fails, Cost::cheap};
  auto reg = test::builtin_registry().clone();
  auto const std_pkg = std::string{entity_pkg_std};
  reg->add(std_pkg, total_fn.name(), std::cref(total_fn));
  reg->add(std_pkg, warns_fn.name(), std::cref(warns_fn));
  reg->add(std_pkg, fails_fn.name(), std::cref(fails_fn));
  // Valid call with total operand -> Fallibility::total, eval-infallible, but
  // unvalidated `is_tql_infallible()` remains false.
  auto const valid_total_q = parse("_test_total(x?)", *reg);
  auto const p_total_q = ir::analyze(valid_total_q, *reg);
  CHECK_EQUAL(p_total_q.fallibility, Fallibility::total);
  CHECK(p_total_q.has_function_call);
  CHECK(p_total_q.is_eval_infallible());
  CHECK(not p_total_q.is_tql_infallible());
  CHECK(try_instantiate(valid_total_q, *reg));
  auto const p_total_const = ir::analyze(parse("_test_total(1)", *reg), *reg);
  CHECK_EQUAL(p_total_const.fallibility, Fallibility::total);
  CHECK(p_total_const.is_eval_infallible());
  CHECK(not p_total_const.is_tql_infallible());
  CHECK(p_total_const.is_const_foldable());
  // Valid call with warns operand joins to Fallibility::warns.
  auto const p_total_x = ir::analyze(parse("_test_total(x)", *reg), *reg);
  CHECK_EQUAL(p_total_x.fallibility, Fallibility::warns);
  CHECK(p_total_x.is_eval_infallible());
  CHECK(not p_total_x.is_tql_infallible());
  // Valid warns plugin.
  auto const p_warns = ir::analyze(parse("_test_warns(x?)", *reg), *reg);
  CHECK_EQUAL(p_warns.fallibility, Fallibility::warns);
  CHECK(p_warns.is_eval_infallible());
  CHECK(not p_warns.is_tql_infallible());
  // Valid fails plugin is neither eval-infallible nor tql-infallible.
  auto const p_fails = ir::analyze(parse("_test_fails(x?)", *reg), *reg);
  CHECK_EQUAL(p_fails.fallibility, Fallibility::fails);
  CHECK(not p_fails.is_eval_infallible());
  CHECK(not p_fails.is_tql_infallible());
  // Malformed calls to a total plugin (wrong arity, lambda in value slot,
  // unknown named option) still fail instantiation and are never certified by
  // unvalidated `is_tql_infallible()`.
  for (auto const* source : {"_test_total(1, 2)", "_test_total(v => v)",
                             "_test_total(x?, unknown=1)"}) {
    auto const malformed = parse(source, *reg);
    auto const props = ir::analyze(malformed, *reg);
    CHECK(props.has_function_call);
    CHECK(not props.is_tql_infallible());
    CHECK(not try_instantiate(malformed, *reg));
  }
}

TEST("ExprProps covers all 21 AST expression kinds") {
  auto const& reg = test::builtin_registry();
  // 1. constant (evaluable vs. non-evaluable direct and nested).
  {
    auto const p = ir::analyze(parse("42"), reg);
    CHECK_EQUAL(p.volatility, Volatility::immutable);
    CHECK_EQUAL(p.fallibility, Fallibility::total);
    CHECK_EQUAL(p.cost, Cost::plain);
    CHECK_EQUAL(p.node_count, uint32_t{1});
    CHECK(p.evaluable);
    CHECK(not p.reads_event);
    CHECK(p.is_plain());
    CHECK(p.is_cheap());
    CHECK(p.is_duplicable());
    CHECK(p.is_tql_infallible());
    CHECK(p.is_const_foldable());
    CHECK(p.is_pure_constant());
    // Direct non-evaluable constants: secret, map, enumeration.
    auto const secret_expr = make_secret_constant();
    auto const p_sec = ir::analyze(secret_expr, reg);
    CHECK(not p_sec.evaluable);
    CHECK(p_sec.has_secret);
    CHECK_EQUAL(p_sec.fallibility, Fallibility::fails);
    CHECK(not p_sec.is_duplicable());
    CHECK(not p_sec.is_tql_infallible());
    CHECK(not p_sec.is_const_foldable());
    CHECK(p_sec.is_pure_constant());
    CHECK(secret_expr.is_deterministic(reg));
    auto const map_expr = ast::expression{ast::constant{
      map{{data{int64_t{1}}, data{int64_t{2}}}}, location::unknown}};
    auto const p_map = ir::analyze(map_expr, reg);
    CHECK(not p_map.evaluable);
    CHECK(not p_map.has_secret);
    CHECK(not p_map.is_tql_infallible());
    CHECK(p_map.is_pure_constant());
    auto const enum_expr
      = ast::expression{ast::constant{enumeration{1}, location::unknown}};
    CHECK(not ir::analyze(enum_expr, reg).evaluable);
    // Pattern nested inside record/list (`ast::constant::kind` excludes
    // top-level `pattern`).
    auto pat = pattern::make("abc");
    REQUIRE(pat);
    auto nested_pat_rec = record{{"p", data{*pat}}};
    auto const pat_rec_expr
      = ast::expression{ast::constant{nested_pat_rec, location::unknown}};
    CHECK(not ir::analyze(pat_rec_expr, reg).evaluable);
    CHECK(not ir::analyze(pat_rec_expr, reg).is_tql_infallible());
    auto nested_pat_list = list{data{*pat}};
    auto const pat_list_expr
      = ast::expression{ast::constant{nested_pat_list, location::unknown}};
    CHECK(not ir::analyze(pat_list_expr, reg).evaluable);
    // Nested legacy secret inside an AST record literal.
    auto const nested_sec = parse_with_secret("{a: s}");
    auto const p_nested_sec = ir::analyze(nested_sec, reg);
    CHECK(p_nested_sec.has_secret);
    CHECK(not p_nested_sec.evaluable);
    CHECK(not p_nested_sec.is_duplicable());
    CHECK(not p_nested_sec.is_tql_infallible());
    CHECK(nested_sec.is_deterministic(reg));
    // Single `ast::constant` holding record -> list -> secret payload
    // (directly exercises `scan_constant` nesting propagation).
    auto const nested_payload = record{
      {"a", data{list{data{secret::make_literal("nested")}}}},
    };
    auto const payload_const_expr
      = ast::expression{ast::constant{nested_payload, location::unknown}};
    auto const p_payload_sec = ir::analyze(payload_const_expr, reg);
    CHECK(p_payload_sec.has_secret);
    CHECK(not p_payload_sec.evaluable);
    CHECK_EQUAL(p_payload_sec.fallibility, Fallibility::fails);
    CHECK(not p_payload_sec.is_duplicable());
    CHECK(not p_payload_sec.is_tql_infallible());
    CHECK(not p_payload_sec.is_const_foldable());
    CHECK(p_payload_sec.is_pure_constant());
    CHECK(payload_const_expr.is_deterministic(reg));
  }
  // 2–4. this_, root_field (with and without `?`), meta.
  {
    auto const p_this = ir::analyze(parse("this"), reg);
    CHECK_EQUAL(p_this.fallibility, Fallibility::total);
    CHECK(p_this.is_plain());
    CHECK(p_this.reads_event);
    CHECK(not p_this.is_pure_constant());
    CHECK(p_this.is_tql_infallible());
    auto const p_meta = ir::analyze(parse("@name"), reg);
    CHECK_EQUAL(p_meta.fallibility, Fallibility::total);
    CHECK(p_meta.is_plain());
    CHECK(p_meta.reads_event);
    CHECK(not p_meta.is_pure_constant());
    auto const p_root = ir::analyze(parse("x"), reg);
    CHECK_EQUAL(p_root.fallibility, Fallibility::warns);
    CHECK(p_root.is_plain());
    CHECK(p_root.reads_event);
    CHECK(p_root.is_tql_infallible());
    auto const p_root_q = ir::analyze(parse("x?"), reg);
    CHECK_EQUAL(p_root_q.fallibility, Fallibility::total);
    CHECK(p_root_q.is_plain());
    CHECK(p_root_q.reads_event);
    CHECK(p_root_q.is_tql_infallible());
  }
  // 5–6. field_access (even `a.?b` warns on non-records) and index_expr.
  {
    auto const p_fa = ir::analyze(parse("a.b"), reg);
    CHECK_EQUAL(p_fa.fallibility, Fallibility::warns);
    CHECK(p_fa.is_plain());
    auto const p_fa_q = ir::analyze(parse("a?.?b"), reg);
    CHECK_EQUAL(p_fa_q.fallibility, Fallibility::warns);
    CHECK(p_fa_q.is_plain());
    auto const p_idx_const = ir::analyze(parse("x.y[\"z\"]"), reg);
    CHECK_EQUAL(p_idx_const.fallibility, Fallibility::warns);
    CHECK(p_idx_const.is_plain());
    auto const p_idx_dyn = ir::analyze(parse("x[y]"), reg);
    CHECK_EQUAL(p_idx_dyn.fallibility, Fallibility::warns);
    CHECK(not p_idx_dyn.is_plain());
    CHECK(p_idx_dyn.is_cheap());
  }
  // 7–8. binary_expr and unary_expr (including `move`).
  {
    auto const p_bin = ir::analyze(parse("x + 1"), reg);
    CHECK_EQUAL(p_bin.fallibility, Fallibility::warns);
    CHECK_EQUAL(p_bin.cost, Cost::cheap);
    CHECK_EQUAL(p_bin.node_count, uint32_t{3});
    auto const p_neg = ir::analyze(parse("-x"), reg);
    CHECK_EQUAL(p_neg.fallibility, Fallibility::warns);
    CHECK_EQUAL(p_neg.cost, Cost::cheap);
    CHECK(not p_neg.has_move);
    auto const p_move = ir::analyze(parse("move x"), reg);
    CHECK(p_move.has_move);
    CHECK(not p_move.is_duplicable());
    CHECK(p_move.is_tql_infallible());
    CHECK(parse("move x").is_deterministic(reg));
    auto const p_move_const = ir::analyze(parse("move 1"), reg);
    CHECK(p_move_const.has_move);
    CHECK(not p_move_const.is_duplicable());
    CHECK(not p_move_const.is_const_foldable());
    CHECK(p_move_const.is_pure_constant());
  }
  // 9. function_call (resolved, secret, named args, malformed LHS, unresolved).
  {
    auto const p_now = ir::analyze(parse("now()"), reg);
    CHECK_EQUAL(p_now.volatility, Volatility::stable);
    CHECK(not p_now.has_secret);
    CHECK(not p_now.is_duplicable());
    CHECK(not p_now.is_pure_constant());
    CHECK(not parse("now()").is_deterministic(reg));
    auto const p_rand = ir::analyze(parse("random()"), reg);
    CHECK_EQUAL(p_rand.volatility, Volatility::volatile_);
    CHECK(not p_rand.is_duplicable());
    CHECK(not parse("random()").is_deterministic(reg));
    auto const p_sec_call = ir::analyze(parse("secret(\"k\")"), reg);
    CHECK_EQUAL(p_sec_call.volatility, Volatility::stable);
    CHECK(p_sec_call.has_secret);
    CHECK(not p_sec_call.is_duplicable());
    CHECK(not p_sec_call.is_pure_constant());
    CHECK(not parse("secret(\"k\")").is_deterministic(reg));
    // Ordinary named argument.
    auto const named_call = parse("int(\"10\", base=16)");
    auto const p_named = ir::analyze(named_call, reg);
    CHECK_EQUAL(p_named.node_count, uint32_t{5});
    CHECK(not p_named.has_bare_assignment);
    CHECK(not p_named.reads_event);
    CHECK(p_named.is_cheap());
    CHECK(p_named.is_duplicable());
    CHECK(p_named.is_const_foldable());
    CHECK(p_named.is_pure_constant());
    CHECK(named_call.is_deterministic(reg));
    // Valid named binary lambda argument (`sort` uses `cmp`).
    auto const named_lambda = parse("sort([1, 2], cmp=(a, b) => a < b)");
    auto const p_named_lambda = ir::analyze(named_lambda, reg);
    CHECK(not p_named_lambda.has_bare_assignment);
    CHECK(not p_named_lambda.reads_event);
    CHECK(p_named_lambda.is_duplicable());
    CHECK(p_named_lambda.is_const_foldable());
    CHECK(p_named_lambda.is_pure_constant());
    CHECK(named_lambda.is_deterministic(reg));
    // Malformed named-argument LHS preserves LHS volatility/cost/size while
    // keeping `pure_constant` from RHS alone.
    auto const bad_named = parse("int(\"10\", random() = 16)");
    auto const p_bad_named = ir::analyze(bad_named, reg);
    CHECK_EQUAL(p_bad_named.volatility, Volatility::volatile_);
    CHECK_EQUAL(p_bad_named.expensive_ops, uint32_t{1});
    CHECK_EQUAL(p_bad_named.node_count, uint32_t{5});
    CHECK(not p_bad_named.is_cheap());
    CHECK(not p_bad_named.is_duplicable());
    CHECK(not bad_named.is_deterministic(reg));
    CHECK(p_bad_named.is_pure_constant());
    auto const move_named = parse("int(\"10\", (move a) = 16)");
    auto const p_move_named = ir::analyze(move_named, reg);
    CHECK(p_move_named.has_move);
    CHECK(not p_move_named.is_duplicable());
    CHECK(p_move_named.is_pure_constant());
    // Unresolved function call returns non-deterministic without panicking.
    auto const unresolved = parse_unresolved("nonexistent_fn(x)");
    auto const p_unres = ir::analyze(unresolved, reg);
    CHECK(p_unres.has_non_value);
    CHECK(p_unres.has_function_call);
    CHECK_EQUAL(p_unres.volatility, Volatility::volatile_);
    CHECK_EQUAL(p_unres.fallibility, Fallibility::fails);
    CHECK_EQUAL(p_unres.cost, Cost::expensive);
    CHECK_EQUAL(p_unres.expensive_ops, uint32_t{1});
    CHECK(not p_unres.is_duplicable());
    CHECK(not p_unres.is_tql_infallible());
    CHECK(not p_unres.is_pure_constant());
    CHECK(not unresolved.is_deterministic(reg));
  }
  // 10–11. record and list (plainness, spreads, operand-aware fallibility).
  {
    auto const p_empty_rec = ir::analyze(parse("{}"), reg);
    CHECK(p_empty_rec.is_plain());
    CHECK_EQUAL(p_empty_rec.fallibility, Fallibility::total);
    auto const p_const_rec = ir::analyze(parse("{a: 1}"), reg);
    CHECK(p_const_rec.is_plain());
    CHECK_EQUAL(p_const_rec.fallibility, Fallibility::total);
    auto const p_dyn_total = ir::analyze(parse("{a: x?}"), reg);
    CHECK_EQUAL(p_dyn_total.cost, Cost::cheap);
    CHECK_EQUAL(p_dyn_total.fallibility, Fallibility::total);
    auto const p_dyn_warns = ir::analyze(parse("{a: x}"), reg);
    CHECK_EQUAL(p_dyn_warns.cost, Cost::cheap);
    CHECK_EQUAL(p_dyn_warns.fallibility, Fallibility::warns);
    auto const p_one_spread = ir::analyze(parse("{...x?}"), reg);
    CHECK_EQUAL(p_one_spread.cost, Cost::cheap);
    CHECK_EQUAL(p_one_spread.fallibility, Fallibility::warns);
    CHECK(p_one_spread.is_tql_infallible());
    auto const p_two_spread = ir::analyze(parse("{...x, ...y}"), reg);
    CHECK_EQUAL(p_two_spread.cost, Cost::cheap);
    CHECK_EQUAL(p_two_spread.fallibility, Fallibility::fails);
    CHECK(p_two_spread.evaluable);
    CHECK(not p_two_spread.is_tql_infallible());
    auto const p_const_two_spread
      = ir::analyze(parse("({...{k: 1}, ...{j: 2}}).k"), reg);
    CHECK(p_const_two_spread.evaluable);
    CHECK(p_const_two_spread.is_pure_constant());
    CHECK(not p_const_two_spread.is_tql_infallible());
    // Lists.
    CHECK_EQUAL(ir::analyze(parse("[]"), reg).fallibility, Fallibility::total);
    CHECK(ir::analyze(parse("[]"), reg).is_plain());
    CHECK_EQUAL(ir::analyze(parse("[1]"), reg).fallibility, Fallibility::total);
    CHECK_EQUAL(ir::analyze(parse("[x?]"), reg).fallibility,
                Fallibility::total);
    CHECK_EQUAL(ir::analyze(parse("[x]"), reg).fallibility, Fallibility::warns);
    auto const p_list2 = ir::analyze(parse("[1, \"a\"]"), reg);
    CHECK(p_list2.is_plain());
    CHECK_EQUAL(p_list2.fallibility, Fallibility::warns);
    auto const p_list_spread = ir::analyze(parse("[...x?]"), reg);
    CHECK_EQUAL(p_list_spread.cost, Cost::cheap);
    CHECK_EQUAL(p_list_spread.fallibility, Fallibility::warns);
  }
  // 12. format_expr.
  {
    auto const p_fmt = ir::analyze(parse("f\"{1 + 2}\""), reg);
    CHECK_EQUAL(p_fmt.cost, Cost::expensive);
    CHECK_EQUAL(p_fmt.expensive_ops, uint32_t{1});
    CHECK_EQUAL(p_fmt.fallibility, Fallibility::warns);
    CHECK(p_fmt.is_const_foldable());
    CHECK(p_fmt.is_pure_constant());
    auto const p_fmt_nested = ir::analyze(parse("f\"{x.parse_json()}\""), reg);
    CHECK_EQUAL(p_fmt_nested.expensive_ops, uint32_t{2});
  }
  // 13. lambda_expr (in call arg vs. bare, bound params, nested).
  {
    auto const p_map_const = ir::analyze(parse("[1, 2].map(v => v + 1)"), reg);
    CHECK(not p_map_const.reads_event);
    CHECK(p_map_const.is_const_foldable());
    CHECK(p_map_const.is_pure_constant());
    CHECK_EQUAL(p_map_const.expensive_ops, uint32_t{2});
    auto const p_map_cap = ir::analyze(parse("[1, 2].map(v => v + x)"), reg);
    CHECK(p_map_cap.reads_event);
    CHECK(not p_map_cap.is_const_foldable());
    CHECK(not p_map_cap.is_pure_constant());
    auto const p_nested_lambda
      = ir::analyze(parse("[[1]].map(xs => xs.map(v => v + xs[0]))"), reg);
    CHECK(not p_nested_lambda.reads_event);
    CHECK(p_nested_lambda.is_pure_constant());
    auto const bare_lambda = parse("v => v + 1");
    auto const p_bare_lambda = ir::analyze(bare_lambda, reg);
    CHECK(not p_bare_lambda.evaluable);
    CHECK_EQUAL(p_bare_lambda.fallibility, Fallibility::fails);
    CHECK(not p_bare_lambda.is_tql_infallible());
    CHECK(not p_bare_lambda.is_const_foldable());
    CHECK(p_bare_lambda.is_pure_constant());
    CHECK(bare_lambda.is_deterministic(reg));
  }
  // 14–21. unpack, bare assignment, dollar_var, pkg_dollar_var, pipeline_expr,
  // underscore, type_expr, resolved_secret.
  {
    auto const unpack_expr
      = ast::expression{ast::unpack{parse("1"), location::unknown}};
    auto const p_unpack = ir::analyze(unpack_expr, reg);
    CHECK(not p_unpack.evaluable);
    CHECK_EQUAL(p_unpack.fallibility, Fallibility::fails);
    CHECK(p_unpack.is_pure_constant());
    CHECK(unpack_expr.is_deterministic(reg));
    // Bare assignments (including parser-reachable arbitrary LHS).
    auto const assign_const = parse("(x = 1)");
    auto const p_assign_const = ir::analyze(assign_const, reg);
    CHECK(p_assign_const.has_bare_assignment);
    CHECK(not p_assign_const.reads_event);
    CHECK(p_assign_const.is_pure_constant());
    CHECK(assign_const.is_deterministic(reg));
    CHECK(not p_assign_const.is_duplicable());
    CHECK(not p_assign_const.is_tql_infallible());
    auto const assign_ev = parse("(x = y)");
    auto const p_assign_ev = ir::analyze(assign_ev, reg);
    CHECK(p_assign_ev.reads_event);
    CHECK(not p_assign_ev.is_pure_constant());
    CHECK(assign_ev.is_deterministic(reg));
    auto const assign_var_lhs = parse("(a[$v] = 1)");
    auto const p_assign_var_lhs = ir::analyze(assign_var_lhs, reg);
    CHECK(p_assign_var_lhs.has_unbound_var);
    CHECK(p_assign_var_lhs.is_pure_constant());
    CHECK(not assign_var_lhs.is_deterministic(reg));
    auto const assign_rand_lhs = parse("(random() = 1)");
    auto const p_assign_rand_lhs = ir::analyze(assign_rand_lhs, reg);
    CHECK_EQUAL(p_assign_rand_lhs.volatility, Volatility::volatile_);
    CHECK(p_assign_rand_lhs.is_pure_constant());
    CHECK(not assign_rand_lhs.is_deterministic(reg));
    // Unbound dollar_var.
    auto const dvar = parse("$x");
    auto const p_dvar = ir::analyze(dvar, reg);
    CHECK(p_dvar.has_unbound_var);
    CHECK(not p_dvar.evaluable);
    CHECK(not p_dvar.is_duplicable());
    CHECK(not p_dvar.is_tql_infallible());
    CHECK(not p_dvar.is_pure_constant());
    CHECK(not dvar.is_deterministic(reg));
    // pkg_dollar_var: cached evaluable, cached legacy secret, uncached.
    auto pkg_eval = ast::pkg_dollar_var{
      {ast::identifier{"pkg", location::unknown}},
      ast::identifier{"$v", location::unknown},
    };
    pkg_eval.value = data{int64_t{42}};
    auto const pkg_eval_expr = ast::expression{pkg_eval};
    auto const p_pkg_eval = ir::analyze(pkg_eval_expr, reg);
    CHECK(p_pkg_eval.evaluable);
    CHECK_EQUAL(p_pkg_eval.fallibility, Fallibility::fails);
    CHECK(not p_pkg_eval.is_tql_infallible());
    CHECK(p_pkg_eval.is_const_foldable());
    CHECK(p_pkg_eval.is_pure_constant());
    CHECK(pkg_eval_expr.is_deterministic(reg));
    auto pkg_sec = pkg_eval;
    pkg_sec.value = data{secret::make_literal("s")};
    auto const pkg_sec_expr = ast::expression{pkg_sec};
    auto const p_pkg_sec = ir::analyze(pkg_sec_expr, reg);
    CHECK(not p_pkg_sec.evaluable);
    CHECK(p_pkg_sec.has_secret);
    CHECK(not p_pkg_sec.is_duplicable());
    CHECK(p_pkg_sec.is_pure_constant());
    CHECK(pkg_sec_expr.is_deterministic(reg));
    auto pkg_uncached = pkg_eval;
    pkg_uncached.value = None{};
    auto const pkg_uncached_expr = ast::expression{pkg_uncached};
    auto const p_pkg_uncached = ir::analyze(pkg_uncached_expr, reg);
    CHECK(not p_pkg_uncached.evaluable);
    CHECK(p_pkg_uncached.is_pure_constant());
    CHECK(pkg_uncached_expr.is_deterministic(reg));
    // pipeline_expr (treated as leaf with node_count == 1).
    auto const pipe_expr = parse("{ from {x: 1} | where x == 1 }");
    auto const p_pipe = ir::analyze(pipe_expr, reg);
    CHECK(p_pipe.has_non_value);
    CHECK_EQUAL(p_pipe.cost, Cost::expensive);
    CHECK_EQUAL(p_pipe.expensive_ops, uint32_t{1});
    CHECK_EQUAL(p_pipe.node_count, uint32_t{1});
    CHECK(not p_pipe.evaluable);
    CHECK(not p_pipe.is_duplicable());
    CHECK(not p_pipe.is_pure_constant());
    CHECK(not pipe_expr.is_deterministic(reg));
    // underscore and type_expr.
    auto const under_expr = ast::expression{ast::underscore{location::unknown}};
    auto const p_under = ir::analyze(under_expr, reg);
    CHECK(not p_under.evaluable);
    CHECK(not p_under.is_tql_infallible());
    CHECK(p_under.is_pure_constant());
    CHECK(under_expr.is_deterministic(reg));
    auto const ty_expr = ast::expression{ast::type_expr{
      location::unknown,
      ast::type_def{ast::type_name{ast::identifier{"int", location::unknown}}},
    }};
    auto const p_ty = ir::analyze(ty_expr, reg);
    CHECK(not p_ty.evaluable);
    CHECK(not p_ty.is_tql_infallible());
    CHECK(p_ty.is_pure_constant());
    CHECK(ty_expr.is_deterministic(reg));
    // resolved_secret.
    auto const rsec_bytes = std::array<std::byte, 3>{
      std::byte{'s'}, std::byte{'e'}, std::byte{'c'}};
    auto const rsec_expr = ast::expression{ast::resolved_secret{
      nova::Secret{ecc::cleansing_blob{rsec_bytes.begin(), rsec_bytes.end()}},
      location::unknown}};
    auto const p_rsec = ir::analyze(rsec_expr, reg);
    CHECK(p_rsec.has_secret);
    CHECK(not p_rsec.evaluable);
    CHECK(not p_rsec.is_duplicable());
    CHECK(not p_rsec.is_tql_infallible());
    CHECK(not p_rsec.is_pure_constant());
    CHECK(rsec_expr.is_deterministic(reg));
  }
}

TEST("SetIr does not substitute literal secret constants across set") {
  auto const& reg = test::builtin_registry();
  auto set_op = ir::SetIr{std::vector<ast::assignment>{ast::assignment{
    parse("x"),
    location::unknown,
    make_secret_constant("my-secret"),
  }}};
  auto opt = std::move(set_op).optimize(
    ir::OptimizeRequest{
      .filter = {parse("x == \"***\"")},
      .order = EventOrder::ordered,
    },
    ir::OptimizeCtx{.reg = reg});
  // `set` censors secrets to "***", so `where x == "***"` must stay behind it.
  CHECK(opt.filter.empty());
  CHECK_EQUAL(opt.replacement.operators.size(), size_t{2});
}

TEST("join key and on= timestamp validation reject runtime-evaluable "
     "constants") {
  CHECK_EQUAL(instantiate_pipeline_error("from {a: 1} | join (move 1) { from "
                                         "{b: 1} }"),
              "join key must reference at least one event field");
  CHECK_EQUAL(instantiate_pipeline_error("from {a: 1} | join {...{k: 1}, "
                                         "...{j: 2}}.k { from {b: 1} }"),
              "join key must reference at least one event field");
  CHECK_EQUAL(instantiate_pipeline_error("from {a: 1} | join ((x = 1) + 0) { "
                                         "from {b: 1} }"),
              "join key must reference at least one event field");
  CHECK_EQUAL(instantiate_pipeline_error("from {a: 1} | join kind == \"left\" "
                                         "{ from {b: 1} }"),
              "unexpected `==` with option name `kind`");
  CHECK_EQUAL(instantiate_pipeline_error("from {id: 1} | join id, within=5m, "
                                         "on=(move 1) { from {id: 1} }"),
              "`on` timestamp expression must reference an event field");
  CHECK_EQUAL(instantiate_pipeline_error("from {id: 1} | join id, within=5m, "
                                         "on={...{k: 1}, ...{j: 2}}.k { from "
                                         "{id: 1} }"),
              "`on` timestamp expression must reference an event field");
}

TEST("analyze_expr_without_registry degrades function calls conservatively") {
  auto const& reg = test::builtin_registry();
  auto const plain = parse("x.y[\"z\"]");
  CHECK_EQUAL(detail::analyze_expr_without_registry(plain),
              ir::analyze(plain, reg));
  auto const cheap_call = parse("x.ip()");
  auto const p_with = ir::analyze(cheap_call, reg);
  auto const p_without = detail::analyze_expr_without_registry(cheap_call);
  CHECK_EQUAL(p_with.node_count, p_without.node_count);
  CHECK_EQUAL(p_without.volatility, Volatility::volatile_);
  CHECK_EQUAL(p_without.fallibility, Fallibility::fails);
  CHECK_EQUAL(p_without.cost, Cost::expensive);
  CHECK_EQUAL(p_without.expensive_ops, uint32_t{1});
  CHECK(p_without.has_function_call);
  CHECK(not p_without.is_pure_constant());
  auto const sec_call = parse("secret(\"k\")");
  CHECK(detail::analyze_expr_without_registry(sec_call).has_secret);
}

TEST("simplify does not invoke field-rewrite hooks on malformed named-arg "
     "targets") {
  auto plugin = RewritablePlugin{"_test_rewritable"};
  auto reg = test::builtin_registry().clone();
  reg->add(std::string{entity_pkg_std}, plugin.name(), std::cref(plugin));
  // Malformed volatile named-argument LHS must be rejected before invoking
  // `rewrite_field_comparison` or `rewrite_field_access`.
  auto const volatile_lhs
    = parse("_test_rewritable(x, random() = 1).a == 1", *reg);
  std::ignore = ir::simplify_predicate(volatile_lhs, *reg);
  CHECK_EQUAL(plugin.comparison_calls, size_t{0});
  CHECK_EQUAL(plugin.access_calls, size_t{0});
  // Optional-marked named-argument LHS is also malformed and rejected up front.
  auto const optional_lhs = parse("_test_rewritable(x, opt?=1).a == 1", *reg);
  std::ignore = ir::simplify_predicate(optional_lhs, *reg);
  CHECK_EQUAL(plugin.comparison_calls, size_t{0});
  CHECK_EQUAL(plugin.access_calls, size_t{0});
  // A valid named argument still reaches the rewrite hook and rewrites to
  // `x == 1`.
  auto const valid = parse("_test_rewritable(x, opt=1).a == 1", *reg);
  auto const simplified = ir::simplify_predicate(valid, *reg);
  CHECK_EQUAL(plugin.comparison_calls, size_t{1});
  CHECK_EQUAL(fmt::format("{:?}", simplified),
              fmt::format("{:?}", parse("x == 1", *reg)));
}
