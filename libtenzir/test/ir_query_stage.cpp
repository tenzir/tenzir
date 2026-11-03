//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova_flag.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/pushdown/plan.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

/// A source that takes over at most `max_stages` stages of each chain that
/// the optimizer offers it, and records the chains.
class ChainSource final : public ir::Operator {
public:
  explicit ChainSource(size_t max_stages = std::numeric_limits<size_t>::max())
    : max_stages_{max_stages} {
  }

  auto name() const -> std::string override {
    return "chain_source";
  }

  auto copy() const -> Box<ir::Operator> override {
    return ChainSource{*this};
  }

  auto move() && -> Box<ir::Operator> override {
    return ChainSource{std::move(*this)};
  }

  auto infer_type(element_type_tag input, base_ctx) const
    -> failure_or<element_type_tag> override {
    return input;
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto spawn(element_type_tag, registry const&) const -> AnyOperator override {
    TENZIR_UNREACHABLE();
  }

  auto takes_query_stages() const -> bool override {
    return plan_.empty();
  }

  auto absorb(ir::QueryChain const& chain, ir::OptimizeCtx const&)
    -> ir::Absorption override {
    chains_.push_back(chain);
    auto const prefix = std::min(max_stages_, chain.stages.size());
    plan_.assign(chain.stages.begin(),
                 chain.stages.begin() + detail::narrow<std::ptrdiff_t>(prefix));
    return {.prefix = prefix, .declined = "too many stages"};
  }

  auto chains() const -> std::vector<ir::QueryChain> const& {
    return chains_;
  }

  auto absorbed_plan() const -> ir::QueryPlan const& {
    return plan_;
  }

private:
  size_t max_stages_;
  std::vector<ir::QueryChain> chains_;
  ir::QueryPlan plan_;
};

auto optimize_ctx() -> ir::OptimizeCtx {
  return ir::OptimizeCtx{.reg = test::builtin_registry()};
}

auto names(ir::pipeline const& pipe) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto const& op : pipe.operators) {
    result.push_back(op->display_name());
  }
  return result;
}

auto stage_kind(ir::QueryStage const& stage) -> std::string {
  return match(
    stage.kind,
    [](ir::QueryStage::Filter const&) {
      return std::string{"filter"};
    },
    [](ir::QueryStage::Select const&) {
      return std::string{"select"};
    },
    [](ir::QueryStage::Drop const&) {
      return std::string{"drop"};
    },
    [](ir::QueryStage::Aggregate const&) {
      return std::string{"aggregate"};
    },
    [](ir::QueryStage::Sort const&) {
      return std::string{"sort"};
    },
    [](ir::QueryStage::Limit const&) {
      return std::string{"limit"};
    });
}

auto kinds(std::span<ir::QueryStage const> stages) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto const& stage : stages) {
    result.push_back(stage_kind(stage));
  }
  return result;
}

/// Compiles and instantiates `source` behind a `ChainSource` that takes over
/// at most `max_stages` stages, and optimizes the result.
struct Optimized {
  ir::pipeline pipe;

  auto source() const -> ChainSource const& {
    auto const* source
      = dynamic_cast<ChainSource const*>(&*pipe.operators.front());
    REQUIRE(source);
    return *source;
  }
};

auto optimize(std::string_view source, size_t max_stages
                                       = std::numeric_limits<size_t>::max())
  -> Optimized {
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto ast = parse_pipeline_with_location_override(source, location::unknown,
                                                   provider.as_session());
  REQUIRE(ast);
  REQUIRE(resolve_entities(*ast, ExternPrelude::empty(), ctx));
  auto compiled = std::move(*ast).compile(compile_ctx::make_root(ctx));
  REQUIRE(compiled);
  auto instantiated = ir::instantiate(std::move(*compiled), ctx);
  REQUIRE(instantiated);
  instantiated->operators.insert(instantiated->operators.begin(),
                                 ChainSource{max_stages});
  return Optimized{ir::optimize(std::move(*instantiated), optimize_ctx())};
}

using Strings = std::vector<std::string>;

} // namespace

TEST("sources take over the chain of stages behind them") {
  auto result = optimize("where x > 1\n"
                         "select a, b=c.d\n"
                         "drop b\n"
                         "sort -a\n"
                         "head 3");
  // Every operator was a stage, so only the source remains.
  CHECK_EQUAL(names(result.pipe), (Strings{"chain_source"}));
  auto const& plan = result.source().absorbed_plan();
  CHECK_EQUAL(kinds(plan),
              (Strings{"filter", "select", "drop", "sort", "limit"}));
  auto const& select = as<ir::QueryStage::Select>(plan[1].kind);
  REQUIRE_EQUAL(select.fields.size(), size_t{2});
  CHECK_EQUAL(select.fields[1].output, (Strings{"b"}));
  CHECK_EQUAL(select.fields[1].input, (Strings{"c", "d"}));
  // The sort absorbed the limit of the `head`, which comes along.
  CHECK_EQUAL(as<ir::QueryStage::Sort>(plan[3].kind).bound,
              Option<uint64_t>{3});
  CHECK(result.source().chains().front().end.empty());
}

TEST("sources take over a prefix of the chain") {
  auto result = optimize("where x > 1\n"
                         "sort x\n"
                         "head 3\n"
                         "write_json",
                         1);
  CHECK_EQUAL(names(result.pipe),
              (Strings{"chain_source", "sort", "head", "write_json"}));
  CHECK_EQUAL(kinds(result.source().absorbed_plan()), (Strings{"filter"}));
  auto const& chain = result.source().chains().front();
  CHECK_EQUAL(kinds(chain.stages), (Strings{"filter", "sort", "limit"}));
  CHECK_EQUAL(chain.end, std::string{"`write_json` is no stage"});
}

TEST("the chain folds transforms into an aggregation") {
  // The summary reads through the rename, and the source takes over both as
  // one stage.
  auto result = optimize("y = x\n"
                         "drop z\n"
                         "summarize y, n=count()\n"
                         "where n > 1");
  CHECK_EQUAL(names(result.pipe), (Strings{"chain_source"}));
  auto const& plan = result.source().absorbed_plan();
  CHECK_EQUAL(kinds(plan), (Strings{"aggregate", "filter"}));
  auto const& aggregate = as<ir::QueryStage::Aggregate>(plan[0].kind);
  CHECK_EQUAL(aggregate.columns[0].output, (Strings{"y"}));
  CHECK_EQUAL(aggregate.columns[0].input, Option{Strings{"x"}});
  CHECK_EQUAL(aggregate.schema_name, std::string{"tenzir.summarize"});
  // Declining the aggregation keeps the transforms, too.
  auto declined = optimize("y = x\n"
                           "summarize y, n=count()",
                           0);
  CHECK_EQUAL(names(declined.pipe),
              (Strings{"chain_source", "set", "summarize"}));
}

TEST("the chain ends where a source could not compute it") {
  // A sort by a computed value is no stage.
  auto computed = optimize("sort x * 2\n"
                           "head 3");
  CHECK(computed.source().absorbed_plan().empty());
  CHECK_EQUAL(computed.source().chains().front().end,
              std::string{"`sort` is no stage"});
  // A sort that keeps only its first events comes only with its limit.
  auto bounded = optimize("sort x\n"
                          "select x\n"
                          "head 3");
  CHECK(bounded.source().absorbed_plan().empty());
  CHECK_EQUAL(bounded.source().chains().front().end,
              std::string{"`sort` keeps only its first events, but no limit "
                          "follows it"});
  // A second aggregation ends the chain.
  auto twice = optimize("summarize x, n=count()\n"
                        "summarize m=count()");
  CHECK_EQUAL(kinds(twice.source().absorbed_plan()), (Strings{"aggregate"}));
  CHECK_EQUAL(names(twice.pipe), (Strings{"chain_source", "summarize"}));
  // Assignments other than plain selections are no stage.
  auto computed_select = optimize("select a=b + 1");
  CHECK(computed_select.source().absorbed_plan().empty());
  auto overlapping = optimize("select a, a.b");
  CHECK(overlapping.source().absorbed_plan().empty());
  // An empty selection keeps the events.
  auto empty = optimize("select");
  CHECK_EQUAL(kinds(empty.source().absorbed_plan()), (Strings{"select"}));
}

TEST("sources leave the rest of a partially accepted chain in place") {
  // A source that declines a folded aggregation keeps its transforms.
  auto folded = optimize("where x > 1\n"
                         "y = x\n"
                         "summarize y, n=count()",
                         1);
  CHECK_EQUAL(kinds(folded.source().absorbed_plan()), (Strings{"filter"}));
  CHECK_EQUAL(names(folded.pipe),
              (Strings{"chain_source", "set", "summarize"}));
  // A pushed sort leaves the `head` that enforces its bound.
  auto sorted = optimize("sort x\n"
                         "head 3",
                         1);
  CHECK_EQUAL(kinds(sorted.source().absorbed_plan()), (Strings{"sort"}));
  CHECK_EQUAL(names(sorted.pipe), (Strings{"chain_source", "head"}));
}

TEST("a source with a plan declines later passes") {
  auto result = optimize("where x > 1\n"
                         "head 3");
  CHECK_EQUAL(result.source().chains().size(), size_t{1});
  auto again = ir::optimize(std::move(result.pipe), optimize_ctx());
  auto const* source
    = dynamic_cast<ChainSource const*>(&*again.operators.front());
  REQUIRE(source);
  CHECK_EQUAL(source->chains().size(), size_t{1});
  CHECK_EQUAL(names(again), (Strings{"chain_source"}));
}

TEST("sources do not see through barriers") {
  auto result = optimize("parallel { where x > 1 }\n"
                         "head 3");
  CHECK(result.source().absorbed_plan().empty());
  CHECK_EQUAL(names(result.pipe),
              (Strings{"chain_source", "parallel", "head"}));
}

namespace {

struct PlanSourceArgs {
  Option<located<bool>> pushdown;
  OptimizationArgs<opt::Plan> optimization;
};

/// An operator plugin whose operator takes over at most two stages, unless
/// `pushdown=false`.
class PlanSource final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "plan_source";
  }

  auto describe() const -> Description override {
    auto d = Describer<PlanSourceArgs>{};
    pushdown::add_pushdown_argument(d, &PlanSourceArgs::pushdown);
    d.optimization(&PlanSourceArgs::optimization);
    d.query_plan([](PlanSourceArgs const& args,
                    ir::QueryChain const& chain) -> ir::Absorption {
      if (not pushdown::pushdown_enabled(args.pushdown)) {
        return {.prefix = 0, .declined = "`pushdown=false`"};
      }
      return {.prefix = std::min(chain.stages.size(), size_t{2}),
              .declined = "only two stages"};
    });
    return d.without_optimize();
  }
};

struct UnboundSourceArgs {};

/// An operator plugin that accepts stages but does not bind `opt::Plan`, so
/// it cannot receive them.
class UnboundSource final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "unbound_source";
  }

  auto describe() const -> Description override {
    auto d = Describer<UnboundSourceArgs>{};
    d.query_plan([](UnboundSourceArgs const&,
                    ir::QueryChain const& chain) -> ir::Absorption {
      return {.prefix = chain.stages.size(), .declined = ""};
    });
    return d.without_optimize();
  }
};

/// Compiles `source` behind an instance of `PlanSource` with `args`,
/// optimizes it, and returns the debug rendering of the pipeline.
auto optimize_plan_source(std::string_view args, std::string_view source,
                          OperatorPlugin const& plugin = PlanSource{})
  -> std::pair<std::vector<std::string>, std::string> {
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto ast = parse_pipeline_with_location_override(
    fmt::format("from {{}}\n{}", source), location::unknown,
    provider.as_session());
  REQUIRE(ast);
  REQUIRE(resolve_entities(*ast, ExternPrelude::empty(), ctx));
  auto compiled = std::move(*ast).compile(compile_ctx::make_root(ctx));
  REQUIRE(compiled);
  // Replace the `from` with the plan source.
  auto invocation = ast::invocation{
    ast::entity{{ast::identifier{plugin.name(), location::unknown}}}, {}};
  if (not args.empty()) {
    invocation.args.push_back(ast::assignment{
      ast::field_path::try_from(
        ast::root_field{ast::identifier{"pushdown", location::unknown}, false})
        ->inner(),
      location::unknown, ast::constant{args == "true", location::unknown}});
  }
  auto op = plugin.compile(std::move(invocation), compile_ctx::make_root(ctx));
  REQUIRE(op);
  auto ops = std::move(*op).unwrap().operators;
  REQUIRE_EQUAL(ops.size(), size_t{1});
  compiled->operators.front() = std::move(ops.front());
  auto instantiated = ir::instantiate(std::move(*compiled), ctx);
  REQUIRE(instantiated);
  auto optimized = ir::optimize(std::move(*instantiated), optimize_ctx());
  return {names(optimized), fmt::format("{:?}", optimized)};
}

} // namespace

TEST("operator plugins take over stages through their description") {
  auto [names, dump] = optimize_plan_source("", "where x > 1\n"
                                                "select x\n"
                                                "head 3");
  CHECK_EQUAL(names, (Strings{"plan_source", "head"}));
  // The explain output shows the plan and why it ends.
  CHECK(dump.contains("plan: ["));
  CHECK(dump.contains("Filter {"));
  CHECK(dump.contains("Select {"));
  CHECK(dump.contains("declined: \"only two stages\""));
  // A filter behind a limit, which the walk cannot move before it, stays
  // where it is when the source takes over less.
  std::tie(names, dump) = optimize_plan_source("", "where x > 1\n"
                                                   "head 5\n"
                                                   "where x > 2\n"
                                                   "head 3");
  CHECK_EQUAL(names, (Strings{"plan_source", "where", "head"}));
  // `pushdown=false` keeps every stage in the pipeline.
  std::tie(names, dump) = optimize_plan_source("false", "where x > 1\n"
                                                        "head 3");
  CHECK_EQUAL(names, (Strings{"plan_source", "where", "head"}));
  CHECK(not dump.contains("plan: ["));
  CHECK(dump.contains("declined: \"`pushdown=false`\""));
}

TEST("operator plugins without a plan binding take over no stages") {
  auto [names, dump] = optimize_plan_source("",
                                            "where x > 1\n"
                                            "head 3",
                                            UnboundSource{});
  CHECK_EQUAL(names, (Strings{"unbound_source", "where", "head"}));
  CHECK(not dump.contains("plan: ["));
}

TEST("queries in logs lose their string literals") {
  // Standard SQL doubles a quote, and a backslash is a character of its own.
  CHECK_EQUAL(
    pushdown::redact_query("SELECT 'C:\\', 'secret', 'it''s' FROM \"a'b\" "
                           "WHERE `c'd` = 1"),
    std::string{"SELECT '***', '***', '***' FROM \"a'b\" WHERE `c'd` = 1"});
  // Where a backslash escapes, it may escape a quote.
  auto const escapes = pushdown::SqlLexicon{
    .backslash_escapes = true,
    .double_quoted_strings = false,
    .bracket_identifiers = false,
    .identifier_backslash_escapes = false,
    .hash_comments = false,
  };
  CHECK_EQUAL(pushdown::redact_query("SELECT 'a\\'b', 'secret'", escapes),
              std::string{"SELECT '***', '***'"});
  // MySQL may quote strings with double quotes.
  auto const mysql = pushdown::SqlLexicon{
    .backslash_escapes = true,
    .double_quoted_strings = true,
    .bracket_identifiers = false,
    .identifier_backslash_escapes = false,
    .hash_comments = false,
  };
  CHECK_EQUAL(pushdown::redact_query("SELECT \"secret\" FROM `t`", mysql),
              std::string{"SELECT \"***\" FROM `t`"});
  // SQL Server quotes identifiers with brackets, but ClickHouse writes arrays
  // with them, whose literals must go.
  auto const brackets = pushdown::SqlLexicon{
    .backslash_escapes = false,
    .double_quoted_strings = false,
    .bracket_identifiers = true,
    .identifier_backslash_escapes = false,
    .hash_comments = false,
  };
  CHECK_EQUAL(pushdown::redact_query("SELECT [a'b] FROM t", brackets),
              std::string{"SELECT [a'b] FROM t"});
  CHECK_EQUAL(pushdown::redact_query("SELECT x IN ['secret'] FROM t"),
              std::string{"SELECT x IN ['***'] FROM t"});
  // Quotes in comments neither start nor end a literal, and comments go, too.
  CHECK_EQUAL(
    pushdown::redact_query("SELECT 1 -- it's fine\nWHERE p = 'secret' /* a'b "
                           "*/ AND q = 'secret'"),
    std::string{"SELECT 1 -- ***\nWHERE p = '***' /* *** */ AND q = '***'"});
  auto const hashes = pushdown::SqlLexicon{
    .backslash_escapes = true,
    .double_quoted_strings = true,
    .bracket_identifiers = false,
    .identifier_backslash_escapes = false,
    .hash_comments = true,
  };
  CHECK_EQUAL(pushdown::redact_query("SELECT 1 # it's\n, 'secret'", hashes),
              std::string{"SELECT 1 # ***\n, '***'"});
  CHECK_EQUAL(pushdown::redact_query("SELECT 1 /* it's", hashes),
              std::string{"SELECT 1 /* *** */"});
  // Block comments nest.
  CHECK_EQUAL(pushdown::redact_query("SELECT 1 /* outer /* inner */ "
                                     "password=secret */, 'x'"),
              std::string{"SELECT 1 /* *** */, '***'"});
  // ClickHouse escapes a backtick in an identifier with a backslash.
  auto const clickhouse = pushdown::SqlLexicon{
    .backslash_escapes = true,
    .double_quoted_strings = false,
    .bracket_identifiers = false,
    .identifier_backslash_escapes = true,
    .hash_comments = false,
  };
  CHECK_EQUAL(pushdown::redact_query("SELECT `with\\`tick`, 'secret'",
                                     clickhouse),
              std::string{"SELECT `with\\`tick`, '***'"});
  // An unterminated literal is redacted to the end.
  CHECK_EQUAL(pushdown::redact_query("SELECT 'unterminated"),
              std::string{"SELECT '***'"});
}
