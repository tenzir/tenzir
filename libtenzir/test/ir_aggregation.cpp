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
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>

#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

using Path = std::vector<std::string>;

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

auto assign(std::string_view left, std::string_view right) -> ast::assignment {
  return ast::assignment{parse(left), location::unknown, parse(right)};
}

/// A summary `summarize k, n=count(), m=max(v)`.
auto make_aggregate() -> ir::QueryStage::Aggregate {
  return ir::QueryStage::Aggregate{
    .columns = {
      {.output = {"k"}, .function = None{}, .input = Path{"k"}},
      {.output = {"n"}, .function = "count", .input = None{}},
      {.output = {"m"}, .function = "max", .input = Path{"v", "w"}},
    },
    .schema_name = "tenzir.summarize",
  };
}

/// Returns the inputs of the columns of `aggregate`, with an empty path for a
/// column without input.
auto inputs(ir::QueryStage::Aggregate const& aggregate) -> std::vector<Path> {
  auto result = std::vector<Path>{};
  for (auto const& column : aggregate.columns) {
    result.push_back(column.input.value_or(Path{}));
  }
  return result;
}

auto outputs(ir::QueryStage::Aggregate const& aggregate) -> std::vector<Path> {
  auto result = std::vector<Path>{};
  for (auto const& column : aggregate.columns) {
    result.push_back(column.output);
  }
  return result;
}

/// An operator for testing how the optimizer folds aggregations: a source
/// that takes over at most a number of stages behind it, an operator that may
/// rewrite an aggregation by renaming a field, a summary that describes one,
/// or another stage. Every other operator is a barrier.
class Mock final : public ir::Operator {
public:
  enum class Kind { source, rename, barrier, summary, stage };

  Mock(std::string tag, Kind kind, std::string from = {}, std::string to = {})
    : tag_{std::move(tag)},
      kind_{kind},
      from_{std::move(from)},
      to_{std::move(to)} {
  }

  /// An operator that describes itself as `stage`.
  Mock(std::string tag, ir::QueryStage stage)
    : tag_{std::move(tag)}, kind_{Kind::stage}, stage_{std::move(stage)} {
  }

  /// A source that takes over at most `max_stages` stages.
  static auto source(std::string tag, size_t max_stages) -> Mock {
    auto result = Mock{std::move(tag), Kind::source};
    result.max_stages_ = max_stages;
    return result;
  }

  auto name() const -> std::string override {
    return tag_;
  }

  auto copy() const -> Box<ir::Operator> override {
    return Mock{*this};
  }

  auto move() && -> Box<ir::Operator> override {
    return Mock{std::move(*this)};
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

  auto rewrite_aggregate(ir::QueryStage::Aggregate aggregate,
                         ir::OptimizeCtx const& octx) const
    -> Option<ir::QueryStage::Aggregate> override {
    TENZIR_UNUSED(octx);
    if (kind_ != Kind::rename) {
      return None{};
    }
    return ir::rewrite_aggregate(
      std::move(aggregate), [&](ast::field_path const& field) {
        if (field.path().size() == 1 and field.path()[0].id.name == to_) {
          return Option<ast::expression>{parse(from_)};
        }
        return Option<ast::expression>{field.inner()};
      });
  }

  auto query_stage(ir::OptimizeCtx const&) const
    -> Option<ir::QueryStage> override {
    if (kind_ == Kind::summary) {
      return ir::QueryStage{make_aggregate()};
    }
    return stage_;
  }

  auto takes_query_stages() const -> bool override {
    return kind_ == Kind::source and plan_.empty();
  }

  auto absorb(ir::QueryChain const& chain, ir::OptimizeCtx const&)
    -> ir::Absorption override {
    auto const prefix = std::min(max_stages_, chain.stages.size());
    plan_.assign(chain.stages.begin(),
                 chain.stages.begin() + detail::narrow<std::ptrdiff_t>(prefix));
    return {.prefix = prefix, .declined = "too many stages"};
  }

  auto absorbed_plan() const -> ir::QueryPlan const& {
    return plan_;
  }

private:
  std::string tag_;
  Kind kind_;
  std::string from_;
  std::string to_;
  Option<ir::QueryStage> stage_;
  size_t max_stages_ = std::numeric_limits<size_t>::max();
  ir::QueryPlan plan_;
};

auto filter_mock(std::string tag) -> Mock {
  return Mock{std::move(tag),
              ir::QueryStage{ir::QueryStage::Filter{parse("n > 1")}}};
}

auto sort_mock(std::string tag, bool bounded = false) -> Mock {
  return Mock{std::move(tag),
              ir::QueryStage{ir::QueryStage::Sort{
                .keys = {{.field = {"n"}, .descending = true}},
                .bound = bounded ? Option<uint64_t>{3} : Option<uint64_t>{},
              }}};
}

auto limit_mock(std::string tag) -> Mock {
  return Mock{std::move(tag), ir::QueryStage{ir::QueryStage::Limit{3}}};
}

/// The kind of `stage`, in lowercase.
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

/// The kinds of `stages`, in order.
auto stage_kinds(ir::QueryPlan const& stages) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto const& stage : stages) {
    result.push_back(stage_kind(stage));
  }
  return result;
}

auto make_pipeline(std::vector<Mock> ops) -> ir::pipeline {
  auto result = ir::pipeline{};
  for (auto& op : ops) {
    result.operators.push_back(std::move(op));
  }
  return result;
}

auto optimize_ctx() -> ir::OptimizeCtx {
  return ir::OptimizeCtx{.reg = test::builtin_registry()};
}

auto optimized(ir::pipeline pipe) -> ir::pipeline {
  return ir::optimize(std::move(pipe), optimize_ctx());
}

auto names(ir::pipeline const& pipe) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto const& op : pipe.operators) {
    result.push_back(op->display_name());
  }
  return result;
}

/// The stages that the source at `index` of `pipe` took over.
auto plan(ir::pipeline const& pipe, size_t index) -> ir::QueryPlan const& {
  auto const* mock = dynamic_cast<Mock const*>(&*pipe.operators.at(index));
  REQUIRE(mock);
  return mock->absorbed_plan();
}

/// The aggregation that the source at `index` of `pipe` took over as its
/// first stage.
auto aggregate(ir::pipeline const& pipe, size_t index)
  -> ir::QueryStage::Aggregate const& {
  auto const& stages = plan(pipe, index);
  REQUIRE(not stages.empty());
  auto const* result = try_as<ir::QueryStage::Aggregate>(stages.front().kind);
  REQUIRE(result);
  return *result;
}

/// Compiles `source` without instantiating it, as for a subpipeline.
auto compile(base_ctx ctx, std::string_view source) -> ir::pipeline {
  auto provider = session_provider::make(ctx.dh(), ctx.reg());
  auto ast = parse_pipeline_with_location_override(source, location::unknown,
                                                   provider.as_session());
  REQUIRE(ast);
  REQUIRE(resolve_entities(*ast, ExternPrelude::empty(), ctx));
  auto compiled = std::move(*ast).compile(compile_ctx::make_root(ctx));
  REQUIRE(compiled);
  return std::move(*compiled);
}

/// Rewrites `make_aggregate()` through a `set` with `assignments`.
auto through_set(std::vector<ast::assignment> assignments)
  -> Option<ir::QueryStage::Aggregate> {
  return ir::SetIr{std::move(assignments)}.rewrite_aggregate(make_aggregate(),
                                                             optimize_ctx());
}

} // namespace

TEST("aggregation rewrite maps inputs and keeps outputs") {
  auto rewritten
    = ir::rewrite_aggregate(make_aggregate(), [](ast::field_path const& field) {
        auto name = field.path().front().id.name;
        if (name == "k") {
          return Option<ast::expression>{parse("a.b")};
        }
        return Option<ast::expression>{field.inner()};
      });
  REQUIRE(rewritten);
  CHECK_EQUAL(inputs(*rewritten),
              (std::vector<Path>{{"a", "b"}, {}, {"v", "w"}}));
  CHECK_EQUAL(outputs(*rewritten), outputs(make_aggregate()));
  CHECK_EQUAL(rewritten->schema_name, "tenzir.summarize");
}

TEST("aggregation rewrite requires plain fields") {
  auto rewrite_k = [](std::string_view replacement) {
    return ir::rewrite_aggregate(
      make_aggregate(), [&](ast::field_path const& field) {
        if (field.path().front().id.name == "k") {
          return Option<ast::expression>{parse(replacement)};
        }
        return Option<ast::expression>{field.inner()};
      });
  };
  CHECK(rewrite_k("x"));
  CHECK(not rewrite_k("x?"));
  CHECK(not rewrite_k("x.y?"));
  CHECK(not rewrite_k("x + 1"));
  CHECK(not rewrite_k("x.length()"));
  CHECK(not rewrite_k("null"));
  CHECK(not rewrite_k("this"));
  CHECK(not rewrite_k("x[0]"));
  // A field without replacement prevents the rewrite.
  CHECK(not ir::rewrite_aggregate(make_aggregate(), [](ast::field_path const&) {
    return Option<ast::expression>{};
  }));
}

TEST("aggregation rewrite through set") {
  // Renames and copies map back to the source field.
  auto renamed = through_set({assign("k", "x"), assign("v", "y")});
  REQUIRE(renamed);
  CHECK_EQUAL(inputs(*renamed), (std::vector<Path>{{"x"}, {}, {"y", "w"}}));
  // Assignments to other fields leave the inputs as they are.
  auto unrelated
    = through_set({assign("z", "x + 1"), assign("@name", "\"a\"")});
  REQUIRE(unrelated);
  CHECK_EQUAL(inputs(*unrelated), inputs(make_aggregate()));
  // The source skips the `set`, so no assignment may fail the pipeline, also
  // not one whose value the summary does not read.
  CHECK(not through_set({assign("z", "x.length()")}));
  CHECK(not through_set({assign("z", "file_contents(x)")}));
  CHECK(not through_set({assign("y[x.length()]", "1")}));
  CHECK(not through_set({assign("@name", "x.string()")}));
  // Type inference rejects `move this` with Nova events, so the `set` must
  // stay, although the summary reads neither field.
  CHECK(not through_set({assign("z", "move this")}));
  CHECK(through_set({assign("z", "move x")}));
  // Every right-hand side reads the input, so a swap maps each field to the
  // other one.
  auto swapped = through_set({assign("k", "v.w"), assign("v.w", "k")});
  REQUIRE(swapped);
  CHECK_EQUAL(inputs(*swapped), (std::vector<Path>{{"v", "w"}, {}, {"k"}}));
  // `move` reads the field before dropping it.
  auto moved = through_set({assign("k", "move x")});
  REQUIRE(moved);
  CHECK_EQUAL(inputs(*moved).front(), Path{"x"});
  CHECK(not through_set({assign("t", "move k")}));
  // Computed values, optional access, and overwrites stay local.
  CHECK(not through_set({assign("k", "x + 1")}));
  CHECK(not through_set({assign("k", "x.length()")}));
  CHECK(not through_set({assign("k", "x if c else y")}));
  CHECK(not through_set({assign("k", "x?")}));
  CHECK(not through_set({assign("k", "x"), assign("k", "k + 1")}));
  // A later assignment shadows an earlier one.
  auto shadowed = through_set({assign("k", "x + 1"), assign("k", "y")});
  REQUIRE(shadowed);
  CHECK_EQUAL(inputs(*shadowed).front(), Path{"y"});
  // Assignments into or over a field that the summary reads change its value.
  CHECK(not through_set({assign("v.w.z", "1")}));
  CHECK(not through_set({assign("v", "{w: 1}")}));
  // Replacing a record that contains the field reads from the new record.
  auto replaced = through_set({assign("v", "x")});
  REQUIRE(replaced);
  CHECK_EQUAL(inputs(*replaced).back(), (Path{"x", "w"}));
}

TEST("aggregation rewrite through select") {
  // `select` desugars to `this = {}` and assignments of the kept fields.
  auto selected = through_set({assign("this", "{}"), assign("k", "k"),
                               assign("v", "v"), assign("x", "x")});
  REQUIRE(selected);
  CHECK_EQUAL(inputs(*selected), inputs(make_aggregate()));
  auto renamed = through_set(
    {assign("this", "{}"), assign("k", "a.b"), assign("v.w", "c")});
  REQUIRE(renamed);
  CHECK_EQUAL(inputs(*renamed), (std::vector<Path>{{"a", "b"}, {}, {"c"}}));
  // A field that `select` does not keep reads as `null`.
  CHECK(not through_set({assign("this", "{}"), assign("k", "k")}));
  // Keeping only part of a record changes the record.
  auto parts = through_set(
    {assign("this", "{}"), assign("k", "k"), assign("v.w.x", "v.w.x")});
  CHECK(not parts);
  // Assigning other values than record literals to `this` stays local.
  CHECK(not through_set({assign("this", "r")}));
  CHECK(not through_set({assign("this", "{...r}")}));
}

TEST("optimizer folds an aggregation through operators that rewrite it") {
  using enum Mock::Kind;
  auto pipe = optimized(make_pipeline({
    Mock{"source", source},
    Mock{"first", rename, "a", "b"},
    Mock{"second", rename, "b", "k"},
    Mock{"summary", summary},
    Mock{"after", barrier},
  }));
  // The source computes the summary in place of the operators before it.
  CHECK_EQUAL(names(pipe), (std::vector<std::string>{"source", "after"}));
  CHECK_EQUAL(stage_kinds(plan(pipe, 0)),
              (std::vector<std::string>{"aggregate"}));
  // The input maps back through both renames, and the outputs stay.
  auto const& folded = aggregate(pipe, 0);
  CHECK_EQUAL(inputs(folded), (std::vector<Path>{{"a"}, {}, {"v", "w"}}));
  CHECK_EQUAL(outputs(folded), outputs(make_aggregate()));
  CHECK_EQUAL(folded.schema_name, "tenzir.summarize");
  // Another pass finds nothing left to take over.
  auto again = optimized(std::move(pipe));
  CHECK_EQUAL(names(again), (std::vector<std::string>{"source", "after"}));
  CHECK_EQUAL(inputs(aggregate(again, 0)),
              (std::vector<Path>{{"a"}, {}, {"v", "w"}}));
}

TEST("optimizer keeps the operators that no source takes over") {
  using enum Mock::Kind;
  // A barrier between the source and the summary stops the rewrite.
  auto blocked = optimized(make_pipeline({
    Mock{"source", source},
    Mock{"barrier", barrier},
    Mock{"rename", rename, "a", "k"},
    Mock{"summary", summary},
  }));
  CHECK_EQUAL(names(blocked), (std::vector<std::string>{"source", "barrier",
                                                        "rename", "summary"}));
  CHECK(plan(blocked, 0).empty());
  // A stage that cannot rewrite the aggregation stays a stage of its own.
  auto unrewritable = optimized(make_pipeline({
    Mock{"source", source},
    filter_mock("where"),
    Mock{"summary", summary},
  }));
  CHECK_EQUAL(names(unrewritable), (std::vector<std::string>{"source"}));
  CHECK_EQUAL(stage_kinds(plan(unrewritable, 0)),
              (std::vector<std::string>{"filter", "aggregate"}));
  // Without a source, the rewrites leave every operator in place.
  auto sourceless = optimized(make_pipeline({
    Mock{"rename", rename, "a", "k"},
    Mock{"summary", summary},
  }));
  CHECK_EQUAL(names(sourceless),
              (std::vector<std::string>{"rename", "summary"}));
  // A source that declines the aggregation keeps the operators that it would
  // have folded.
  auto declined = optimized(make_pipeline({
    Mock::source("source", 0),
    Mock{"rename", rename, "a", "k"},
    Mock{"summary", summary},
  }));
  CHECK_EQUAL(names(declined),
              (std::vector<std::string>{"source", "rename", "summary"}));
  CHECK(plan(declined, 0).empty());
}

TEST("optimizer folds only the first aggregation behind a source") {
  using enum Mock::Kind;
  // The source takes over the first summary. The second one ends the chain.
  auto pipe = optimized(make_pipeline({
    Mock{"source", source},
    Mock{"first", summary},
    Mock{"second", summary},
  }));
  CHECK_EQUAL(names(pipe), (std::vector<std::string>{"source", "second"}));
  CHECK_EQUAL(stage_kinds(plan(pipe, 0)),
              (std::vector<std::string>{"aggregate"}));
  // A rename between the two cannot carry the second one to the source
  // either.
  auto renamed = optimized(make_pipeline({
    Mock{"source", source},
    Mock{"first", summary},
    Mock{"rename", rename, "a", "k"},
    Mock{"second", summary},
  }));
  CHECK_EQUAL(names(renamed),
              (std::vector<std::string>{"source", "rename", "second"}));
  CHECK_EQUAL(stage_kinds(plan(renamed, 0)),
              (std::vector<std::string>{"aggregate"}));
}

TEST("sources take over aggregations behind drops") {
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto run = [&](std::string_view source) {
    auto pipe = ir::instantiate(compile(ctx, source), ctx);
    REQUIRE(pipe);
    return names(ir::optimize(std::move(*pipe), optimize_ctx()));
  };
  // `drop` passes the summary to the source.
  CHECK_EQUAL(run("from_mysql table=\"t\", host=\"h\"\n"
                  "drop y\n"
                  "summarize x, n=count()"),
              (std::vector<std::string>{"from_mysql"}));
  // MySQL computes a limit before the summary, too.
  CHECK_EQUAL(run("from_mysql table=\"t\", host=\"h\"\n"
                  "head 3\n"
                  "drop y\n"
                  "summarize x, n=count()"),
              (std::vector<std::string>{"from_mysql"}));
}

TEST("optimizer offers the stages after an aggregation") {
  using enum Mock::Kind;
  using Strings = std::vector<std::string>;
  // The source takes over the stages, whose operators leave the pipeline.
  auto pipe = optimized(make_pipeline({
    Mock{"source", source},
    Mock{"summary", summary},
    filter_mock("where"),
    sort_mock("sort", true),
    limit_mock("head"),
    Mock{"after", barrier},
  }));
  CHECK_EQUAL(names(pipe), (Strings{"source", "after"}));
  CHECK_EQUAL(stage_kinds(plan(pipe, 0)),
              (Strings{"aggregate", "filter", "sort", "limit"}));
  // A source that takes over fewer stages leaves the others in place.
  auto fewer = optimized(make_pipeline({
    Mock::source("source", 2),
    Mock{"summary", summary},
    filter_mock("where"),
    sort_mock("sort"),
    limit_mock("head"),
  }));
  CHECK_EQUAL(stage_kinds(plan(fewer, 0)), (Strings{"aggregate", "filter"}));
  CHECK_EQUAL(names(fewer), (Strings{"source", "sort", "head"}));
}

TEST("operators describe the stages after an aggregation") {
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto run = [&](std::string_view source) {
    auto pipe = ir::instantiate(compile(ctx, source), ctx);
    REQUIRE(pipe);
    return ir::optimize(std::move(*pipe), optimize_ctx());
  };
  // Renders the first operator of `pipe`.
  auto first = [](ir::pipeline const& pipe) {
    auto single = ir::pipeline{};
    single.operators.push_back(pipe.operators.front()->copy());
    return fmt::format("{:?}", single);
  };
  auto pipe = run("from_mysql table=\"t\", host=\"h\"\n"
                  "summarize x, n=count()\n"
                  "where n > 1\n"
                  "sort -n, x\n"
                  "head 3");
  // The source takes over the stages, and their operators leave.
  CHECK_EQUAL(names(pipe), (std::vector<std::string>{"from_mysql"}));
  auto dump = first(pipe);
  CHECK(dump.contains("Filter"));
  CHECK(dump.contains("Sort"));
  CHECK(dump.contains("bound: 3"));
  CHECK(dump.contains("Limit"));
  // A copy serializes the stages.
  auto copied = ir::pipeline{};
  copied.operators.push_back(pipe.operators.front()->copy());
  CHECK_EQUAL(first(copied), dump);
  // `top` sorts by the count, which the source computes with the limit.
  pipe = run("from_mysql table=\"t\", host=\"h\"\n"
             "top x\n"
             "head 3");
  CHECK_EQUAL(names(pipe), (std::vector<std::string>{"from_mysql"}));
  // A sort by a computed value is no stage.
  pipe = run("from_mysql table=\"t\", host=\"h\"\n"
             "summarize x, n=count()\n"
             "sort n * 2\n"
             "head 3");
  CHECK_EQUAL(names(pipe),
              (std::vector<std::string>{"from_mysql", "sort", "head"}));
  // A filter that MySQL cannot spell stays, and so does the limit after it.
  pipe = run("from_mysql table=\"t\", host=\"h\"\n"
             "summarize x, n=count()\n"
             "where x.length() > 1\n"
             "head 3");
  CHECK_EQUAL(names(pipe),
              (std::vector<std::string>{"from_mysql", "where", "head"}));
  // A sort that keeps only its first events goes along only with the limit,
  // so an operator that is no stage between them keeps the sort.
  pipe = run("from_mysql table=\"t\", host=\"h\"\n"
             "summarize x, n=count()\n"
             "sort x\n"
             "y = 1\n"
             "head 1");
  CHECK_EQUAL(names(pipe),
              (std::vector<std::string>{"from_mysql", "sort", "set", "head"}));
  // MySQL computes a limit before the summary, too.
  pipe = run("from_mysql table=\"t\", host=\"h\"\n"
             "head 10\n"
             "summarize x, n=count()\n"
             "head 3");
  CHECK_EQUAL(names(pipe), (std::vector<std::string>{"from_mysql"}));
}

TEST("stages after an aggregation that read bindings stay in the pipeline") {
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  // As in the subpipeline of `every`, the optimizer runs before the bindings
  // resolve, and again afterwards. A source spells only constants, so the
  // filter stays, and so does the limit after it.
  auto pipe = compile(ctx, "let $t = 42\n"
                           "from_mysql table=\"t\", host=\"h\"\n"
                           "summarize x, n=count()\n"
                           "where n > $t\n"
                           "head 3");
  REQUIRE(pipe.substitute(substitute_ctx{ctx, nullptr}, false));
  // The optimizer sees the operators of the subpipeline, but not its `let`.
  auto lets = std::move(pipe.lets);
  pipe.lets.clear();
  auto optimized = ir::optimize(std::move(pipe), optimize_ctx());
  optimized.lets = std::move(lets);
  CHECK_EQUAL(names(optimized),
              (std::vector<std::string>{"from_mysql", "where", "head"}));
  auto instantiated = ir::instantiate(std::move(optimized), ctx);
  REQUIRE(instantiated);
  auto again = ir::optimize(std::move(*instantiated), optimize_ctx());
  CHECK_EQUAL(names(again),
              (std::vector<std::string>{"from_mysql", "where", "head"}));
  auto rendered = fmt::format("{:?}", again);
  CHECK(not rendered.contains("dollar_var"));
  CHECK(rendered.contains("42"));
  CHECK(std::move(dh).collect().empty());
}
