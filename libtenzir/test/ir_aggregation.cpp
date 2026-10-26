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
auto make_aggregation() -> ir::OptimizeAggregation {
  return ir::OptimizeAggregation{
    .columns = {
      {.output = {"k"}, .function = None{}, .input = Path{"k"}},
      {.output = {"n"}, .function = "count", .input = None{}},
      {.output = {"m"}, .function = "max", .input = Path{"v", "w"}},
    },
    .schema_name = "tenzir.summarize",
    .fallback = {},
  };
}

/// Returns the inputs of the columns of `aggregation`, with an empty path for
/// a column without input.
auto inputs(ir::OptimizeAggregation const& aggregation) -> std::vector<Path> {
  auto result = std::vector<Path>{};
  for (auto const& column : aggregation.columns) {
    result.push_back(column.input.value_or(Path{}));
  }
  return result;
}

auto outputs(ir::OptimizeAggregation const& aggregation) -> std::vector<Path> {
  auto result = std::vector<Path>{};
  for (auto const& column : aggregation.columns) {
    result.push_back(column.output);
  }
  return result;
}

/// An operator for testing how the optimizer offers aggregations: a source
/// that may absorb them, an operator that may rewrite them by renaming a field,
/// or a summary that describes one. Every other operator is a barrier.
class Mock final : public ir::Operator {
public:
  enum class Kind { source, rename, barrier, summary };

  Mock(std::string tag, Kind kind, std::string from = {}, std::string to = {})
    : tag_{std::move(tag)},
      kind_{kind},
      from_{std::move(from)},
      to_{std::move(to)} {
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

  auto aggregation() const -> Option<ir::OptimizeAggregation> override {
    if (kind_ != Kind::summary) {
      return None{};
    }
    auto result = make_aggregation();
    result.fallback.operators.push_back(copy());
    return result;
  }

  auto absorb_aggregation(ir::OptimizeAggregation const& aggregation)
    -> bool override {
    if (kind_ != Kind::source or absorbed_) {
      return false;
    }
    absorbed_ = aggregation;
    return true;
  }

  auto rewrite_aggregation(ir::OptimizeAggregation aggregation,
                           ir::OptimizeCtx const& octx) const
    -> Option<ir::OptimizeAggregation> override {
    TENZIR_UNUSED(octx);
    if (kind_ != Kind::rename) {
      return None{};
    }
    return ir::rewrite_aggregation(
      std::move(aggregation), [&](ast::field_path const& field) {
        if (field.path().size() == 1 and field.path()[0].id.name == to_) {
          return Option<ast::expression>{parse(from_)};
        }
        return Option<ast::expression>{field.inner()};
      });
  }

  auto absorbed() const -> Option<ir::OptimizeAggregation> const& {
    return absorbed_;
  }

private:
  std::string tag_;
  Kind kind_;
  std::string from_;
  std::string to_;
  Option<ir::OptimizeAggregation> absorbed_;
};

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

auto absorbed(ir::pipeline const& pipe, size_t index)
  -> Option<ir::OptimizeAggregation> const& {
  auto const* mock = dynamic_cast<Mock const*>(&*pipe.operators.at(index));
  REQUIRE(mock);
  return mock->absorbed();
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

/// Rewrites `make_aggregation()` through a `set` with `assignments`.
auto through_set(std::vector<ast::assignment> assignments)
  -> Option<ir::OptimizeAggregation> {
  return ir::SetIr{std::move(assignments)}.rewrite_aggregation(
    make_aggregation(), optimize_ctx());
}

} // namespace

TEST("aggregation rewrite maps inputs and keeps outputs") {
  auto rewritten = ir::rewrite_aggregation(
    make_aggregation(), [](ast::field_path const& field) {
      auto name = field.path().front().id.name;
      if (name == "k") {
        return Option<ast::expression>{parse("a.b")};
      }
      return Option<ast::expression>{field.inner()};
    });
  REQUIRE(rewritten);
  CHECK_EQUAL(inputs(*rewritten),
              (std::vector<Path>{{"a", "b"}, {}, {"v", "w"}}));
  CHECK_EQUAL(outputs(*rewritten), outputs(make_aggregation()));
  CHECK_EQUAL(rewritten->schema_name, "tenzir.summarize");
}

TEST("aggregation rewrite requires plain fields") {
  auto rewrite_k = [](std::string_view replacement) {
    return ir::rewrite_aggregation(
      make_aggregation(), [&](ast::field_path const& field) {
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
  CHECK(
    not ir::rewrite_aggregation(make_aggregation(), [](ast::field_path const&) {
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
  CHECK_EQUAL(inputs(*unrelated), inputs(make_aggregation()));
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
  CHECK_EQUAL(inputs(*selected), inputs(make_aggregation()));
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

TEST("optimizer offers an aggregation through operators that rewrite it") {
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
  auto const& aggregation = absorbed(pipe, 0);
  REQUIRE(aggregation);
  // The input maps back through both renames, and the outputs stay.
  CHECK_EQUAL(inputs(*aggregation), (std::vector<Path>{{"a"}, {}, {"v", "w"}}));
  CHECK_EQUAL(outputs(*aggregation), outputs(make_aggregation()));
  // The fallback replays the skipped operators in their original order.
  CHECK_EQUAL(names(aggregation->fallback),
              (std::vector<std::string>{"first", "second", "summary"}));
  // Another pass finds nothing left to absorb.
  auto again = optimized(std::move(pipe));
  CHECK_EQUAL(names(again), (std::vector<std::string>{"source", "after"}));
  REQUIRE(absorbed(again, 0));
  CHECK_EQUAL(names(absorbed(again, 0)->fallback),
              (std::vector<std::string>{"first", "second", "summary"}));
}

TEST("optimizer keeps the pipeline when no operator absorbs an aggregation") {
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
  CHECK(not absorbed(blocked, 0));
  // Without a source, the rewrites leave every operator in place.
  auto sourceless = optimized(make_pipeline({
    Mock{"rename", rename, "a", "k"},
    Mock{"summary", summary},
  }));
  CHECK_EQUAL(names(sourceless),
              (std::vector<std::string>{"rename", "summary"}));
}

TEST("optimizer offers each aggregation to the closest absorber") {
  using enum Mock::Kind;
  // The first source takes over the first summary. The second summary then
  // follows a source that already emits an aggregation, which declines.
  auto pipe = optimized(make_pipeline({
    Mock{"source", source},
    Mock{"first", summary},
    Mock{"second", summary},
  }));
  CHECK_EQUAL(names(pipe), (std::vector<std::string>{"source", "second"}));
  REQUIRE(absorbed(pipe, 0));
  CHECK_EQUAL(names(absorbed(pipe, 0)->fallback),
              (std::vector<std::string>{"first"}));
}

TEST("absorbed fallback receives the bindings of its pipeline") {
  // Only the Nova operator of `from_mysql` computes aggregations.
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  // A `set` that reads a binding never qualifies for the rewrite, because the
  // unresolved binding could fail. This simulates an operator that does, which
  // the source absorbs before the pipeline is instantiated, as in `every`.
  auto pipe = compile(ctx, "let $v = 42\n"
                           "from_mysql table=\"t\", host=\"h\"\n"
                           "x = $v\n"
                           "summarize x, n=count()");
  REQUIRE_EQUAL(pipe.operators.size(), size_t{3});
  // As for the subpipeline of `every`, constant arguments resolve before
  // instantiation, but bindings do not.
  REQUIRE(pipe.substitute(substitute_ctx{ctx, nullptr}, false));
  CHECK(not pipe.operators[1]->rewrite_aggregation(
    *pipe.operators[2]->aggregation(), optimize_ctx()));
  auto aggregation = pipe.operators[2]->aggregation();
  REQUIRE(aggregation);
  aggregation->fallback.operators.insert(
    aggregation->fallback.operators.begin(), pipe.operators[1]->copy());
  REQUIRE(pipe.operators[0]->absorb_aggregation(*aggregation));
  pipe.operators.resize(1);
  CHECK(fmt::format("{:?}", pipe).contains("dollar_var"));
  auto instantiated = ir::instantiate(std::move(pipe), ctx);
  REQUIRE(instantiated);
  auto rendered = fmt::format("{:?}", *instantiated);
  CHECK(not rendered.contains("dollar_var"));
  CHECK(rendered.contains("42"));
  CHECK(std::move(dh).collect().empty());
}

TEST("generic operators decline aggregations after absorbing events") {
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
  // A limit counts the events before the summary.
  CHECK_EQUAL(
    run("from_mysql table=\"t\", host=\"h\"\n"
        "head 3\n"
        "drop y\n"
        "summarize x, n=count()"),
    (std::vector<std::string>{"from_mysql", "head", "drop", "summarize"}));
}
