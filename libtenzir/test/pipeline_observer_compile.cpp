//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/nova_flag.hpp"
#include "tenzir/package.hpp"
#include "tenzir/pipeline_observer.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/inspection.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/ManualExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <filesystem>
#include <fstream>

namespace tenzir {
namespace {

struct CompileFixture {
  CompileFixture() {
    set_nova_enabled(true);
  }

  ~CompileFixture() {
    set_nova_enabled(previous_mode);
  }

  auto compile(Arc<Source const> source, compile_ctx ctx) -> ir::pipeline {
    ctx.source_map().add_source(source);
    auto parsed = parse(*source, provider.as_session());
    REQUIRE(parsed);
    REQUIRE(resolve_entities(*parsed, ExternPrelude::all_packages_deprecated(),
                             base_ctx{ctx, ctx.reg()}));
    auto compiled = std::move(*parsed).compile(ctx);
    REQUIRE(compiled);
    return std::move(*compiled);
  }

  auto compile(std::string text) -> ir::pipeline {
    auto root = compile_ctx::make_root(context, sources);
    return compile(Source::new_source(std::move(text), "<inspection>", false),
                   root);
  }

  auto lower(ir::pipeline pipe, element_type_tag input = tag_v<nova::Events>)
    -> Arc<ir::Plan const> {
    auto result = ir::make_plan(std::move(pipe), input, context,
                                ir::parallelism::disabled);
    REQUIRE(result);
    return Arc<ir::Plan const>{std::move(*result)};
  }

  bool previous_mode = nova_enabled();
  collecting_diagnostic_handler diagnostics;
  Arc<registry const> reg = make_builtin_registry();
  session_provider provider = session_provider::make(diagnostics, *reg);
  base_ctx context{diagnostics, provider.as_session().reg()};
  SourceMap sources;
};

auto snapshot(GraphSubscription& graph) -> GraphSnapshot {
  auto result = folly::coro::blockingWait(graph.recv());
  REQUIRE(result);
  return **result;
}

} // namespace

TEST("inspection locates both fork_merge output ports at its keyword") {
  auto fixture = CompileFixture{};
  auto plan = fixture.lower(
    fixture.compile("// split\nfork_merge { x = 1 }, { x = 2 }"));
  auto observer = test::InspectionFixture{uuid::null()};
  observer.set_sources(fixture.sources);
  observer.start(PipeId{"root"});
  auto registration = observer.register_plan(plan);
  auto graph = observer.subscribe_graph();
  auto current = snapshot(graph);
  require_eq(current.graphs.size(), 1u);
  auto const& pipeline = *current.graphs.front();
  auto ports = std::vector<size_t>{};
  for (auto const& channel : pipeline.channels) {
    if (channel.from != 0) {
      continue;
    }
    auto const& op = pipeline.operators[channel.from];
    check_eq(op.source.begin, 9u);
    check_eq(op.source.end, 19u);
    check_eq(op.source.source_index, fixture.sources.sources().front()->index);
    ports.push_back(channel.from_port);
  }
  check_eq(ports, (std::vector<size_t>{0, 1}));
}

TEST("inspection source locations resolve through nested package calls") {
  auto fixture = CompileFixture{};
  auto pkg = package{};
  pkg.id = "sourcepkg";
  pkg.name = "Inspection Source Package";
  auto inner = package_operator{};
  auto inner_file = Source::new_source("---\nname: Inner\n---\nx = 1",
                                       "operators/inner.tql", false);
  inner.file_source = inner_file;
  inner.tql_body
    = std::string_view{inner_file->text}.substr(inner_file->text.find("x = 1"));
  pkg.operators.emplace(std::vector<std::string>{"inner"}, std::move(inner));
  auto outer = package_operator{};
  outer.tql_body = "sourcepkg::inner";
  pkg.operators.emplace(std::vector<std::string>{"outer"}, std::move(outer));
  auto module
    = build_package_operator_module(pkg, fixture.diagnostics, *fixture.reg);
  REQUIRE(module);
  auto registry = fixture.provider.as_session().reg().clone();
  registry->add_module(std::string{"packages"}, "sourcepkg",
                       std::move((*module)->defs.at("sourcepkg").mod));
  auto context = base_ctx{fixture.diagnostics, *registry};
  auto root = compile_ctx::make_root(context, fixture.sources);
  auto source = Source::new_source("sourcepkg::outer", "<inspection>", false);
  auto compiled = fixture.compile(source, root);
  auto lowered
    = ir::make_plan(std::move(compiled), tag_v<nova::Events>, context);
  REQUIRE(lowered);
  auto observer = test::InspectionFixture{uuid::null()};
  observer.set_sources(fixture.sources);
  observer.start(PipeId{"root"});
  auto registration
    = observer.register_plan(Arc<ir::Plan const>{std::move(*lowered)});
  auto graph = observer.subscribe_graph();
  auto current = snapshot(graph);
  require(current.sources.is_some());
  require_eq(current.graphs.size(), 1u);
  require_eq(current.graphs.front()->operators.size(), 1u);
  auto const& op = current.graphs.front()->operators.front();
  check_eq(op.name, "set");
  auto const& sources = **current.sources;
  auto definition = sources.source(op.source.source_index);
  require(definition.is_some());
  check_eq(definition->origin, "operators/inner.tql");
  check_eq(op.source.begin, inner_file->text.find("x = 1"));
  check_eq(definition->text.substr(op.source.begin,
                                   op.source.end - op.source.begin),
           "x = 1");
  require(op.source.callsite_index != 0);
  auto inner_call = sources.call_site(op.source.callsite_index);
  require(inner_call.is_some());
  auto outer_source = sources.source(inner_call->source_index);
  require(outer_source.is_some());
  check_eq(outer_source->origin, "<packages/sourcepkg:outer>");
  check_eq(outer_source->text.substr(inner_call->begin,
                                     inner_call->end - inner_call->begin),
           "sourcepkg::inner");
  require(inner_call->callsite_index != 0);
  auto outer_call = sources.call_site(inner_call->callsite_index);
  require(outer_call.is_some());
  check_eq(outer_call->source_index, source->index);
  check_eq(source->text.substr(outer_call->begin,
                               outer_call->end - outer_call->begin),
           "sourcepkg::outer");
  check_eq(outer_call->callsite_index, 0u);
}

TEST("inspection identifies compiled definitions after binding and "
     "optimization") {
  auto fixture = CompileFixture{};
  auto root = compile_ctx::make_root(fixture.context, fixture.sources);
  auto ctx = compile_ctx{root};
  auto scope = ctx.open_scope();
  auto keep = scope.let("keep");
  auto increment = scope.let("increment");
  auto source = Source::new_source("where $keep or value > 0\nhead $increment",
                                   "<nested>", false);
  auto compiled = fixture.compile(source, ctx);
  auto bound = [&](bool keep_value, int64_t increment_value) {
    auto copy = compiled;
    copy.bind(keep, keep_value);
    copy.bind(increment, increment_value);
    return fixture.lower(std::move(copy));
  };
  auto kept = bound(true, 1);
  auto repeated = bound(true, 99);
  auto filtered = bound(false, 1);
  check_eq(kept->definition_location, compiled.definition_location);
  check_eq(filtered->definition_location, compiled.definition_location);
  check_eq(kept->definition_location.source_index, source->index);
  check_eq(kept->operators.size(), 1u);
  check_eq(filtered->operators.size(), 2u);
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto parent = observer.register_plan(
    fixture.lower(fixture.compile("from {value: 1} | discard"), tag_v<void>));
  auto one = observer.register_plan(kept, Ref{*parent}, 0);
  auto two = observer.register_plan(repeated, Ref{*parent}, 0);
  auto other = observer.register_plan(filtered, Ref{*parent}, 0);
  auto graph = Option{observer.subscribe_graph()};
  auto initial = snapshot(*graph);
  check_eq(initial.graphs.size(), 3u);
  auto find_nested = [](GraphSnapshot const& snapshot, size_t operators) {
    auto found = std::ranges::find_if(snapshot.graphs, [&](auto const& graph) {
      return graph->parent and graph->operators.size() == operators;
    });
    REQUIRE(found != snapshot.graphs.end());
    return (*found)->path;
  };
  auto id = find_nested(initial, 1);
  auto filtered_id = find_nested(initial, 2);
  check_ne(id, filtered_id);
  for (auto const& pipeline : initial.graphs) {
    if (pipeline->path == id) {
      require_eq(pipeline->operators.size(), 1u);
      check_eq(pipeline->operators[0].source.source_index, source->index);
    }
  }
  // Reopening after the last subscriber leaves must preserve both identities.
  graph = None{};
  graph = observer.subscribe_graph();
  auto reopened = snapshot(*graph);
  check_eq(reopened.graphs.size(), 3u);
  check_eq(find_nested(reopened, 1), id);
  check_eq(find_nested(reopened, 2), filtered_id);
  // Identical text from another definition remains a different nested target.
  auto other_source = Source::new_source(source->text, "<another>", false);
  auto other_definition = fixture.compile(other_source, ctx);
  other_definition.bind(keep, true);
  other_definition.bind(increment, int64_t{1});
  auto distinct = observer.register_plan(
    fixture.lower(std::move(other_definition)), Ref{*parent}, 0);
  auto updated = observer.subscribe_graph();
  auto current = snapshot(updated);
  check_eq(current.graphs.size(), 4u);
  auto distinct_graph
    = std::ranges::find_if(current.graphs, [&](auto const& pipeline) {
        return pipeline->parent
               and pipeline->operators[0].source.source_index
                     == other_source->index;
      });
  require(distinct_graph != current.graphs.end());
  check_ne(id, (*distinct_graph)->path);
}

TEST("inspection describes surviving operators after filter simplification") {
  auto fixture = CompileFixture{};
  auto compiled = fixture.compile(
    "where value > 0\nwhere false\nwhere value < 100\nwhere true\n"
    "value = value + 1");
  auto lowered = fixture.lower(compiled);
  require(lowered->operators.size() < compiled.operators.size());
  check_eq(lowered->definition_location, compiled.definition_location);
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto registration = observer.register_plan(lowered);
  auto graph = observer.subscribe_graph();
  auto current = snapshot(graph);
  require_eq(current.graphs.size(), 1u);
  auto const& pipeline = *current.graphs[0];
  require_eq(pipeline.operators.size(), lowered->operators.size());
  check_eq(pipeline.channels.size(), lowered->channels.size());
  for (auto i = size_t{0}; i < pipeline.operators.size(); ++i) {
    check_eq(pipeline.operators[i].name,
             lowered->operators[i].op->display_name());
    check_eq(pipeline.operators[i].source,
             lowered->operators[i].op->main_location());
  }
}

TEST("inspection groups generated readers without operator-specific identity") {
  auto fixture = CompileFixture{};
  auto compile_reader = [&](std::string_view text) {
    // This is the parse/compile path used by read_auto: every location is
    // explicitly unknown, rather than belonging to a fresh Source object.
    auto root = compile_ctx::make_root(fixture.context);
    auto parsed = parse(text, location::unknown, fixture.provider.as_session());
    REQUIRE(parsed);
    REQUIRE(resolve_entities(*parsed, ExternPrelude::empty(), fixture.context));
    auto compiled = std::move(*parsed).compile(root);
    REQUIRE(compiled);
    return fixture.lower(std::move(*compiled), tag_v<chunk_ptr>);
  };
  auto json_a = compile_reader("read_json");
  auto json_b = compile_reader("read_json");
  auto lines = compile_reader("read_lines");
  check_eq(json_a->definition_location, location::unknown);
  check_eq(json_b->definition_location, location::unknown);
  for (auto const& op : json_a->operators) {
    check_eq(op.op->main_location(), location::unknown);
  }
  auto observer = test::InspectionFixture{uuid::null()};
  observer.start(PipeId{"root"});
  auto parent = observer.register_plan(
    fixture.lower(fixture.compile("from {value: 1} | discard"), tag_v<void>));
  auto first = observer.register_plan(json_a, Ref{*parent}, 0);
  auto repeated = observer.register_plan(json_b, Ref{*parent}, 0);
  auto different = observer.register_plan(lines, Ref{*parent}, 0);
  auto graph = observer.subscribe_graph();
  auto current = snapshot(graph);
  check_eq(current.graphs.size(), 3u);
  auto readers = std::vector<std::string>{};
  for (auto const& pipeline : current.graphs) {
    if (pipeline->parent) {
      readers.push_back(pipeline->operators[0].name);
    }
  }
  std::ranges::sort(readers);
  check_eq(readers, (std::vector<std::string>{"read_json", "read_lines"}));
}

} // namespace tenzir
