//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/registry.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/CurrentExecutor.h>

#include <cstdint>
#include <deque>
#include <string>
#include <utility>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova;

namespace {
/// `Events` over `data` with every row selected and no origin metadata.
auto make_events(Array<Record> data) -> Events {
  auto const length = data.length();
  return Events{std::move(data), storage::BitMap{length, true},
                Events::Meta::make_empty(length)};
}

struct TestArgs {
  ValueArgument x;
  located<std::int64_t> n;
  Option<located<std::string>> s;
  bool flag = false;
  located<std::uint64_t> count = {0, location::unknown};
  location call;
};

class TestFunction final {
public:
  static auto eval(TestArgs const& args, EvalFrame frame) -> Array<Data> {
    diagnostic::warning("test_fn was evaluated").primary(args.call).emit(frame);
    return frame.null();
  }
};

auto describe_test() -> FunctionDescriber<TestArgs, TestFunction> {
  auto d = FunctionDescriber<TestArgs, TestFunction>{};
  d.positional("x", &TestArgs::x, "any");
  d.positional("n", &TestArgs::n);
  d.named("s", &TestArgs::s);
  d.named("flag", &TestArgs::flag);
  d.named_optional("count", &TestArgs::count);
  d.call_location(&TestArgs::call);
  return d;
}

struct LambdaArgs {
  LambdaArgument fn;
};

/// Applies the lambda to an all-null subject.
class LambdaFunction final {
public:
  static auto eval(LambdaArgs const& args, EvalFrame frame) -> Array<Data> {
    auto subject = MaskedArray<Array<Data>>{frame.null(), frame.mask()};
    if (auto const* input = frame.input()) {
      return frame.eval(args.fn, std::move(subject), *input);
    }
    return frame.eval(args.fn, std::move(subject));
  }
};

auto describe_lambda() -> FunctionDescriber<LambdaArgs, LambdaFunction> {
  auto d = FunctionDescriber<LambdaArgs, LambdaFunction>{};
  d.positional("fn", &LambdaArgs::fn, "any => any");
  return d;
}

struct VariadicArgs {
  std::vector<located<std::int64_t>> xs;
  located<std::string> mode;
  Option<location> flag;
  Option<ast::field_path> sel;
  Option<Data> any;
};

class VariadicFunction final {
public:
  static auto eval(VariadicArgs const&, EvalFrame frame) -> Array<Data> {
    return frame.null();
  }
};

auto describe_variadic() -> FunctionDescriber<VariadicArgs, VariadicFunction> {
  auto d = FunctionDescriber<VariadicArgs, VariadicFunction>{};
  d.variadic("xs", &VariadicArgs::xs);
  d.named(std::vector<std::string>{"mode", "m"}, &VariadicArgs::mode);
  d.named("flag", &VariadicArgs::flag);
  d.named("sel", &VariadicArgs::sel);
  d.named("any", &VariadicArgs::any);
  return d;
}

/// A positional argument in front of a variadic one, to pin the arity check
/// for the shapes that `add_variadic` allows.
struct HeadVariadicArgs {
  located<std::int64_t> head;
  std::vector<located<std::int64_t>> rest;
};

class HeadVariadicFunction final {
public:
  static auto eval(HeadVariadicArgs const&, EvalFrame frame) -> Array<Data> {
    return frame.null();
  }
};

auto describe_head_variadic()
  -> FunctionDescriber<HeadVariadicArgs, HeadVariadicFunction> {
  auto d = FunctionDescriber<HeadVariadicArgs, HeadVariadicFunction>{};
  d.positional("head", &HeadVariadicArgs::head);
  d.variadic("rest", &HeadVariadicArgs::rest);
  return d;
}

auto describe_head_optional_variadic()
  -> FunctionDescriber<HeadVariadicArgs, HeadVariadicFunction> {
  auto d = FunctionDescriber<HeadVariadicArgs, HeadVariadicFunction>{};
  d.positional("head", &HeadVariadicArgs::head);
  d.optional_variadic("rest", &HeadVariadicArgs::rest);
  return d;
}

/// Optional expression and lambda arguments, which stay default constructed
/// when the caller omits them.
struct OptionalExprArgs {
  ValueArgument x;
  Option<ValueArgument> extra;
  LambdaArgument fn;
};

class OptionalExprFunction final {
public:
  static auto eval(OptionalExprArgs const&, EvalFrame frame) -> Array<Data> {
    return frame.null();
  }
};

auto describe_optional_expr()
  -> FunctionDescriber<OptionalExprArgs, OptionalExprFunction> {
  auto d = FunctionDescriber<OptionalExprArgs, OptionalExprFunction>{};
  d.positional("x", &OptionalExprArgs::x, "any");
  d.optional_positional("fn", &OptionalExprArgs::fn);
  d.named_optional("extra", &OptionalExprArgs::extra);
  return d;
}

/// Evaluates its argument itself instead of receiving it as a `ValueArgument`,
/// for all rows or for none of them.
struct LazyArgs {
  LazyArgument x;
  bool all = false;
};

class LazyFunction final {
public:
  static auto eval(LazyArgs const& args, EvalFrame frame) -> Array<Data> {
    if (args.all) {
      return frame.eval(args.x).data;
    }
    return frame.narrow(storage::BitMap{frame.length(), false})
      .eval(args.x)
      .data;
  }
};

auto describe_lazy() -> FunctionDescriber<LazyArgs, LazyFunction> {
  auto d = FunctionDescriber<LazyArgs, LazyFunction>{};
  d.positional("x", &LazyArgs::x, "any");
  d.named("all", &LazyArgs::all);
  return d;
}

struct ResolvedArgs {
  Secret value;
  Option<located<Secret>> optional;
  std::vector<Secret> rest;
};

/// Sums the resolved sizes at evaluation time.
class ResolvedFunction {
public:
  static auto eval(ResolvedArgs const& args, EvalFrame frame) -> Array<Data> {
    auto size = std::int64_t{0};
    size += static_cast<std::int64_t>(args.value.data.size());
    for (auto const& value : args.rest) {
      size += static_cast<std::int64_t>(value.data.size());
    }
    if (args.optional) {
      size += static_cast<std::int64_t>(args.optional->inner.data.size());
    }
    return repeat(Data{size}, frame.length());
  }
};

auto describe_resolved() -> FunctionDescriber<ResolvedArgs, ResolvedFunction> {
  auto d = FunctionDescriber<ResolvedArgs, ResolvedFunction>{};
  d.positional("value", &ResolvedArgs::value);
  d.optional_variadic("rest", &ResolvedArgs::rest);
  d.named("optional", &ResolvedArgs::optional);
  return d;
}

/// Counts resolutions and resolves every secret to the same bytes, or fails.
class SecretCtx final : public tenzir::test::NovaOpCtx {
public:
  using NovaOpCtx::NovaOpCtx;

  auto resolve_secrets(std::vector<secret_request> requests)
    -> Task<failure_or<void>> override {
    co_await folly::coro::co_reschedule_on_current_executor;
    for (auto& request : requests) {
      ++resolutions;
      if (fail) {
        diagnostic::error("test secret resolution failed")
          .primary(request.location)
          .emit(dh());
        co_return failure::promise();
      }
      auto const text = std::string_view{"resolved"};
      auto const bytes = std::as_bytes(std::span{text.data(), text.size()});
      CO_TRY(request.callback(
        resolved_secret_value{ecc::cleansing_blob{bytes.begin(), bytes.end()},
                              request.secret.is_all_literal()}));
    }
    co_return failure_or<void>{};
  }

  int resolutions = 0;
  bool fail = false;
};

/// A nova function plugin that is described by a fixed describer.
template <class Describer>
class TestPlugin final : public virtual FunctionPlugin {
public:
  TestPlugin(std::string name, Describer (*describe)())
    : name_{std::move(name)}, describe_{describe} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> FunctionDescription override {
    return std::move(describe_()).finish();
  }

  auto make_function(function_invocation, session) const
    -> failure_or<function_ptr> override {
    panic("the legacy function path is not used in this test");
  }

private:
  std::string name_;
  Describer (*describe_)();
};

/// The plugins that calls in these tests can refer to.
class TestRegistry {
public:
  TestRegistry() {
    add(test_fn_);
    add(lambda_fn_);
    add(variadic_fn_);
    add(lazy_fn_);
    add(resolved_fn_);
  }

  explicit(false) operator const registry&() const {
    return reg_;
  }

private:
  auto add(const function_plugin& plugin) -> void {
    reg_.add(std::string{entity_pkg_std}, plugin.name(),
             std::reference_wrapper<const function_plugin>{plugin});
  }

  TestPlugin<FunctionDescriber<TestArgs, TestFunction>> test_fn_{
    "test_fn", &describe_test};
  TestPlugin<FunctionDescriber<LambdaArgs, LambdaFunction>> lambda_fn_{
    "lambda_fn", &describe_lambda};
  TestPlugin<FunctionDescriber<VariadicArgs, VariadicFunction>> variadic_fn_{
    "var_fn", &describe_variadic};
  TestPlugin<FunctionDescriber<LazyArgs, LazyFunction>> lazy_fn_{
    "lazy_fn", &describe_lazy};
  TestPlugin<FunctionDescriber<ResolvedArgs, ResolvedFunction>> resolved_fn_{
    "resolved_fn", &describe_resolved};
  registry reg_;
};

auto constant(data value) -> ast::expression {
  return ast::expression{
    ast::constant::make(located<data>{std::move(value), location::unknown})};
}

auto root_field(std::string name) -> ast::expression {
  return ast::expression{
    ast::root_field{ast::identifier{std::move(name), location::unknown}}};
}

auto named(std::string name, ast::expression value) -> ast::expression {
  return ast::expression{ast::assignment{root_field(std::move(name)),
                                         location::unknown, std::move(value)}};
}

auto lambda(std::string parameter, ast::expression body) -> ast::expression {
  return ast::expression{
    ast::lambda_expr{ast::identifier{std::move(parameter), location::unknown},
                     location::unknown, std::move(body)}};
}

/// A call to `name` that resolves through `TestRegistry`.
auto call(std::string name, std::vector<ast::expression> args)
  -> ast::function_call {
  auto result = ast::function_call{
    ast::entity{{ast::identifier{name, location::unknown}}}, std::move(args),
    location::unknown, false};
  result.fn.ref = entity_path{
    std::string{entity_pkg_std}, {std::move(name)}, entity_ns::fn};
  return result;
}

auto call(std::vector<ast::expression> args) -> ast::function_call {
  return call("test_fn", std::move(args));
}

/// Instantiates `desc` for `call`, keeping the call alive for the rest of the
/// process: a lambda argument borrows its AST node from the call instead of
/// taking ownership, because in production the enclosing `Evaluator` owns the
/// whole expression.
auto instantiate(FunctionDescription const& desc, ast::function_call call,
                 InstantiateCtx ctx) -> failure_or<nova::_::CallSite> {
  static auto arena = std::deque<ast::function_call>{};
  auto& stored = arena.emplace_back(std::move(call));
  // The plugin supplies the name in production; here the call names it.
  TRY(auto instantiation,
      desc.instantiate(stored.fn.path.back().name, stored, ctx));
  return std::move(instantiation.call_site);
}

/// The prepared arguments of an instantiated call. Only the constants are
/// meaningful outside a call: values are filled per call.
template <class Args>
auto args_of(const nova::_::CallSite& call_site) -> const Args& {
  return call_site.args<Args>();
}

auto errors(const std::vector<diagnostic>& diags) -> std::vector<diagnostic> {
  auto result = std::vector<diagnostic>{};
  for (const auto& diag : diags) {
    if (diag.severity == severity::error) {
      result.push_back(diag);
    }
  }
  return result;
}

auto first_error(std::vector<diagnostic> diags) -> std::string {
  auto errs = errors(diags);
  return errs.empty() ? std::string{} : errs.front().message;
}

/// The message of the first note of the given kind, or empty if there is none.
auto note(const diagnostic& diag, diagnostic_note_kind kind) -> std::string {
  for (const auto& note : diag.notes) {
    if (note.kind == kind) {
      return note.message;
    }
  }
  return {};
}

} // namespace

TEST("instantiate stores expressions and constant evaluates the rest") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  CHECK_EQUAL(desc.usage("test_fn"),
              "test_fn(x:any, n:int, [s=string, flag=bool, "
              "count=int])");
  auto result = instantiate(desc,
                            call({root_field("x"), constant(std::int64_t{3}),
                                  named("s", constant(std::string{"abc"})),
                                  named("flag", constant(true)),
                                  named("count", constant(std::int64_t{5}))}),
                            InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(std::move(dh).collect().empty());
  const auto& args = args_of<TestArgs>(*result);
  CHECK_EQUAL(args.x.source, location::unknown);
  CHECK_EQUAL(args.n.inner, std::int64_t{3});
  REQUIRE(args.s);
  CHECK_EQUAL(args.s->inner, "abc");
  CHECK(args.flag);
  CHECK_EQUAL(args.count.inner, std::uint64_t{5});
}

TEST("instantiate keeps defaults for omitted optional arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  auto result
    = instantiate(desc, call({root_field("x"), constant(std::int64_t{3})}),
                  InstantiateCtx{dh, reg});
  REQUIRE(result);
  const auto& args = args_of<TestArgs>(*result);
  CHECK(not args.s);
  CHECK(not args.flag);
  CHECK_EQUAL(args.count.inner, std::uint64_t{0});
}

TEST("instantiate reports missing positional arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  CHECK(
    not instantiate(desc, call({root_field("x")}), InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected exactly 2 positional arguments");
}

TEST("instantiate reports unknown and duplicate named arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::int64_t{3}),
                              named("flg", constant(true))}),
                        InstantiateCtx{dh, reg}));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(errors(diags).size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "named argument `flg` does not exist");
  CHECK_EQUAL(note(diags.front(), diagnostic_note_kind::hint),
              "did you mean `flag`?");
  dh = collecting_diagnostic_handler{};
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::int64_t{3}),
                              named("s", constant(std::string{"a"})),
                              named("s", constant(std::string{"b"}))}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "duplicate named argument `s`");
}

TEST("instantiate rejects constants of the wrong type") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::string{"three"})}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected argument of type `int`, but got `string`");
  dh = collecting_diagnostic_handler{};
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::int64_t{3}),
                              named("count", constant(std::int64_t{-1}))}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected positive integer, got `-1`");
}

TEST("instantiate rejects non-constant values for constant arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  CHECK(not instantiate(desc, call({root_field("x"), root_field("y")}),
                        InstantiateCtx{dh, reg}));
  auto diags = errors(std::move(dh).collect());
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "expected a constant expression");
  // Errors from constant evaluating an argument carry the function's usage.
  CHECK_EQUAL(note(diags.front(), diagnostic_note_kind::usage),
              desc.usage("test_fn"));
  CHECK_EQUAL(note(diags.front(), diagnostic_note_kind::docs),
              "https://tenzir.com/docs/reference/functions/test_fn");
}

TEST("instantiate requires a lambda for lambda arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_lambda()).finish();
  CHECK_EQUAL(desc.usage("lambda_fn"), "lambda_fn(fn:any => any)");
  CHECK(
    not instantiate(desc, call({root_field("x")}), InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()), "expected a lambda");
  auto result = instantiate(desc, call({lambda("a", root_field("a"))}),
                            InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(args_of<LambdaArgs>(*result).fn.captures().empty());
  // Fields the body does not bind itself are captures of the input.
  result = instantiate(desc, call({lambda("a", root_field("y"))}),
                       InstantiateCtx{dh, reg});
  REQUIRE(result);
  const auto& captures = args_of<LambdaArgs>(*result).fn.captures();
  REQUIRE_EQUAL(captures.size(), size_t{1});
  CHECK_EQUAL(captures[0].name, "y");
}

TEST("instantiate rejects lambdas for expression arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  CHECK(not instantiate(
    desc, call({lambda("a", root_field("a")), constant(std::int64_t{1})}),
    InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected an expression, got a lambda");
}

TEST("validators can modify arguments and fail the instantiation") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto d = describe_test();
  d.validate([](TestArgs& args, diagnostic_handler& vdh) -> failure_or<void> {
    if (args.n.inner == 0) {
      diagnostic::error("`n` must not be zero").primary(args.n).emit(vdh);
      return failure::promise();
    }
    args.flag = args.n.inner == 1;
    return {};
  });
  auto desc = std::move(d).finish();
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::int64_t{0})}),
                        InstantiateCtx{dh, reg}));
  auto diags = errors(std::move(dh).collect());
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "`n` must not be zero");
  CHECK_EQUAL(note(diags.front(), diagnostic_note_kind::usage),
              desc.usage("test_fn"));
  auto result
    = instantiate(desc, call({root_field("x"), constant(std::int64_t{1})}),
                  InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(args_of<TestArgs>(*result).flag);
}

TEST("validator errors fail the instantiation even without a failure") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto d = describe_test();
  d.validate([](TestArgs& args, diagnostic_handler& vdh) -> failure_or<void> {
    diagnostic::error("always wrong").primary(args.call).emit(vdh);
    return {};
  });
  auto desc = std::move(d).finish();
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::int64_t{1})}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()), "always wrong");
}

TEST("a required variadic after a positional still requires a value") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_head_variadic()).finish();
  CHECK_EQUAL(desc.usage("head_var_fn"), "head_var_fn(head:int, rest:int...)");
  CHECK(not instantiate(desc, call("head_var_fn", {constant(std::int64_t{1})}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected at least 2 positional arguments");
  dh = collecting_diagnostic_handler{};
  auto result = instantiate(
    desc,
    call("head_var_fn", {constant(std::int64_t{1}), constant(std::int64_t{2})}),
    InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(std::move(dh).collect().empty());
  CHECK_EQUAL(args_of<HeadVariadicArgs>(*result).rest.size(), size_t{1});
}

TEST("an optional variadic after a positional accepts zero values") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_head_optional_variadic()).finish();
  auto result
    = instantiate(desc, call("head_var_fn", {constant(std::int64_t{1})}),
                  InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(std::move(dh).collect().empty());
  const auto& args = args_of<HeadVariadicArgs>(*result);
  CHECK_EQUAL(args.head.inner, std::int64_t{1});
  CHECK(args.rest.empty());
}

TEST("instantiate reports every bad argument value") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  // Both `n` and `count` have the wrong type; preparation must not stop at
  // the first one.
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::string{"three"}),
                              named("count", constant(std::int64_t{-1}))}),
                        InstantiateCtx{dh, reg}));
  auto diags = errors(std::move(dh).collect());
  REQUIRE_EQUAL(diags.size(), size_t{2});
  CHECK_EQUAL(diags[0].message,
              "expected argument of type `int`, but got `string`");
  CHECK_EQUAL(diags[1].message, "expected positive integer, got `-1`");
}

TEST("the validator does not run when preparing an argument failed") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto d = describe_test();
  d.validate([](TestArgs& args, diagnostic_handler& vdh) -> failure_or<void> {
    diagnostic::error("validator ran").primary(args.call).emit(vdh);
    return {};
  });
  auto desc = std::move(d).finish();
  // A failed preparation leaves members default constructed, so the validator
  // must not observe them.
  CHECK(not instantiate(
    desc, call({root_field("x"), constant(std::string{"not an int"})}),
    InstantiateCtx{dh, reg}));
  auto diags = errors(std::move(dh).collect());
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message,
              "expected argument of type `int`, but got `string`");
}

TEST("a failed variadic element does not leave a partial vector visible") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_variadic()).finish();
  // `prepare_element` appends, so a failing element would otherwise leave the
  // vector holding only the elements that happened to succeed.
  CHECK(not instantiate(
    desc,
    call("var_fn",
         {constant(std::int64_t{1}), constant(std::string{"bad"}),
          constant(std::int64_t{3}), named("m", constant(std::string{}))}),
    InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected argument of type `int`, but got `string`");
}

TEST("optional expression and lambda arguments may be omitted") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_optional_expr()).finish();
  CHECK_EQUAL(desc.usage("opt_expr_fn"),
              "opt_expr_fn(x:any, [fn:any -> any, extra=any])");
  auto result = instantiate(desc, call("opt_expr_fn", {root_field("x")}),
                            InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(std::move(dh).collect().empty());
  dh = collecting_diagnostic_handler{};
  result = instantiate(desc,
                       call("opt_expr_fn",
                            {root_field("x"), lambda("y", root_field("y")),
                             named("extra", root_field("z"))}),
                       InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(std::move(dh).collect().empty());
}

TEST("instantiation rejects undeclared positional arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_test()).finish();
  CHECK(not instantiate(desc,
                        call({root_field("x"), constant(std::int64_t{1}),
                              constant(std::int64_t{2})}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "too many positional arguments");
}

TEST("variadic, aliased, selector, and flag arguments") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto desc = std::move(describe_variadic()).finish();
  CHECK_EQUAL(desc.usage("var_fn"),
              "var_fn(xs:int..., mode|m=string, [flag=bool, sel=field, "
              "any=any])");
  auto result = instantiate(
    desc,
    call("var_fn",
         {constant(std::int64_t{1}), constant(std::int64_t{2}),
          constant(std::int64_t{3}), named("m", constant(std::string{"fast"})),
          named("flag", constant(true)), named("sel", root_field("foo")),
          named("any", constant(std::string{"anything"}))}),
    InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(std::move(dh).collect().empty());
  const auto& args = args_of<VariadicArgs>(*result);
  REQUIRE_EQUAL(args.xs.size(), size_t{3});
  CHECK_EQUAL(args.xs[0].inner, std::int64_t{1});
  CHECK_EQUAL(args.xs[2].inner, std::int64_t{3});
  CHECK_EQUAL(args.mode.inner, "fast");
  CHECK(args.flag);
  REQUIRE(args.sel);
  REQUIRE_EQUAL(args.sel->path().size(), size_t{1});
  CHECK_EQUAL(args.sel->path()[0].id.name, "foo");
  REQUIRE(args.any);
  auto const* any = try_as<std::string>(&*args.any);
  REQUIRE(any);
  CHECK_EQUAL(*any, "anything");
  dh = collecting_diagnostic_handler{};
  CHECK(not instantiate(
    desc, call("var_fn", {named("mode", constant(std::string{"fast"}))}),
    InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected at least 1 positional argument");
  dh = collecting_diagnostic_handler{};
  CHECK(not instantiate(desc, call("var_fn", {constant(std::int64_t{1})}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "required argument `mode` was not provided");
  dh = collecting_diagnostic_handler{};
  CHECK(not instantiate(desc,
                        call("var_fn", {constant(std::int64_t{1}),
                                        named("m", constant(std::string{})),
                                        named("sel", constant(true))}),
                        InstantiateCtx{dh, reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()), "expected a selector");
  dh = collecting_diagnostic_handler{};
  result = instantiate(desc,
                       call("var_fn", {constant(std::int64_t{1}),
                                       named("m", constant(std::string{})),
                                       named("flag", constant(false))}),
                       InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(not args_of<VariadicArgs>(*result).flag);
}

TEST("evaluation diagnostics go to the evaluation handler") {
  auto reg = TestRegistry{};
  auto instantiate_dh = collecting_diagnostic_handler{};
  auto evaluator = tenzir::test::make_evaluator(
    ast::expression{
      call({constant(std::int64_t{2}), constant(std::int64_t{1})})},
    instantiate_dh, reg);
  REQUIRE(evaluator);
  CHECK(std::move(instantiate_dh).collect().empty());
  // The handler used for instantiation is gone; evaluation uses another one.
  auto eval_dh = collecting_diagnostic_handler{};
  auto result = evaluator->eval(EvalCtx{eval_dh});
  CHECK_EQUAL(result.length(), 1);
  auto diags = std::move(eval_dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "test_fn was evaluated");
  CHECK(diags.front().severity == severity::warning);
}

TEST("nested functions are instantiated with their own usage") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto inner = std::move(describe_lambda()).finish();
  // The inner call lacks its argument. Nested calls are prepared by the
  // evaluator, together with the expression they belong to.
  auto evaluator = tenzir::test::make_evaluator(
    ast::expression{
      call({root_field("x"), ast::expression{call("lambda_fn", {})}})},
    dh, reg);
  CHECK(not evaluator);
  auto diags = errors(std::move(dh).collect());
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "expected exactly 1 positional argument");
  CHECK_EQUAL(note(diags.front(), diagnostic_note_kind::usage),
              inner.usage("lambda_fn"));
  CHECK_EQUAL(note(diags.front(), diagnostic_note_kind::docs),
              "https://tenzir.com/docs/reference/functions/lambda_fn");
  // After the inner call, errors of the outer call carry its usage again.
  dh = collecting_diagnostic_handler{};
  auto variadic = std::move(describe_variadic()).finish();
  auto result = instantiate(
    variadic,
    call("var_fn",
         {constant(std::int64_t{1}), named("m", constant(std::string{})),
          named("any", ast::expression{call({constant(std::int64_t{5}),
                                             constant(std::int64_t{1})})}),
          named("flag", constant(std::int64_t{1}))}),
    InstantiateCtx{dh, reg});
  CHECK(not result);
  diags = errors(std::move(dh).collect());
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message,
              "expected argument of type `bool`, but got `int`");
  CHECK_EQUAL(note(diags.front(), diagnostic_note_kind::usage),
              variadic.usage("var_fn"));
}

TEST("expression arguments are evaluated before the function runs") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto evaluator = tenzir::test::make_evaluator(
    ast::expression{call({ast::expression{call({constant(std::int64_t{1}),
                                                constant(std::int64_t{1})})},
                          constant(std::int64_t{2})})},
    dh, reg);
  REQUIRE(evaluator);
  CHECK(std::move(dh).collect().empty());
  auto eval_dh = collecting_diagnostic_handler{};
  auto events = make_events(Array<Record>::make_empty(2));
  auto value = evaluator->eval(events, EvalCtx{eval_dh});
  CHECK_EQUAL(value.length(), 2);
  // Both functions ran: the framework evaluates the outer call's argument,
  // which is the inner call, before the outer function itself.
  auto diags = std::move(eval_dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{2});
  CHECK_EQUAL(diags.front().message, "test_fn was evaluated");
  CHECK_EQUAL(diags.back().message, "test_fn was evaluated");
}

TEST("lazy arguments are evaluated by the function, for the rows it picks") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  // The argument is a call that warns whenever it is evaluated.
  auto prepare = [&](bool all) {
    return tenzir::test::make_evaluator(
      ast::expression{
        call("lazy_fn", {ast::expression{call({constant(std::int64_t{1}),
                                               constant(std::int64_t{1})})},
                         named("all", constant(all))})},
      dh, reg);
  };
  auto lazy = prepare(false);
  REQUIRE(lazy);
  auto eval_dh = collecting_diagnostic_handler{};
  lazy->eval(EvalCtx{eval_dh});
  // No rows were requested, so the argument never ran.
  CHECK(std::move(eval_dh).collect().empty());
  auto eager = prepare(true);
  REQUIRE(eager);
  eval_dh = collecting_diagnostic_handler{};
  eager->eval(EvalCtx{eval_dh});
  auto diags = std::move(eval_dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "test_fn was evaluated");
}

TEST("lambda bodies are prepared by the enclosing evaluator") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  // A nested call inside the lambda body is checked while preparing, because
  // its call site belongs to the enclosing expression.
  CHECK(not tenzir::test::make_evaluator(
    ast::expression{call(
      "lambda_fn", {lambda("a", ast::expression{call({root_field("a")})})})},
    dh, reg));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected exactly 2 positional arguments");
  dh = collecting_diagnostic_handler{};
  auto evaluator = tenzir::test::make_evaluator(
    ast::expression{
      call("lambda_fn",
           {lambda("a", ast::expression{call(
                          {root_field("a"), constant(std::int64_t{1})})})})},
    dh, reg);
  REQUIRE(evaluator);
  CHECK(std::move(dh).collect().empty());
  // Applying the lambda evaluates the body in the same run, so the body's
  // function instance reports to the evaluation handler.
  auto eval_dh = collecting_diagnostic_handler{};
  auto value = evaluator->eval(EvalCtx{eval_dh});
  CHECK_EQUAL(value.length(), 1);
  auto diags = std::move(eval_dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "test_fn was evaluated");
}

TEST("lambda captures are rejected when there is no input") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto evaluator = tenzir::test::make_evaluator(
    ast::expression{call("lambda_fn", {lambda("a", root_field("y"))})}, dh,
    reg);
  REQUIRE(evaluator);
  CHECK(std::move(dh).collect().empty());
  auto eval_dh = collecting_diagnostic_handler{};
  evaluator->eval(EvalCtx{eval_dh});
  CHECK_EQUAL(first_error(std::move(eval_dh).collect()),
              "expected a constant expression");
}

TEST("speculative constant evaluation of function calls stays silent") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  // Instantiation fails, so nothing is reported.
  auto failed
    = try_const_eval(ast::expression{call({constant(std::int64_t{1})})},
                     InstantiateCtx{dh, reg});
  CHECK(not failed);
  CHECK(std::move(dh).collect().empty());
  // Evaluation succeeds, so its diagnostics are forwarded.
  dh = collecting_diagnostic_handler{};
  auto result
    = try_const_eval(ast::expression{call(
                       {constant(std::int64_t{1}), constant(std::int64_t{2})})},
                     InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK(result->index() == 0);
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().message, "test_fn was evaluated");
}

TEST("secret arguments resolve during construction and are reused") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  auto expression = ast::expression{
    call("resolved_fn",
         {call("secret", {constant("key")}), constant(std::string{"literal"}),
          named("optional", constant(std::string{"literal"}))})};
  auto evaluator
    = folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx));
  REQUIRE(evaluator);
  CHECK_EQUAL(ctx.resolutions, 1);
  auto events = make_events(Array<Record>::make_empty(4));
  auto result = evaluator->eval(events, EvalCtx{dh});
  REQUIRE(is<Array<Int>>(result));
  CHECK_EQUAL(*result.try_as<Int>()->get(3), 22);
  auto moved = std::move(*evaluator);
  result = moved.eval(events, EvalCtx{dh});
  CHECK_EQUAL(*result.try_as<Int>()->get(0), 22);
  CHECK_EQUAL(ctx.resolutions, 1);
  CHECK(std::move(dh).collect().empty());
}

TEST("failed secret resolution and nonconstant arguments abort construction") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  ctx.fail = true;
  auto expression
    = ast::expression{call("resolved_fn", {call("secret", {constant("key")})})};
  CHECK(
    not folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx)));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "test secret resolution failed");
  dh = collecting_diagnostic_handler{};
  ctx.fail = false;
  expression = ast::expression{call("resolved_fn", {root_field("value")})};
  CHECK(
    not folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx)));
  CHECK_EQUAL(ctx.resolutions, 1);
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected a constant expression");
}

TEST("secret arguments in lambda bodies resolve during construction") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  auto body
    = ast::expression{call("resolved_fn", {call("secret", {constant("key")})})};
  auto expression
    = ast::expression{call("lambda_fn", {lambda("x", std::move(body))})};
  auto evaluator
    = folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx));
  REQUIRE(evaluator);
  CHECK_EQUAL(ctx.resolutions, 1);
  auto result = evaluator->eval(EvalCtx{dh});
  REQUIRE(result.try_as<Int>());
  CHECK_EQUAL(*result.try_as<Int>()->get(0), 8);
  CHECK_EQUAL(ctx.resolutions, 1);
  CHECK(std::move(dh).collect().empty());
}

TEST("secret calls in value positions resolve to secret arrays") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  auto expression = ast::expression{call("secret", {constant("key")})};
  auto evaluator
    = folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx));
  REQUIRE(evaluator);
  CHECK_EQUAL(ctx.resolutions, 1);
  auto events = make_events(Array<Record>::make_empty(3));
  auto result = evaluator->eval(events, EvalCtx{dh});
  auto secrets = result.try_as<Secret>();
  REQUIRE(secrets);
  CHECK_EQUAL(secrets->length(), 3);
  CHECK_EQUAL((*secrets->get(2)).size(), size_t{8});
  CHECK(std::move(dh).collect().empty());
}

TEST("secret names must be constant strings") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  auto expression = ast::expression{call("secret", {root_field("name")})};
  CHECK(
    not folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx)));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "expected a constant expression");
  dh = collecting_diagnostic_handler{};
  expression = ast::expression{call("secret", {constant(int64_t{1})})};
  CHECK(
    not folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx)));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "`secret` expects a string");
  CHECK_EQUAL(ctx.resolutions, 0);
}

namespace {

auto format(std::vector<ast::format_expr::segment> segments)
  -> ast::expression {
  return ast::expression{
    ast::format_expr{std::move(segments), location::unknown}};
}

auto replace(ast::expression expr) -> ast::format_expr::segment {
  return ast::format_expr::replacement{std::move(expr)};
}

auto secret_text(SecretView value) -> std::string {
  return std::string{reinterpret_cast<char const*>(value.data()), value.size()};
}

} // namespace

TEST("format strings with a secret replacement produce secrets") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  auto expression = format({std::string{"Bearer "},
                            replace(call("secret", {constant("key")})),
                            std::string{"!"}, replace(constant(data{}))});
  auto evaluator
    = folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx));
  REQUIRE(evaluator);
  auto events = make_events(Array<Record>::make_empty(3));
  auto result = evaluator->eval(events, EvalCtx{dh});
  auto secrets = result.try_as<Secret>();
  REQUIRE(secrets);
  REQUIRE_EQUAL(secrets->length(), 3);
  for (auto i = storage::Index{0}; i < 3; ++i) {
    CHECK_EQUAL(secret_text(*secrets->get(i)), "Bearer resolved!null");
  }
  expression = format({replace(call("secret", {constant("key")}))});
  evaluator
    = folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx));
  REQUIRE(evaluator);
  result = evaluator->eval(events, EvalCtx{dh});
  secrets = result.try_as<Secret>();
  REQUIRE(secrets);
  CHECK_EQUAL(secret_text(*secrets->get(0)), "resolved");
  CHECK(std::move(dh).collect().empty());
}

TEST("format strings without secrets produce strings") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  auto expression = format(
    {std::string{"n="}, replace(constant(std::int64_t{42})), std::string{"."}});
  auto evaluator
    = folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx));
  REQUIRE(evaluator);
  auto events = make_events(Array<Record>::make_empty(2));
  auto result = evaluator->eval(events, EvalCtx{dh});
  auto strings = result.try_as<String>();
  REQUIRE(strings);
  CHECK_EQUAL(*strings->get(1), "n=42.");
  CHECK_EQUAL(ctx.resolutions, 0);
  CHECK(std::move(dh).collect().empty());
}

TEST("format strings produce secrets only for rows with a secret") {
  auto reg = TestRegistry{};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = SecretCtx{dh, reg};
  auto choice = ast::expression{ast::binary_expr{
    ast::expression{ast::binary_expr{call("secret", {constant("key")}),
                                     ast::binary_op::if_, root_field("c")}},
    ast::binary_op::else_, constant(std::string{"plain"})}};
  auto expression
    = format({std::string{"<"}, replace(std::move(choice)), std::string{">"}});
  auto evaluator
    = folly::coro::blockingWait(Evaluator::make(std::move(expression), ctx));
  REQUIRE(evaluator);
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("c").data(true);
  builder.record().field("c").data(false);
  builder.record().field("c").data(true);
  auto events = make_events(builder.finish());
  auto result = evaluator->eval(events, EvalCtx{dh});
  REQUIRE_EQUAL(result.length(), 3);
  auto secrets = result.get_alternative<Secret>();
  auto strings = result.get_alternative<String>();
  REQUIRE(secrets);
  REQUIRE(strings);
  CHECK(secrets->present.get(0));
  CHECK(not secrets->present.get(1));
  CHECK(secrets->present.get(2));
  CHECK(strings->present.get(1));
  CHECK_EQUAL(secret_text(*secrets->data.get(0)), "<resolved>");
  CHECK_EQUAL(secret_text(*secrets->data.get(2)), "<resolved>");
  CHECK_EQUAL(*strings->data.get(1), "<plain>");
  CHECK(std::move(dh).collect().empty());
}
