//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The test framework's `CHECK` macro clashes with folly's, so folly must come
// first.
// clang-format off
#include <folly/coro/Collect.h>
// clang-format on

#include "tenzir/aws_iam.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/fbs/data.hpp"
#include "tenzir/http.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/startup_argument.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/eval.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

auto constant(data value) -> ast::expression {
  return ast::expression{
    ast::constant::make(located<data>{std::move(value), location::unknown})};
}

/// A call to the builtin `name`, resolved to the `std` package.
auto call(std::string name, std::vector<ast::expression> args,
          bool method = false) -> ast::expression {
  auto result = ast::function_call{
    ast::entity{{ast::identifier{name, location::unknown}}}, std::move(args),
    location::unknown, method};
  result.fn.ref = entity_path{
    std::string{entity_pkg_std}, {std::move(name)}, entity_ns::fn};
  return ast::expression{std::move(result)};
}

auto add(ast::expression left, ast::expression right) -> ast::expression {
  return ast::expression{
    ast::binary_expr{std::move(left), ast::binary_op::add, std::move(right)}};
}

auto format(std::vector<ast::format_expr::segment> segments)
  -> ast::expression {
  return ast::expression{
    ast::format_expr{std::move(segments), location::unknown}};
}

auto replacement(ast::expression expr) -> ast::format_expr::segment {
  return ast::format_expr::replacement{std::move(expr)};
}

auto record_expr(std::vector<std::pair<std::string, ast::expression>> fields)
  -> ast::expression {
  auto items = std::vector<ast::record::item>{};
  for (auto& [name, expr] : fields) {
    items.emplace_back(ast::record::field{
      ast::identifier{std::move(name), location::unknown}, std::move(expr)});
  }
  return ast::expression{
    ast::record{location::unknown, std::move(items), location::unknown}};
}

auto field(ast::expression left, std::string name,
           bool has_question_mark = false) -> ast::expression {
  return ast::expression{
    ast::field_access{std::move(left), location::unknown, has_question_mark,
                      ast::identifier{std::move(name), location::unknown}}};
}

auto index(ast::expression expr, ast::expression index) -> ast::expression {
  return ast::expression{ast::index_expr{std::move(expr), location::unknown,
                                         std::move(index), location::unknown,
                                         false}};
}

auto eval(ast::expression const& expr, diagnostic_handler& dh)
  -> failure_or<data> {
  TRY(auto result, const_eval(expr, base_ctx{dh, test::builtin_registry()}));
  return std::move(result.inner);
}

auto first_error(std::vector<diagnostic> const& diags) -> std::string {
  for (auto const& diag : diags) {
    if (diag.severity == severity::error) {
      return diag.message;
    }
  }
  return {};
}

} // namespace

namespace {

class StartupCtx final : public test::NovaOpCtx {
public:
  using NovaOpCtx::NovaOpCtx;

  auto resolve_secrets(std::vector<secret_request> requests)
    -> Task<failure_or<void>> override {
    co_await folly::coro::co_reschedule_on_current_executor;
    for (auto& request : requests) {
      ++resolutions;
      if (fail) {
        diagnostic::error("test secret is unavailable")
          .primary(request.location)
          .emit(dh());
        co_return failure::promise();
      }
      auto bytes = std::as_bytes(std::span{text});
      auto requested = request_map_t{};
      auto value = located_resolved_secret{request.location};
      value.value = ecc::cleansing_blob{bytes.begin(), bytes.end()};
      requested.emplace("key", std::move(value));
      auto finisher
        = secret_finisher{std::move(request.secret),
                          std::move(request.callback), request.location};
      CO_TRY(finisher.finish(requested, censor, dh()));
    }
    co_return {};
  }

  auto register_secret(nova::Secret const& value) -> void override {
    censor.add(value.data);
  }

  std::string text = "runtime-canary-7f13";
  size_t resolutions = 0;
  bool fail = false;
  secret_censor censor;
};

auto secret_text(nova::Secret const& value) -> std::string {
  return {reinterpret_cast<char const*>(value.data.data()), value.data.size()};
}

auto serialize_argument(auto& argument) -> caf::byte_buffer {
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  REQUIRE(serializer.apply(argument));
  return bytes;
}

} // namespace

TEST("startup arguments persist recipes and resolve again after restore") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument = StartupArgument<nova::Secret>{
    add(constant("prefix-"), call("secret", {constant("key")}))};
  auto before = serialize_argument(argument);
  CHECK_EQUAL(ctx.resolutions, 0u);
  auto result = folly::coro::blockingWait(argument.evaluate(ctx));
  REQUIRE(result);
  CHECK_EQUAL(secret_text(*result), "prefix-runtime-canary-7f13");
  CHECK_EQUAL(serialize_argument(argument), before);
  auto persisted
    = std::string{reinterpret_cast<char const*>(before.data()), before.size()};
  CHECK_EQUAL(persisted.find(ctx.text), std::string::npos);
  CHECK_EQUAL(persisted.find(secret_text(*result)), std::string::npos);
  CHECK_EQUAL(ctx.censor.censor(secret_text(*result)), "***");
  auto restored = StartupArgument<nova::Secret>{};
  auto reader = caf::binary_deserializer{before};
  REQUIRE(reader.apply(restored));
  ctx.text = "rotated-runtime-canary";
  auto rotated = folly::coro::blockingWait(restored.evaluate(ctx));
  REQUIRE(rotated);
  CHECK_EQUAL(secret_text(*rotated), "prefix-rotated-runtime-canary");
}

TEST("startup arguments promote literals without resolving secrets") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument = StartupArgument<nova::Secret>{constant("literal")};
  auto result = folly::coro::blockingWait(argument.evaluate(ctx));
  REQUIRE(result);
  CHECK_EQUAL(secret_text(*result), "literal");
  CHECK_EQUAL(ctx.resolutions, 0u);
}

TEST("startup arguments evaluate structured functions with resolved secrets") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument = StartupArgument<nova::Record>{
    call("drop_null_fields",
         {
           record_expr({{"key", call("secret", {constant("key")})},
                        {"omit", constant(data{})}}),
         })};
  auto result = folly::coro::blockingWait(argument.evaluate(ctx));
  REQUIRE(result);
  CHECK_EQUAL(result->size(), 1u);
  auto it = result->find("key");
  REQUIRE(it != result->end());
  auto* value = try_as<nova::Secret>(&it->second);
  REQUIRE(value);
  CHECK_EQUAL(secret_text(*value), ctx.text);
}

TEST("startup arguments evaluate for concurrent consumers") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument
    = StartupArgument<nova::Secret>{call("secret", {constant("key")})};
  auto before = serialize_argument(argument);
  auto [left, right] = folly::coro::blockingWait(
    folly::coro::collectAll(argument.evaluate(ctx), argument.evaluate(ctx)));
  REQUIRE(left);
  REQUIRE(right);
  CHECK_EQUAL(secret_text(*left), ctx.text);
  CHECK_EQUAL(secret_text(*right), ctx.text);
  CHECK_EQUAL(ctx.resolutions, 2u);
  CHECK_EQUAL(serialize_argument(argument), before);
}

TEST("startup argument failures preserve their recipes") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument
    = StartupArgument<nova::Secret>{call("secret", {constant("key")})};
  auto before = serialize_argument(argument);
  ctx.fail = true;
  CHECK(not folly::coro::blockingWait(argument.evaluate(ctx)));
  CHECK_EQUAL(serialize_argument(argument), before);
  ctx.fail = false;
  auto wrong_type = StartupArgument<uint64_t>{constant(int64_t{-1})};
  CHECK(not folly::coro::blockingWait(wrong_type.evaluate(ctx)));
  auto input = StartupArgument<nova::Data>{ast::this_{location::unknown}};
  CHECK(not folly::coro::blockingWait(input.evaluate(ctx)));
}

TEST("startup arguments evaluate constant defaults") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument = StartupArgument<nova::Secret>::constant("localhost");
  auto result = folly::coro::blockingWait(argument.evaluate_string(ctx, "x"));
  REQUIRE(result);
  CHECK_EQUAL(*result, "localhost");
  CHECK_EQUAL(ctx.resolutions, 0u);
}

TEST("startup arguments reject secrets that are not UTF-8") {
  auto dh = collecting_diagnostic_handler{};
  auto bytes = std::array{std::byte{0xff}, std::byte{0xfe}};
  auto secret = nova::Secret{ecc::cleansing_blob{bytes.begin(), bytes.end()}};
  CHECK(not nova::reveal_string(secret, "key", location::unknown, dh));
  CHECK_EQUAL(std::move(dh).collect().size(), 1u);
}

TEST("startup header records reveal secrets") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument = StartupArgument<nova::Record>{record_expr({
    {"X-Plain", constant("plain")},
    {"X-Secret", call("secret", {constant("key")})},
  })};
  auto record = folly::coro::blockingWait(argument.evaluate(ctx));
  REQUIRE(record);
  auto headers = http::make_headers(
    located{std::move(*record), argument.get_location()}, dh);
  REQUIRE(headers);
  REQUIRE_EQUAL(headers->size(), 2u);
  CHECK_EQUAL((*headers)[0].value, "plain");
  CHECK_EQUAL((*headers)[1].value, ctx.text);
  CHECK_EQUAL(ctx.censor.censor(ctx.text), "***");
  auto invalid = nova::Record{};
  invalid.emplace("X-Number", nova::Data{int64_t{42}});
  CHECK(
    not http::make_headers(located{std::move(invalid), location::unknown}, dh));
}

TEST("startup AWS IAM records reveal secrets") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = StartupCtx{dh, test::builtin_registry()};
  auto argument = Option<StartupArgument<nova::Record>>{
    StartupArgument<nova::Record>{record_expr({
      {"access_key_id", call("secret", {constant("key")})},
      {"secret_access_key", constant("secret-access-key")},
    })}};
  auto auth
    = folly::coro::blockingWait(resolve_aws_iam_auth(argument, None{}, ctx));
  REQUIRE(auth);
  REQUIRE(auth->credentials);
  CHECK_EQUAL(auth->credentials->access_key_id, ctx.text);
  CHECK_EQUAL(auth->credentials->secret_access_key, "secret-access-key");
  auto incomplete
    = Option<StartupArgument<nova::Record>>{StartupArgument<nova::Record>{
      record_expr({{"access_key_id", constant("id")}})}};
  CHECK(not folly::coro::blockingWait(
    resolve_aws_iam_auth(incomplete, None{}, ctx)));
}

TEST("const_eval evaluates with nova") {
  auto dh = null_diagnostic_handler{};
  auto result
    = eval(add(constant(std::int64_t{1}), constant(std::int64_t{2})), dh);
  REQUIRE(result);
  CHECK_EQUAL(*result, data{std::int64_t{3}});
}

TEST("const_eval yields an unresolved secret") {
  auto dh = null_diagnostic_handler{};
  auto result = eval(call("secret", {constant("key")}), dh);
  REQUIRE(result);
  CHECK_EQUAL(*result, data{secret::make_managed("key")});
}

TEST("const_eval concatenates and transforms secrets") {
  auto dh = null_diagnostic_handler{};
  auto result
    = eval(add(constant("x"),
               call("decode_hex", {call("secret", {constant("key")})}, true)),
           dh);
  REQUIRE(result);
  auto expected
    = secret::make_managed("key")
        .with_operation(fbs::data::SecretTransformations::decode_hex)
        .with_prepended("x");
  CHECK_EQUAL(*result, data{std::move(expected)});
}

TEST("const_eval combines a secret constant") {
  auto dh = null_diagnostic_handler{};
  auto result
    = eval(add(constant(secret::make_managed("key")), constant("y")), dh);
  REQUIRE(result);
  CHECK_EQUAL(*result, data{secret::make_managed("key").with_appended("y")});
}

TEST("const_eval formats secrets") {
  auto dh = null_diagnostic_handler{};
  auto result
    = eval(format({replacement(constant(std::int64_t{1})), " ",
                   replacement(call("secret", {constant("key")})), "!"}),
           dh);
  REQUIRE(result);
  auto expected = secret::make_literal("1 ")
                    .with_appended(secret::make_managed("key"))
                    .with_appended("!");
  CHECK_EQUAL(*result, data{std::move(expected)});
}

TEST("const_eval nests secrets in records") {
  auto dh = null_diagnostic_handler{};
  auto result
    = eval(record_expr({
             {"key", call("secret", {constant("key")})},
             {"n", add(constant(std::int64_t{1}), constant(std::int64_t{1}))},
           }),
           dh);
  REQUIRE(result);
  auto expected = record{
    {"key", secret::make_managed("key")},
    {"n", std::int64_t{2}},
  };
  CHECK_EQUAL(*result, data{std::move(expected)});
}

TEST("const_eval stringifies nested secrets in format strings") {
  auto dh = null_diagnostic_handler{};
  auto result = eval(format({"x", replacement(record_expr({
                                    {"a", call("secret", {constant("key")})},
                                  }))}),
                     dh);
  REQUIRE(result);
  CHECK_EQUAL(*result, data{std::string{R"(x{a:"***"})"}});
}

TEST("const_eval accesses fields that hold secrets") {
  auto dh = null_diagnostic_handler{};
  auto cfg = constant(record{
    {"user", "u"},
    {"pass", secret::make_managed("pass")},
  });
  auto by_field = eval(field(cfg, "pass"), dh);
  REQUIRE(by_field);
  CHECK_EQUAL(*by_field, data{secret::make_managed("pass")});
  auto by_index = eval(index(cfg, constant("pass")), dh);
  REQUIRE(by_index);
  CHECK_EQUAL(*by_index, data{secret::make_managed("pass")});
  auto other = eval(field(cfg, "user"), dh);
  REQUIRE(other);
  CHECK_EQUAL(*other, data{std::string{"u"}});
}

TEST("const_eval indexes lists that hold secrets") {
  auto dh = null_diagnostic_handler{};
  auto keys = constant(list{
    secret::make_managed("a"),
    secret::make_managed("b"),
  });
  auto last = eval(index(keys, constant(std::int64_t{-1})), dh);
  REQUIRE(last);
  CHECK_EQUAL(*last, data{secret::make_managed("b")});
}

TEST("const_eval warns about missing fields next to secrets") {
  auto cfg = constant(record{{"pass", secret::make_managed("pass")}});
  auto dh = collecting_diagnostic_handler{};
  auto missing = eval(field(cfg, "nope"), dh);
  REQUIRE(missing);
  CHECK_EQUAL(*missing, data{});
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].severity, severity::warning);
  CHECK_EQUAL(diags[0].message, "record does not have field");
  auto quiet_dh = collecting_diagnostic_handler{};
  auto optional = eval(field(cfg, "nope", true), quiet_dh);
  REQUIRE(optional);
  CHECK_EQUAL(*optional, data{});
  CHECK(std::move(quiet_dh).collect().empty());
}

TEST("const_eval rejects unsupported operations on secrets") {
  auto dh = collecting_diagnostic_handler{};
  CHECK(
    not eval(call("length", {call("secret", {constant("key")})}, true), dh));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "unsupported operation on a secret");
}

TEST("const_eval rejects a secret with a non-string name") {
  auto dh = collecting_diagnostic_handler{};
  CHECK(not eval(call("secret", {constant(std::int64_t{1})}), dh));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "`secret` expects a string");
}

TEST("startup bindings fold everything but their secrets") {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = base_ctx{dh, test::builtin_registry()};
  auto expr = record_expr({
    {"key", call("secret", {add(constant("k"), constant("ey"))})},
    {"at", call("now", {})},
  });
  auto binding = nova::fold_startup_binding(expr, ctx);
  REQUIRE(binding);
  CHECK(binding->requires_startup);
  CHECK(binding->deterministic);
  auto const* record = try_as<ast::record>(&expr);
  REQUIRE(record);
  REQUIRE_EQUAL(record->items.size(), size_t{2});
  auto const& key = as<ast::record::field>(record->items[0]).expr;
  auto const* secret_call = try_as<ast::function_call>(&key);
  REQUIRE(secret_call);
  REQUIRE_EQUAL(secret_call->args.size(), size_t{1});
  auto const* name = try_as<ast::constant>(&secret_call->args[0]);
  REQUIRE(name);
  CHECK_EQUAL(name->as_data(), data{std::string{"key"}});
  auto const& at = as<ast::record::field>(record->items[1]).expr;
  auto const* at_constant = try_as<ast::constant>(&at);
  REQUIRE(at_constant);
  CHECK(is<tenzir::time>(at_constant->as_data()));
  CHECK(std::move(dh).collect().empty());
}
