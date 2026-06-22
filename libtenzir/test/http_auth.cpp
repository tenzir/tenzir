//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http_auth.hpp"

#include "tenzir/ecc.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/secret_store.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/registry.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/settings.hpp>
#include <folly/coro/BlockingWait.h>

#include <memory>
#include <stdexcept>
#include <string_view>

namespace tenzir {

namespace {

struct NullDiagnostics final : diagnostic_handler {
  void emit(diagnostic) override {
  }
};

struct TestOpCtx final : OpCtx {
  explicit TestOpCtx(caf::actor_system& system) : system_{system} {
  }

  auto actor_system() -> caf::actor_system& override {
    return system_;
  }

  auto dh() -> diagnostic_handler& override {
    return dh_;
  }

  auto reg() -> const registry& override {
    return *global_registry();
  }

  auto resolve_secrets(std::vector<secret_request> requests)
    -> Task<failure_or<void>> override {
    resolved_secret_requests += requests.size();
    for (auto& request : requests) {
      auto const* literal = request.secret.buffer->data_as_literal();
      if (not literal) {
        throw std::logic_error{"expected literal secret in test"};
      }
      auto const value_view = literal->value()->string_view();
      auto value = ecc::cleansing_blob{};
      value.reserve(value_view.size());
      for (auto c : value_view) {
        value.push_back(std::byte{static_cast<unsigned char>(c)});
      }
      auto resolved = resolved_secret_value{std::move(value), true};
      if (auto result = request.callback(std::move(resolved));
          result.is_error()) {
        co_return failure::promise();
      }
    }
    co_return {};
  }

  auto spawn_sub(SubKey, ir::pipeline, element_type_tag, DiagnosticBehavior)
    -> Task<AnySubHandle&> override {
    throw std::logic_error{"unexpected spawn_sub call"};
  }

  auto spawn_sub_fused(SubKey, ir::pipeline, element_type_tag)
    -> Task<AnySubHandle&> override {
    throw std::logic_error{"unexpected spawn_sub_fused call"};
  }

  auto get_sub(SubKeyView) -> Option<AnySubHandle&> override {
    return None{};
  }

  auto io_executor() -> folly::Executor::KeepAlive<folly::IOExecutor> override {
    throw std::logic_error{"unexpected io_executor call"};
  }

  auto spawn_task(Task<void>) -> AsyncHandle<void> override {
    throw std::logic_error{"unexpected spawn_task call"};
  }

  auto save_checkpoint(chunk_ptr) -> Task<void> override {
    throw std::logic_error{"unexpected save_checkpoint call"};
  }

  auto load_checkpoint() -> Task<chunk_ptr> override {
    throw std::logic_error{"unexpected load_checkpoint call"};
  }

  auto flush() -> Task<void> override {
    throw std::logic_error{"unexpected flush call"};
  }

  auto make_counter(MetricsLabel, MetricsDirection, MetricsVisibility,
                    MetricsUnit) -> MetricsCounter override {
    return {};
  }

  auto metrics_receiver() const -> metrics_receiver_actor override {
    return {};
  }

  auto is_hidden() const -> bool override {
    return false;
  }

  auto has_terminal() const -> bool override {
    return false;
  }

  auto checkpoint_settings() const
    -> Option<CheckpointSettings const&> override {
    return None{};
  }

  caf::actor_system& system_;
  NullDiagnostics dh_;
  size_t resolved_secret_requests = 0;
};

auto make_auth_entry(std::string name, std::string strategy)
  -> caf::config_value::dictionary {
  auto entry = caf::config_value::dictionary{};
  entry.emplace("name", std::move(name));
  entry.emplace("strategy", std::move(strategy));
  return entry;
}

struct TestSystem {
  caf::actor_system_config config{};
  std::unique_ptr<caf::actor_system> system;

  explicit TestSystem(std::vector<caf::config_value::dictionary> auths) {
    auto settings = caf::settings{};
    caf::config_value::list auth_list{};
    for (auto& auth : auths) {
      auth_list.emplace_back(std::move(auth));
    }
    caf::put(settings, "tenzir.auth", std::move(auth_list));
    config.content = std::move(settings);
    system = std::make_unique<caf::actor_system>(config);
  }
};

auto has_header(std::vector<http::Header> const& headers, std::string_view name,
                std::string_view value) -> bool {
  for (auto const& header : headers) {
    if (header.name == name and header.value == value) {
      return true;
    }
  }
  return false;
}

} // namespace

TEST("fetch_authorization reports missing config") {
  auto config = caf::actor_system_config{};
  auto system = caf::actor_system{config};
  auto ctx = TestOpCtx{system};
  auto result = folly::coro::blockingWait(fetch_authorization("missing", ctx));
  CHECK(not result);
}

TEST("fetch_authorization rejects unknown auth names") {
  auto ts = TestSystem{{make_auth_entry("known", "basic")}};
  auto& system = *ts.system;
  auto ctx = TestOpCtx{system};
  auto result = folly::coro::blockingWait(fetch_authorization("missing", ctx));
  CHECK(not result);
}

TEST("fetch_authorization supports simple auth strategies") {
  auto basic = make_auth_entry("basic", "basic");
  basic.emplace("username", "user");
  basic.emplace("password", "pass");
  auto api_key = make_auth_entry("api-key", "api-key");
  api_key.emplace("api_key", "token");
  auto bearer = make_auth_entry("bearer-static", "bearer-static");
  bearer.emplace("token", "token");
  auto ts = TestSystem{
    {std::move(basic), std::move(api_key), std::move(bearer)},
  };
  auto& system = *ts.system;
  auto ctx = TestOpCtx{system};
  for (auto name : {"basic", "api-key", "bearer-static"}) {
    auto result = folly::coro::blockingWait(fetch_authorization(name, ctx));
    REQUIRE(result);
  }
}

TEST("fetch_authorization authorizes simple strategies") {
  auto basic = make_auth_entry("basic", "basic");
  basic.emplace("username", "user");
  basic.emplace("password", "pass");
  auto api_key = make_auth_entry("api-key", "api-key");
  api_key.emplace("api_key", "token");
  auto bearer = make_auth_entry("bearer-static", "bearer-static");
  bearer.emplace("token", "token");
  auto ts = TestSystem{
    {std::move(basic), std::move(api_key), std::move(bearer)},
  };
  auto& system = *ts.system;
  auto ctx = TestOpCtx{system};
  auto basic_auth
    = folly::coro::blockingWait(fetch_authorization("basic", ctx));
  REQUIRE(basic_auth);
  CHECK(has_header(basic_auth->headers, "Authorization", "Basic dXNlcjpwYXNz"));

  auto api_key_auth
    = folly::coro::blockingWait(fetch_authorization("api-key", ctx));
  REQUIRE(api_key_auth);
  CHECK(has_header(api_key_auth->headers, "X-Api-Key", "token"));

  auto bearer_auth
    = folly::coro::blockingWait(fetch_authorization("bearer-static", ctx));
  REQUIRE(bearer_auth);
  CHECK(has_header(bearer_auth->headers, "Authorization", "Bearer token"));
}

TEST("build_platform_auth_record materializes references as managed secrets") {
  auto auth = platform_authentication{
    .strategy = "oauth-client-credentials",
    .public_config = record{
      {"client_id", std::string{"my-client-id"}},
      {"token_url", std::string{"https://example.com/oauth/token"}},
      {"audience", std::string{"https://example.com/api/"}},
      {"unused", caf::none},
    },
    .secret_field_references = {{"client_secret", "MY_CLIENT_SECRET"}},
  };
  auto config = build_platform_auth_record("my-auth", std::move(auth));
  // Public fields kept verbatim.
  CHECK_EQUAL(as<std::string>(config["client_id"]), "my-client-id");
  CHECK_EQUAL(as<std::string>(config["token_url"]),
              "https://example.com/oauth/token");
  CHECK_EQUAL(as<std::string>(config["audience"]), "https://example.com/api/");
  // Injected metadata.
  CHECK_EQUAL(as<std::string>(config["name"]), "my-auth");
  CHECK_EQUAL(as<std::string>(config["strategy"]), "oauth-client-credentials");
  // Null public-config fields are stripped.
  CHECK(not config.contains("unused"));
  // Referenced secret becomes a managed-secret reference.
  auto const* secret_data = try_as<secret>(&config["client_secret"]);
  REQUIRE(secret_data);
  auto const* name_buffer = secret_data->buffer->data_as_name();
  REQUIRE(name_buffer);
  CHECK_EQUAL(name_buffer->value()->string_view(), "MY_CLIENT_SECRET");
}

TEST("build_platform_auth_record handles strategies without secrets") {
  auto auth = platform_authentication{
    .strategy = "bearer-static",
    .public_config = {},
    .secret_field_references = {{"token", "BEARER_TOKEN"}},
  };
  auto config = build_platform_auth_record("static", std::move(auth));
  CHECK_EQUAL(as<std::string>(config["name"]), "static");
  CHECK_EQUAL(as<std::string>(config["strategy"]), "bearer-static");
  auto const* token = try_as<secret>(&config["token"]);
  REQUIRE(token);
  auto const* name_buffer = token->buffer->data_as_name();
  REQUIRE(name_buffer);
  CHECK_EQUAL(name_buffer->value()->string_view(), "BEARER_TOKEN");
}

TEST("build_platform_auth_record supports api-key shape") {
  auto auth = platform_authentication{
    .strategy = "api-key",
    .public_config = record{{"header_name", std::string{"X-Custom-Key"}}},
    .secret_field_references = {{"api_key", "API_KEY_SECRET"}},
  };
  auto config = build_platform_auth_record("apikey", std::move(auth));
  CHECK_EQUAL(as<std::string>(config["header_name"]), "X-Custom-Key");
  CHECK_EQUAL(as<std::string>(config["strategy"]), "api-key");
  auto const* key = try_as<secret>(&config["api_key"]);
  REQUIRE(key);
  auto const* name_buffer = key->buffer->data_as_name();
  REQUIRE(name_buffer);
  CHECK_EQUAL(name_buffer->value()->string_view(), "API_KEY_SECRET");
}

TEST("build_platform_auth_record supports basic shape") {
  auto auth = platform_authentication{
    .strategy = "basic",
    .public_config = record{{"username", std::string{"alice"}}},
    .secret_field_references = {{"password", "ALICE_PASSWORD"}},
  };
  auto config = build_platform_auth_record("basic", std::move(auth));
  CHECK_EQUAL(as<std::string>(config["username"]), "alice");
  CHECK_EQUAL(as<std::string>(config["strategy"]), "basic");
  auto const* password = try_as<secret>(&config["password"]);
  REQUIRE(password);
  auto const* name_buffer = password->buffer->data_as_name();
  REQUIRE(name_buffer);
  CHECK_EQUAL(name_buffer->value()->string_view(), "ALICE_PASSWORD");
}

TEST("fetch_authorization caches simple strategies") {
  // Use distinct auth entry names so the process-global auth cache
  // from earlier tests in this suite (e.g. "authorizes simple strategies")
  // cannot pollute this test if the underlying actor_system pointer is
  // reused by the allocator across tests.
  auto basic = make_auth_entry("cache-basic", "basic");
  basic.emplace("username", "user");
  basic.emplace("password", "pass");
  auto api_key = make_auth_entry("cache-api-key", "api-key");
  api_key.emplace("api_key", "token");
  auto bearer = make_auth_entry("cache-bearer-static", "bearer-static");
  bearer.emplace("token", "token");
  auto ts = TestSystem{
    {std::move(basic), std::move(api_key), std::move(bearer)},
  };
  auto& system = *ts.system;
  auto ctx = TestOpCtx{system};
  for (auto const name :
       {"cache-basic", "cache-api-key", "cache-bearer-static"}) {
    auto first = folly::coro::blockingWait(fetch_authorization(name, ctx));
    REQUIRE(first);
    auto second = folly::coro::blockingWait(fetch_authorization(name, ctx));
    REQUIRE(second);
  }
  CHECK(ctx.resolved_secret_requests == 4);
}

} // namespace tenzir
