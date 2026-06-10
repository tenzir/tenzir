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
      auto const* literal
        = request.secret.buffer->data_as_literal();
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
      if (auto result = request.callback(std::move(resolved)); result.is_error()) {
        co_return failure::promise();
      }
    }
    co_return {};
  }

  auto resolve_authentication(std::string)
    -> Task<failure_or<resolved_authentication>> override {
    throw std::logic_error{"unexpected resolve_authentication call"};
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
  auto result = folly::coro::blockingWait(
    fetch_authorization("missing", ctx));
  CHECK(not result);
}

TEST("fetch_authorization rejects unknown auth names") {
  auto ts = TestSystem{{make_auth_entry("known", "basic")}};
  auto& system = *ts.system;
  auto ctx = TestOpCtx{system};
  auto result = folly::coro::blockingWait(
    fetch_authorization("missing", ctx));
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
    auto result = folly::coro::blockingWait(
      fetch_authorization(name, ctx));
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
  auto basic_auth = folly::coro::blockingWait(fetch_authorization("basic", ctx));
  REQUIRE(basic_auth);
  CHECK(has_header(basic_auth->headers, "Authorization", "Basic dXNlcjpwYXNz"));

  auto api_key_auth
    = folly::coro::blockingWait(fetch_authorization("api-key", ctx));
  REQUIRE(api_key_auth);
  CHECK(has_header(api_key_auth->headers, "X-Api-Key", "token"));

  auto bearer_auth = folly::coro::blockingWait(
    fetch_authorization("bearer-static", ctx));
  REQUIRE(bearer_auth);
  CHECK(has_header(bearer_auth->headers, "Authorization", "Bearer token"));
}

TEST("fetch_authorization caches simple strategies") {
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
  for (auto const name : {"basic", "api-key", "bearer-static"}) {
    auto first = folly::coro::blockingWait(fetch_authorization(name, ctx));
    REQUIRE(first);
    auto second = folly::coro::blockingWait(fetch_authorization(name, ctx));
    REQUIRE(second);
  }
  CHECK(ctx.resolved_secret_requests == 4);
}

} // namespace tenzir
