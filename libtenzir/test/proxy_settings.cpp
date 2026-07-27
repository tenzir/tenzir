//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/proxy_settings.hpp"

#include "tenzir/detail/env.hpp"
#include "tenzir/error.hpp"
#include "tenzir/test/test.hpp"

#include <optional>
#include <ranges>
#include <string>
#include <vector>

using namespace tenzir;

namespace {

struct env_fixture {
  env_fixture() {
    for (auto const* key :
         {"TENZIR_HTTP_PROXY", "TENZIR_HTTPS_PROXY", "TENZIR_NO_PROXY",
          "HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY",
          "no_proxy"}) {
      remember(key);
    }
  }

  auto set(std::string_view key, std::string_view value) -> void {
    remember(key);
    REQUIRE_EQUAL(detail::setenv(key, value), caf::none);
  }

  auto unset(std::string_view key) -> void {
    remember(key);
    REQUIRE_EQUAL(detail::unsetenv(key), caf::none);
  }

  ~env_fixture() {
    for (auto const& [key, value] : previous) {
      auto error = value ? detail::setenv(key, *value) : detail::unsetenv(key);
      TENZIR_ASSERT(error == caf::none);
    }
  }

  auto remember(std::string_view key) -> void {
    auto name = std::string{key};
    if (std::ranges::any_of(previous, [&](auto const& entry) {
          return entry.first == name;
        })) {
      return;
    }
    previous.emplace_back(std::move(name), detail::getenv(key));
  }

  std::vector<std::pair<std::string, std::optional<std::string>>> previous;
};

} // namespace

WITH_FIXTURE(env_fixture) {
  TEST("http and https proxies are resolved independently from config") {
    auto settings = caf::settings{};
    caf::put(settings, "tenzir.http-proxy",
             std::string{"http://http-proxy.example:3128"});
    caf::put(settings, "tenzir.https-proxy",
             std::string{"http://https-proxy.example:8443"});
    REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
    auto http_proxy = proxy_for_target("http", "example.com");
    auto https_proxy = proxy_for_target("https", "example.com");
    REQUIRE(http_proxy);
    REQUIRE(https_proxy);
    CHECK_EQUAL(http_proxy->url, "http://http-proxy.example:3128");
    CHECK_EQUAL(https_proxy->url, "http://https-proxy.example:8443");
    CHECK_EQUAL(https_proxy->host, "https-proxy.example");
    CHECK_EQUAL(https_proxy->port, uint16_t{8443});
  }

  TEST("http and https proxies are resolved independently from environment") {
    unset("TENZIR_HTTP_PROXY");
    unset("TENZIR_HTTPS_PROXY");
    unset("TENZIR_NO_PROXY");
    unset("HTTP_PROXY");
    unset("http_proxy");
    unset("HTTPS_PROXY");
    unset("https_proxy");
    unset("NO_PROXY");
    unset("no_proxy");
    set("HTTP_PROXY", "http://http-env-proxy.example:3128");
    set("HTTPS_PROXY", "http://https-env-proxy.example:8443");
    auto settings = caf::settings{};
    REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
    auto http_proxy = proxy_for_target("http", "example.com");
    auto https_proxy = proxy_for_target("https", "example.com");
    REQUIRE(http_proxy);
    REQUIRE(https_proxy);
    CHECK_EQUAL(http_proxy->url, "http://http-env-proxy.example:3128");
    CHECK_EQUAL(https_proxy->url, "http://https-env-proxy.example:8443");
    CHECK_EQUAL(detail::getenv("http_proxy").value_or(""),
                "http://http-env-proxy.example:3128");
    CHECK_EQUAL(detail::getenv("https_proxy").value_or(""),
                "http://https-env-proxy.example:8443");
  }

  TEST("Tenzir proxy environment overrides generic environment") {
    unset("TENZIR_HTTP_PROXY");
    unset("TENZIR_HTTPS_PROXY");
    unset("TENZIR_NO_PROXY");
    unset("HTTP_PROXY");
    unset("http_proxy");
    unset("HTTPS_PROXY");
    unset("https_proxy");
    unset("NO_PROXY");
    unset("no_proxy");
    set("HTTP_PROXY", "http://http-env-proxy.example:3128");
    set("HTTPS_PROXY", "http://https-env-proxy.example:8443");
    set("NO_PROXY", "generic-blocked.example");
    set("TENZIR_HTTP_PROXY", "http://tenzir-http-proxy.example:3128");
    set("TENZIR_HTTPS_PROXY", "http://tenzir-https-proxy.example:8443");
    set("TENZIR_NO_PROXY", "tenzir-blocked.example");
    auto settings = caf::settings{};
    REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
    auto http_proxy = proxy_for_target("http", "example.com");
    auto https_proxy = proxy_for_target("https", "example.com");
    REQUIRE(http_proxy);
    REQUIRE(https_proxy);
    CHECK_EQUAL(http_proxy->url, "http://tenzir-http-proxy.example:3128");
    CHECK_EQUAL(https_proxy->url, "http://tenzir-https-proxy.example:8443");
    CHECK(bypass_proxy("tenzir-blocked.example"));
    CHECK(not bypass_proxy("generic-blocked.example"));
  }

  TEST("malformed configured proxies do not fall back to environment") {
    unset("TENZIR_HTTP_PROXY");
    unset("TENZIR_HTTPS_PROXY");
    unset("HTTP_PROXY");
    unset("http_proxy");
    unset("HTTPS_PROXY");
    unset("https_proxy");
    set("TENZIR_HTTP_PROXY", "http://tenzir-http-proxy.example:3128");
    set("TENZIR_HTTPS_PROXY", "http://tenzir-https-proxy.example:8443");
    auto http_settings = caf::settings{};
    caf::put(http_settings, "tenzir.http-proxy",
             caf::config_value::list{
               caf::config_value{std::string{"http://proxy.example:3128"}},
             });
    auto http_error = initialize_proxy_settings(http_settings);
    REQUIRE(http_error);
    CHECK_EQUAL(render(http_error),
                "!! invalid_configuration: tenzir.http-proxy must be a string");
    CHECK(not proxy_for_target("http", "example.com"));
    auto https_settings = caf::settings{};
    caf::put(https_settings, "tenzir.https-proxy",
             caf::config_value::integer{42});
    auto https_error = initialize_proxy_settings(https_settings);
    REQUIRE(https_error);
    CHECK_EQUAL(render(https_error), "!! invalid_configuration: "
                                     "tenzir.https-proxy must be a string");
    CHECK(not proxy_for_target("https", "example.com"));
  }

  TEST("empty Tenzir no-proxy environment overrides generic environment") {
    unset("TENZIR_HTTP_PROXY");
    unset("TENZIR_HTTPS_PROXY");
    unset("TENZIR_NO_PROXY");
    unset("HTTP_PROXY");
    unset("http_proxy");
    unset("HTTPS_PROXY");
    unset("https_proxy");
    unset("NO_PROXY");
    unset("no_proxy");
    set("NO_PROXY", ".example.com");
    set("TENZIR_NO_PROXY", "");
    set("TENZIR_HTTPS_PROXY", "http://tenzir-https-proxy.example:8443");
    auto settings = caf::settings{};
    REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
    auto https_proxy = proxy_for_target("https", "api.example.com");
    REQUIRE(https_proxy);
    CHECK_EQUAL(https_proxy->url, "http://tenzir-https-proxy.example:8443");
  }

  TEST("empty configured no-proxy overrides generic environment") {
    unset("TENZIR_HTTP_PROXY");
    unset("TENZIR_HTTPS_PROXY");
    unset("TENZIR_NO_PROXY");
    unset("HTTP_PROXY");
    unset("http_proxy");
    unset("HTTPS_PROXY");
    unset("https_proxy");
    unset("NO_PROXY");
    unset("no_proxy");
    set("NO_PROXY", ".example.com");
    auto settings = caf::settings{};
    caf::put(settings, "tenzir.https-proxy",
             std::string{"http://https-proxy.example:8443"});
    caf::put(settings, "tenzir.no-proxy", std::string{});
    REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
    auto https_proxy = proxy_for_target("https", "api.example.com");
    REQUIRE(https_proxy);
    CHECK_EQUAL(https_proxy->url, "http://https-proxy.example:8443");
  }

  TEST("no-proxy bypass applies after scheme-specific proxy selection") {
    auto settings = caf::settings{};
    caf::put(settings, "tenzir.http-proxy",
             std::string{"http://http-proxy.example:3128"});
    caf::put(settings, "tenzir.https-proxy",
             std::string{"http://https-proxy.example:8443"});
    caf::put(settings, "tenzir.no-proxy", std::string{"blocked.example"});
    REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
    CHECK(not proxy_for_target("http", "blocked.example"));
    CHECK(not proxy_for_target("https", "api.blocked.example"));
    CHECK(proxy_for_target("http", "allowed.example"));
    CHECK(proxy_for_target("https", "allowed.example"));
  }
}

TEST("no-proxy entries match curl-style domain suffixes") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{"example.com"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("example.com"));
  CHECK(bypass_proxy("api.example.com"));
  CHECK(not bypass_proxy("badexample.com"));
  CHECK(not bypass_proxy("example.com.org"));
}

TEST("no-proxy dotted entries match curl-style domain suffixes") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{".example.com"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("example.com"));
  CHECK(bypass_proxy("api.example.com"));
  CHECK(not bypass_proxy("badexample.com"));
}

TEST("no-proxy accepts list values from environment parsing") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy",
           caf::config_value::list{
             caf::config_value{std::string{"example.com"}},
             caf::config_value{std::string{".internal"}},
           });
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("example.com"));
  CHECK(bypass_proxy("api.example.com"));
  CHECK(bypass_proxy("api.internal"));
}

TEST("no-proxy CIDR entries match IP literals") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy",
           std::string{"10.0.0.0/8,2001:db8::/32"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("10.1.2.3"));
  CHECK(not bypass_proxy("11.1.2.3"));
  CHECK(bypass_proxy("2001:db8::1"));
  CHECK(bypass_proxy("[2001:db8::1]"));
  CHECK(not bypass_proxy("2001:db9::1"));
}

TEST("no-proxy CIDR entries match scoped IPv6 literals") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{"fe80::/10"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("fe80::1%eth0"));
  CHECK(bypass_proxy("[fe80::1%25eth0]"));
  CHECK(not bypass_proxy("fec0::1%eth0"));
  CHECK_EQUAL(effective_no_proxy_for_target("fe80::1%eth0"),
              "localhost,127.0.0.1,127.0.0.0/8,169.254.0.0/16,::1,fe80::/10,"
              "fe80::/10,fe80::1");
  CHECK_EQUAL(effective_no_proxy_for_target("[fe80::1%25eth0]"),
              "localhost,127.0.0.1,127.0.0.0/8,169.254.0.0/16,::1,fe80::/10,"
              "fe80::/10,fe80::1");
  CHECK_EQUAL(effective_no_proxy_for_target("fec0::1%eth0"),
              "localhost,127.0.0.1,127.0.0.0/8,169.254.0.0/16,::1,fe80::/10,"
              "fe80::/10");
}

TEST("no-proxy preserves percent-encoded hostnames") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{"example.com"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(not bypass_proxy("example%2ecom"));
}

TEST("link-local addresses always bypass proxies") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.http-proxy",
           std::string{"http://proxy.example:3128"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("169.254.169.254"));
  CHECK(bypass_proxy("fe80::1%eth0"));
  CHECK(not proxy_for_target("http", "169.254.169.254"));
  CHECK(not proxy_for_target("http", "169.254.170.2"));
  CHECK(not proxy_for_target("http", "fe80::1%eth0"));
  CHECK(not proxy_for_target("http", "[fe80::1%25eth0]"));
  CHECK(proxy_for_target("http", "169.253.255.255"));
  CHECK(proxy_for_target("http", "fec0::1%eth0"));
}

TEST("no-proxy CIDR entries do not resolve DNS names") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{"10.0.0.0/8"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(not bypass_proxy("example.internal"));
}

TEST("invalid slash entries do not match as hostnames") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{"internal/name"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(not bypass_proxy("internal/name"));
}
