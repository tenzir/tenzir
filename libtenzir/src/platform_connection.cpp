// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/controller/http_transport.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/platform_connection.hpp>
#include <tenzir/tunnel/platform.hpp>

#include <fmt/format.h>
#include <folly/coro/DetachOnCancel.h>

#include <filesystem>
#include <fstream>
#include <mutex>

namespace tenzir {

namespace {

auto platform_mutex = std::mutex{};
auto platform = Option<PlatformConnection>{};

auto describe(variant<ReadAccountError, CallError> const& error)
  -> std::string {
  return match(
    error,
    [](ReadAccountError const& declared) {
      return match(
        declared,
        [](InternalServerError const&) -> std::string {
          return "the platform failed to say whose key it is";
        },
        [](auto const& refused) -> std::string {
          return fmt::format("the platform rejected the deployment key: {}",
                             refused.message);
        });
    },
    [](CallError const& failed) {
      return match(
        failed,
        [](RequestError const& request) {
          return fmt::format("could not reach the platform: {}",
                             request.message);
        },
        [](ParseError const& parse) {
          return fmt::format("the platform answered with an invalid {}: {}",
                             parse.schema, parse.reason);
        },
        [](ResponseError const& response) {
          return fmt::format("the platform answered with HTTP {}",
                             response.status);
        });
    });
}

} // namespace

auto DeploymentIdentity::path() const -> DeploymentPath {
  return DeploymentPath{std::string{org_id.to_string()},
                        std::string{workspace_id.to_string()},
                        std::string{deployment_id.to_string()}};
}

auto DeploymentIdentity::route() const -> std::string {
  return fmt::format("/organizations/{}/workspaces/{}/deployments/{}",
                     detail::percent_escape(org_id.to_string()),
                     detail::percent_escape(workspace_id.to_string()),
                     detail::percent_escape(deployment_id.to_string()));
}

auto read_deployment_identity(Transport& transport,
                              DeploymentId const& expected)
  -> Task<Result<DeploymentIdentity, std::string>> {
  auto answered = co_await call(transport, ReadAccount{});
  if (answered.is_err()) {
    co_return Err{describe(answered.unwrap_err())};
  }
  co_return match(
    std::move(answered).unwrap(),
    [&](DeploymentPrincipal principal)
      -> Result<DeploymentIdentity, std::string> {
      if (principal.deployment.id != expected) {
        return Err{fmt::format("the deployment key belongs to deployment `{}`, "
                               "not to `{}`",
                               principal.deployment.id, expected)};
      }
      return DeploymentIdentity{std::move(principal.org_id),
                                std::move(principal.deployment.workspace_id),
                                std::move(principal.deployment.id)};
    },
    [&](UserPrincipal const&) -> Result<DeploymentIdentity, std::string> {
      return Err{fmt::format("the platform takes the deployment key for a "
                             "user's, not for deployment `{}`'s",
                             expected)};
    },
    [](NoPrincipal const&) -> Result<DeploymentIdentity, std::string> {
      return Err{std::string{"the platform does not know the deployment key"}};
    });
}

auto deployment_platform() -> Option<PlatformConnection> {
  auto lock = std::scoped_lock{platform_mutex};
  return platform;
}

auto set_deployment_platform(Option<PlatformConnection> connection) -> void {
  auto lock = std::scoped_lock{platform_mutex};
  platform = std::move(connection);
}

auto read_deployment_key(std::string const& path) -> Option<std::string> {
  auto error = std::error_code{};
  if (not std::filesystem::is_regular_file(path, error) or error) {
    return {};
  }
  auto file = std::ifstream{path, std::ios::binary};
  auto contents = std::string(4097, '\0');
  file.read(contents.data(), contents.size());
  contents.resize(file.gcount());
  if (file.bad() or contents.size() > 4096) {
    return {};
  }
  auto value = std::string{detail::trim(contents)};
  if (value.empty() or value.find_first_of("\r\n") != std::string::npos) {
    return {};
  }
  return value;
}

auto connect_platform_api(PlatformConnection const& connection)
  -> Task<Result<Box<Transport>, std::string>> {
  auto discovered = co_await discover_api_url(connection.platform_url);
  if (discovered.is_err()) {
    co_return Err{std::move(discovered).unwrap_err()};
  }
  auto key = co_await folly::coro::detachOnCancel(
    spawn_blocking([path = connection.key_file] {
      return read_deployment_key(path);
    }));
  if (not key) {
    co_return Err{fmt::format("the deployment key in `{}` is unavailable or "
                              "invalid",
                              connection.key_file)};
  }
  // The generated calls spell out the `/api` that the discovered URL already
  // ends in.
  auto origin = std::move(discovered).unwrap();
  while (origin.ends_with('/')) {
    origin.pop_back();
  }
  if (origin.ends_with("/api")) {
    origin.resize(origin.size() - 4);
  }
  co_return Box<Transport>::from_non_null(
    std::make_unique<BufferedHttpTransport>(std::move(origin),
                                            std::move(*key)));
}

auto PlatformConnection::confirmed_as(DeploymentIdentity identity)
  -> PlatformConnection {
  auto result = PlatformConnection{};
  result.deployment_id = std::string{identity.deployment_id.to_string()};
  result.confirmed = Arc<Mutex<Option<DeploymentIdentity>>>{
    std::in_place, Option<DeploymentIdentity>{std::move(identity)}};
  return result;
}

auto PlatformConnection::identity() const
  -> Task<Result<DeploymentIdentity, std::string>> {
  if (auto known = *co_await confirmed->lock()) {
    co_return std::move(*known);
  }
  auto transport = co_await connect_platform_api(*this);
  if (transport.is_err()) {
    co_return Err{std::move(transport).unwrap_err()};
  }
  auto api = std::move(transport).unwrap();
  co_return co_await identity(*api);
}

auto PlatformConnection::identity(Transport& transport) const
  -> Task<Result<DeploymentIdentity, std::string>> {
  if (auto known = *co_await confirmed->lock()) {
    co_return std::move(*known);
  }
  auto expected = DeploymentId::make(deployment_id);
  if (expected.is_err()) {
    co_return Err{fmt::format("`{}` is not a deployment id", deployment_id)};
  }
  auto looked_up
    = co_await read_deployment_identity(transport, expected.unwrap());
  if (looked_up.is_ok()) {
    *co_await confirmed->lock() = looked_up.unwrap();
  }
  co_return looked_up;
}

} // namespace tenzir
