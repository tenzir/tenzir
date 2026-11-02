// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/controller/http_transport.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/platform_connection.hpp>
#include <tenzir/tunnel/platform.hpp>

#include <folly/coro/DetachOnCancel.h>

#include <filesystem>
#include <fstream>
#include <mutex>

namespace tenzir {

namespace {

auto platform_mutex = std::mutex{};
auto platform = Option<PlatformConnection>{};

} // namespace

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

} // namespace tenzir
