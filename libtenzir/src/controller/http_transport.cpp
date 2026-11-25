// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/config.hpp>
#include <tenzir/controller/http_transport.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/tunnel/platform.hpp>

#include <folly/coro/DetachOnCancel.h>

#include <span>

namespace tenzir {

namespace {

/// Whether a request reached the platform itself. Over TLS, that is when the
/// handshake with it completed, which holds through a proxy too. Without TLS,
/// it is when a connection was made that was not to a proxy: through one,
/// the connection says nothing about whether the platform answers.
auto reached_platform(curl::easy& handle, bool tls) -> bool {
  if (tls) {
    return handle.get<curl::easy::info::appconnect_time_t>().second > 0;
  }
  auto connected = handle.get<curl::easy::info::connect_time_t>().second > 0;
#if LIBCURL_VERSION_NUM >= 0x080700
  return connected and handle.get<curl::easy::info::used_proxy>().second == 0;
#else
  // Before curl 8.7, which proxy a request went through is unknown, so a
  // connection counts as one to the platform.
  return connected;
#endif
}

/// How a request failed. A timeout before the request reached the platform
/// counts as unreachable, as when its address silently drops packets after
/// the platform moved, or a proxy cannot reach it.
auto kind_of(curl::easy::code code, bool reached) -> RequestError::Kind {
  switch (code) {
    case curl::easy::code::operation_timedout:
      return reached ? RequestError::Kind::timeout
                     : RequestError::Kind::unreachable;
    case curl::easy::code::couldnt_resolve_host:
    case curl::easy::code::couldnt_connect:
      return RequestError::Kind::unreachable;
    default:
      return RequestError::Kind::other;
  }
}

} // namespace

BufferedHttpTransport::BufferedHttpTransport(std::string api_url,
                                             std::string key,
                                             std::chrono::milliseconds timeout)
  : api_url_{std::move(api_url)}, key_{std::move(key)}, timeout_{timeout} {
  while (not api_url_.empty() and api_url_.back() == '/') {
    api_url_.pop_back();
  }
}

auto BufferedHttpTransport::request(HttpRequest request)
  -> Task<Result<HttpResponse, RequestError>> {
  // Own every input in the blocking task: cancellation may outlive its caller.
  return folly::coro::detachOnCancel(spawn_blocking(
    [url = api_url_ + request.path, key = key_, timeout = timeout_,
     request = std::move(request)] -> Result<HttpResponse, RequestError> {
      auto handle = curl::easy{};
      auto body = std::string{};
      handle.set(CURLOPT_URL, url);
      handle.set(CURLOPT_CUSTOMREQUEST, request.method);
      handle.set(CURLOPT_POSTFIELDS, request.body);
      handle.set_postfieldsize(static_cast<long>(request.body.size()));
      handle.set(CURLOPT_TIMEOUT_MS, static_cast<long>(timeout.count()));
      handle.set(CURLOPT_CONNECTTIMEOUT_MS, 1000L);
      handle.set(CURLOPT_FOLLOWLOCATION, 0L);
      handle.set(CURLOPT_NOSIGNAL, 1L);
      handle.set(CURLOPT_NOPROXY, "localhost,127.0.0.0/8,::1");
      handle.set_http_header(deployment_key_header, key);
      handle.set_http_header(deployment_version_header, version::version);
      for (auto const& [name, value] : request.headers) {
        if (name != deployment_key_header) {
          handle.set_http_header(name, value);
        }
      }
      handle.set_write_result_callback([&](std::span<std::byte const> data) {
        constexpr auto max_reply = std::size_t{64} << 10;
        if (data.size() > max_reply - body.size()) {
          return std::size_t{0};
        }
        body.append(reinterpret_cast<char const*>(data.data()), data.size());
        return data.size();
      });
      auto result = handle.perform();
      if (result != curl::easy::code::ok) {
        auto reached = reached_platform(handle, url.starts_with("https://"));
        return Err{RequestError{std::string{curl::to_string(result)},
                                kind_of(result, reached)}};
      }
      auto status = handle.get<curl::easy::info::response_code>().second;
      return HttpResponse{static_cast<uint16_t>(status), std::move(body)};
    }));
}

} // namespace tenzir
