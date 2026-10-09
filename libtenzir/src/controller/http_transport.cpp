// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/controller/http_transport.hpp>
#include <tenzir/curl.hpp>

#include <folly/coro/DetachOnCancel.h>

#include <span>

namespace tenzir {

BufferedHttpTransport::BufferedHttpTransport(std::string api_url,
                                             std::string key)
  : api_url_{std::move(api_url)}, key_{std::move(key)} {
  while (not api_url_.empty() and api_url_.back() == '/') {
    api_url_.pop_back();
  }
}

auto BufferedHttpTransport::request(HttpRequest request)
  -> Task<Result<HttpResponse, RequestError>> {
  // Own every input in the blocking task: cancellation may outlive its caller.
  return folly::coro::detachOnCancel(spawn_blocking(
    [url = api_url_ + request.path, key = key_,
     request = std::move(request)] -> Result<HttpResponse, RequestError> {
      auto handle = curl::easy{};
      auto body = std::string{};
      handle.set(CURLOPT_URL, url);
      handle.set(CURLOPT_CUSTOMREQUEST, request.method);
      handle.set(CURLOPT_POSTFIELDS, request.body);
      handle.set_postfieldsize(static_cast<long>(request.body.size()));
      handle.set(CURLOPT_TIMEOUT_MS, 2000L);
      handle.set(CURLOPT_CONNECTTIMEOUT_MS, 1000L);
      handle.set(CURLOPT_FOLLOWLOCATION, 0L);
      handle.set(CURLOPT_NOSIGNAL, 1L);
      handle.set(CURLOPT_NOPROXY, "localhost,127.0.0.0/8,::1");
      handle.set_http_header("x-tenzir-deployment-key", key);
      for (auto const& [name, value] : request.headers) {
        if (name != "x-tenzir-deployment-key") {
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
        return Err{RequestError{std::string{curl::to_string(result)}}};
      }
      auto status = handle.get<curl::easy::info::response_code>().second;
      return HttpResponse{static_cast<uint16_t>(status), std::move(body)};
    }));
}

} // namespace tenzir
