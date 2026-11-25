// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/controller/transport.hpp>

#include <chrono>

namespace tenzir {

/// Buffered authenticated HTTP, with a per-request deadline and bounded
/// replies.
class BufferedHttpTransport final : public Transport {
public:
  /// `timeout` bounds each whole request, from connecting to the last byte of
  /// the response.
  BufferedHttpTransport(std::string api_url, std::string key,
                        std::chrono::milliseconds timeout
                        = std::chrono::seconds{2});
  auto request(HttpRequest request)
    -> Task<Result<HttpResponse, RequestError>> override;

private:
  std::string api_url_;
  std::string key_;
  std::chrono::milliseconds timeout_;
};

} // namespace tenzir
