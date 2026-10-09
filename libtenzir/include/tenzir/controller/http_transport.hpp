// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/controller/transport.hpp>

namespace tenzir {

/// Buffered authenticated HTTP, with a two-second deadline and bounded replies.
class BufferedHttpTransport final : public Transport {
public:
  BufferedHttpTransport(std::string api_url, std::string key);
  auto request(HttpRequest request)
    -> Task<Result<HttpResponse, RequestError>> override;

private:
  std::string api_url_;
  std::string key_;
};

} // namespace tenzir
