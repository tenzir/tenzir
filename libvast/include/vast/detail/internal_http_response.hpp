//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/http_api.hpp>

#include <caf/typed_response_promise.hpp>

namespace vast::detail {

class internal_http_response : public http_response {
public:
  internal_http_response(caf::typed_response_promise<std::string>);
  ~internal_http_response() override;

  internal_http_response(const internal_http_response&) = delete;
  internal_http_response(internal_http_response&&) = default;
  auto operator=(const internal_http_response&)
    -> internal_http_response& = delete;
  auto operator=(internal_http_response&&) -> internal_http_response& = default;

  void append(std::string body) override;

  void
  abort(uint16_t error_code, std::string message, caf::error detail) override;

  /// Return the full response body.
  auto release() && -> caf::expected<std::string>;

private:
  caf::expected<std::string> body_;
  caf::typed_response_promise<std::string> promise_;
};

} // namespace vast::detail
