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

  void append(std::string body) override;

  void abort(uint16_t error_code, std::string message) override;

  /// Return the full response body.
  caf::expected<std::string> release() &&;

private:
  caf::expected<std::string> body_;
  caf::typed_response_promise<std::string> promise_;
};

} // namespace vast::detail
