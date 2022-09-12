//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/data.hpp>
#include <vast/type.hpp>

#include <caf/actor_addr.hpp>
#include <caf/meta/annotation.hpp>
#include <caf/optional.hpp>

#include <string>

namespace vast {

enum class http_method : uint8_t {
  get,
  post,
};

enum class http_content_type : uint16_t {
  json,
  ldjson,
};

enum class http_status_code : uint16_t {
  bad_request = 400,
  unprocessable_entity = 422,
};

enum class api_version : uint8_t {
  v0,
  latest = v0,
};

struct api_endpoint {
  /// Arbitrary id for endpoint identification
  uint64_t endpoint_id = 0ull;

  /// The HTTP verb of this endpoint
  http_method method;

  /// Path can use the express.js conventions
  std::string path;

  /// Expected parameters.
  //  (A record_type cannot be empty, so we need an optional.
  //  TODO: Use `std::optional` after the upgrade to CAF 0.18.
  caf::optional<vast::record_type> params;

  /// Version for that endpoint.
  //  TODO: Maybe this would be better as a `uint8` to allow
  //  each plugin to use its own version numbers.
  api_version version;

  /// Response content type.
  http_content_type content_type;

  template <class Inspector>
  friend auto inspect(Inspector& f, api_endpoint& e) {
    return f(caf::meta::type_name("vast.api_endpoint"), e.endpoint_id, e.method,
             e.path, e.params, e.version, e.content_type);
  }
};

// We use the virtual inheritance as a compilation firewall to
// avoid having the dependency on restinio creep into main VAST
// until we gained a bit more implementation experience and are
// confident that it is what we want in the long term.
class http_response {
public:
  virtual ~http_response() = default;

  /// Append data to the response body.
  virtual void append(std::string body) = 0;

  /// Return an HTTP error code and close the connection.
  virtual void abort(uint16_t error_code, std::string message) = 0;
};

class http_request {
public:
  /// Data according to the type of the endpoint.
  vast::record params;

  /// Shared response data.
  // TODO: Probably makes sense to use a `caf::cow_ptr`.
  std::shared_ptr<http_response> response;
};

} // namespace vast
