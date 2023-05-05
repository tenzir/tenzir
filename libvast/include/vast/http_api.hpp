//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/data.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/detail/stable_map.hpp>
#include <vast/type.hpp>

#include <caf/actor_addr.hpp>
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
  v0 = 0,
  latest = v0,
};

template <class Inspector>
auto inspect(Inspector& f, http_content_type& x) {
  return detail::inspect_enum(f, x);
}

template <class Inspector>
auto inspect(Inspector& f, http_method& x) {
  return detail::inspect_enum(f, x);
}

template <class Inspector>
auto inspect(Inspector& f, api_version& x) {
  return detail::inspect_enum(f, x);
}

struct rest_endpoint {
  /// A string that uniquely identifies this endpoint.
  //  e.g. "POST /query/:id/next (v0)"
  [[nodiscard]] auto canonical_path() const -> std::string;

  /// Arbitrary id for endpoint identification
  //  The node will add the correct value to incoming requests based
  //  on the canonical path; this allows plugin to use a switch
  //  statement to ensure they cover all their endpoints.
  uint64_t endpoint_id = 0ull;

  /// The HTTP verb of this endpoint
  http_method method;

  /// Path can use the express.js conventions
  std::string path;

  /// Expected parameters.
  //  (A record_type cannot be empty, so we need an optional)
  std::optional<vast::record_type> params;

  /// Version for that endpoint.
  api_version version;

  /// Response content type.
  http_content_type content_type;

  template <class Inspector>
  friend auto inspect(Inspector& f, rest_endpoint& e) {
    auto params = e.params ? type{*e.params} : type{};
    auto cb = [&] {
      e.params
        = params ? caf::get<record_type>(params) : std::optional<record_type>{};
      return true;
    };
    return f.object(e)
      .pretty_name("vast.rest_endpoint")
      .on_load(cb)
      .fields(f.field("endpoint-id", e.endpoint_id),
              f.field("method", e.method), f.field("path", e.path),
              f.field("params", params), f.field("version", e.version),
              f.field("content-type", e.content_type));
  }
};

/// Go through the provided parameters; discard those that are not understood by
/// the endpoint and attempt to parse the rest to the expected type.
auto parse_endpoint_parameters(
  const vast::rest_endpoint& endpoint,
  const detail::stable_map<std::string, std::string>& params)
  -> caf::expected<vast::record>;

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
  //  TODO: Add a `&&` qualifier to ensure one-time use.
  virtual void
  abort(uint16_t error_code, std::string message, caf::error detail)
    = 0;
};

class http_request {
public:
  /// Data according to the type of the endpoint.
  vast::record params;

  /// The response corresponding to this request.
  std::shared_ptr<http_response> response;
};

/// Used for serializing an incoming request to be able to send it as a caf
/// message.
class http_request_description {
public:
  // /// The name of the plugin to handle the request.
  std::string canonical_path;

  /// Request parameters. (unvalidated)
  detail::stable_map<std::string, std::string> params;

  template <class Inspector>
  friend auto inspect(Inspector& f, http_request_description& e) {
    return f.object(e)
      .pretty_name("vast.http_request_description")
      .fields(f.field("canonical_path", e.canonical_path),
              f.field("params", e.params));
  }
};

} // namespace vast

template <>
struct fmt::formatter<vast::http_method> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::http_method& value, FormatContext& ctx) const {
    std::string value_string;
    switch (value) {
      case vast::http_method::get:
        value_string = "GET";
        break;
      case vast::http_method::post:
        value_string = "POST";
        break;
    }
    return formatter<std::string_view>{}.format(value_string, ctx);
  }
};

template <>
struct fmt::formatter<vast::api_version> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::api_version& value, FormatContext& ctx) const {
    std::string value_string;
    switch (value) {
      case vast::api_version::v0:
        value_string = "v0";
        break;
    }
    return formatter<std::string_view>{}.format(value_string, ctx);
  }
};
