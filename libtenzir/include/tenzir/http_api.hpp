//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/detail/stable_map.hpp>
#include <tenzir/type.hpp>

#include <caf/actor_addr.hpp>
#include <caf/expected.hpp>
#include <caf/optional.hpp>

#include <string>

namespace tenzir {

enum class http_method : uint8_t {
  get,
  post,
  put,
  head,
  delete_,
  options,
};

auto http_method_from_string(const std::string& str)
  -> caf::expected<http_method>;

enum class http_content_type : uint16_t {
  json,
  ldjson,
};

enum class http_status_code : uint16_t {
  ok = 200,
  bad_request = 400,
  unprocessable_entity = 422,
  internal_server_error = 500,
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

  /// Expected parameters, if any.
  // Note that the node will currently only forward basic types and
  // lists of basic types as parameters.
  std::optional<tenzir::record_type> params;

  /// Version for that endpoint.
  api_version version;

  /// Response content type.
  http_content_type content_type;

  template <class Inspector>
  friend auto inspect(Inspector& f, rest_endpoint& e) {
    auto params = e.params ? type{*e.params} : type{};
    auto cb = [&] {
      e.params
        = params ? as<record_type>(params) : std::optional<record_type>{};
      return true;
    };
    return f.object(e)
      .pretty_name("tenzir.rest_endpoint")
      .on_load(cb)
      .fields(f.field("endpoint-id", e.endpoint_id),
              f.field("method", e.method), f.field("path", e.path),
              f.field("params", params), f.field("version", e.version),
              f.field("content-type", e.content_type));
  }
};

struct rest_response {
  rest_response() = default;

  /// Create a response from a `tenzir::record`.
  explicit rest_response(const tenzir::record& data);

  /// Create a response from a JSON string.
  static auto from_json_string(std::string json) -> rest_response;

  /// Returns an error that uses `{error: "{message}"}` as the response body.
  static auto make_error(uint16_t error_code, std::string_view message,
                         caf::error detail = {}) -> rest_response;
  static auto make_error(uint16_t error_code, const caf::error& message,
                         caf::error detail = {}) -> rest_response {
    return make_error(error_code, fmt::to_string(message), std::move(detail));
  }

  /// Returns an error that uses `body` as the response body.
  static auto make_error_raw(uint16_t error_code, std::string body,
                             caf::error detail = {}) -> rest_response;

  auto is_error() const -> bool;
  auto body() const -> const std::string&;
  auto code() const -> size_t;
  auto error_detail() const -> const caf::error&;
  auto release() && -> std::string;

  template <class Inspector>
  friend auto inspect(Inspector& f, rest_response& r) {
    return f.object(r)
      .pretty_name("tenzir.rest_response")
      .fields(f.field("code", r.code_), f.field("body", r.body_),
              f.field("detail", r.detail_));
  }

private:
  // The HTTP status code.
  size_t code_ = 200;

  // The response body
  std::string body_ = "{}";

  // Whether this is an error response. We can't just check `code_` because
  // HTTP defines many different "success" values, and we can't just check
  // `detail_` because some call sites may not be able to provide a detailed
  // error.
  bool is_error_ = false;

  // For log messages, debugging, etc. Not returned to the client.
  caf::error detail_ = {};
};

/// Used for serializing an incoming request to be able to send it as a caf
/// message.
class http_request_description {
public:
  /// Unique identification of the request endpoint.
  std::string canonical_path;

  /// Query parameters, path parameters, x-www-urlencoded body parameters
  // TODO: Currently this is unused since platform plugin only accepts
  // json bodies, but it will become necessary once the web plugin
  // also goes through the node.
  // detail::stable_map<std::string, std::string> params;

  /// The POST JSON body, if it existed.
  std::string json_body;

  template <class Inspector>
  friend auto inspect(Inspector& f, http_request_description& e) {
    return f.object(e)
      .pretty_name("tenzir.http_request_description")
      .fields(f.field("canonical_path", e.canonical_path),
              f.field("json_body", e.json_body));
  }
};

/// Structured parameter data.
struct http_parameter_map {
  http_parameter_map() = default;
  http_parameter_map(const http_parameter_map&) = default;
  http_parameter_map(http_parameter_map&&) = default;
  auto operator=(const http_parameter_map&) -> http_parameter_map& = default;
  auto operator=(http_parameter_map&&) -> http_parameter_map& = default;
  ~http_parameter_map() = default;

  static auto from_json(std::string_view) -> caf::expected<http_parameter_map>;

  /// Access to the internal data.
  [[nodiscard]] auto params() const
    -> const detail::stable_map<std::string, tenzir::data>&;

  /// Insert a new key and value.
  auto emplace(std::string&& key, tenzir::data&& value) -> void;

  /// Unchecked access to the internal data. Used by unit tests.
  auto get_unsafe() -> detail::stable_map<std::string, tenzir::data>&;

private:
  /// Partially parsed request parameters.
  // Contains the combined request parameters from all sources (ie. query
  // parameters, path parameters, body parameters) The web server is responsible
  // for deciding if and how duplicates are merged or rejected.
  //
  // The key is the parameter name, the value is a "mildly parsed" version of
  // the original request parameter. In particular, if the incoming data was a
  // JSON POST body then the object structure is retained, nulls are discarded,
  // and all other values are passed as string. For example:
  //
  //      {"foo": "T",
  //       "bar": ["x", "y"],
  //       "baz": 3}
  //
  //  -> stable_map{
  //      {"foo"s, "T"s},
  //      {"bar"s, list{"x"s, "y"s}},
  //      {"baz"s, "3"s}}
  //
  // The leaf values are kept as unparsed strings since the server does not have
  // the requisite type information to parse the JSON correctly. On the other
  // hand, we don't require actual JSON objects since we also can't safely
  // convert query parameters into the correct JSON representation without the
  // type information.
  // http_parameter_map params;
  detail::stable_map<std::string, tenzir::data> params_;

  template <class Inspector>
  friend auto inspect(Inspector& f, http_parameter_map& pm) {
    return f.object(pm)
      .pretty_name("tenzir.http_parameter_map")
      .fields(f.field("params", pm.params_));
  }
};

/// Go through the provided parameters; discard those that are not understood by
/// the endpoint and attempt to parse the rest to the expected type.
auto parse_endpoint_parameters(const tenzir::rest_endpoint& endpoint,
                               const http_parameter_map& params)
  -> caf::expected<tenzir::record>;

} // namespace tenzir

template <>
struct fmt::formatter<tenzir::http_method> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::http_method& value, FormatContext& ctx) const {
    std::string value_string;
    switch (value) {
      case tenzir::http_method::get:
        value_string = "GET";
        break;
      case tenzir::http_method::post:
        value_string = "POST";
        break;
      case tenzir::http_method::put:
        value_string = "PUT";
        break;
      case tenzir::http_method::delete_:
        value_string = "DELETE";
        break;
      case tenzir::http_method::options:
        value_string = "OPTIONS";
        break;
      case tenzir::http_method::head:
        value_string = "HEAD";
        break;
    }
    return formatter<std::string_view>{}.format(value_string, ctx);
  }
};

template <>
struct fmt::formatter<tenzir::api_version> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::api_version& value, FormatContext& ctx) const {
    std::string value_string;
    switch (value) {
      case tenzir::api_version::v0:
        value_string = "v0";
        break;
    }
    return formatter<std::string_view>{}.format(value_string, ctx);
  }
};
