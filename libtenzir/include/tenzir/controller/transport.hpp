//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// The seam between the generated API bindings and however bytes actually
/// travel, and the three failures no endpoint declares. `generated/api.hpp` is
/// written against this and nothing else, so the same bindings work over the
/// deployment tunnel and authenticated buffered HTTP.

#pragma once

#include <tenzir/async/task.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir {

/// The request never produced a response.
///
/// What TypeScript calls a `RequestError`, and defined here for the same
/// reason it lives in `HttpClientError` there: it is the transport's own
/// failure, not one the API declares. Only a transport raises it, so what it
/// can say is up to whoever wrote one.
class RequestError {
public:
  explicit RequestError(std::string message) : message{std::move(message)} {
  }

  std::string message;
};

/// Why a value did not decode.
///
/// One type for every decode, the way Effect has a single `ParseError`
/// whatever is being parsed: a field, a whole struct, or a response body. It
/// carries the schema it failed against rather than a tree locating the
/// failure, which is enough to read and not enough to react to. Add the path
/// the day something needs to.
class ParseError {
public:
  ParseError(std::string schema, std::string reason)
    : schema{std::move(schema)}, reason{std::move(reason)} {
  }

  std::string schema;
  std::string reason;
};

/// A response arrived that could not be turned into a declared outcome.
///
/// `decode` is a status the API does not declare: something out there
/// answered on its own behalf. `status_code` is one it declares with no body
/// to read. `empty_body` is a body that should have been there and was not.
/// The names are TypeScript's, so that a reader of both finds the same words.
class ResponseError {
public:
  enum class Reason { status_code, decode, empty_body };

  ResponseError(Reason reason, std::uint16_t status, std::string body)
    : reason{reason}, status{status}, body{std::move(body)} {
  }

  Reason reason;
  std::uint16_t status;
  std::string body;
};

/// One exchange, in either direction, independent of the carrier.
///
/// Named for the API rather than for a transport, because the transport is not
/// the same everywhere: the server adapter turns an arriving request into
/// this, and `call` hands this to a transport on the way out. Whole bodies,
/// because that is the least any carrier can do: a tunnel frames whole
/// bodies and matches responses by id, and HTTP can always be reduced to it.
class HttpRequest {
public:
  HttpRequest(std::string method, std::string path, std::string body,
              std::vector<std::pair<std::string, std::string>> headers = {})
    : method{std::move(method)},
      path{std::move(path)},
      body{std::move(body)},
      headers{std::move(headers)} {
  }

  auto header(std::string_view name) const -> Option<std::string_view> {
    for (auto const& [key, value] : headers) {
      if (key == name) {
        return std::string_view{value};
      }
    }
    return None{};
  }

  std::string method;
  std::string path;
  std::string body;
  std::vector<std::pair<std::string, std::string>> headers;
};

class HttpResponse {
public:
  HttpResponse(std::uint16_t status, std::string body)
    : status{status}, body{std::move(body)} {
  }

  std::uint16_t status;
  std::string body;
};

/// Carries a request to the platform and brings an answer back.
///
/// Abstract on purpose: the generated `call` overloads take this by reference
/// and never learn whether they are speaking over a WebSocket or a socket.
class Transport {
public:
  virtual ~Transport() = default;

  /// Fails with `RequestError` and nothing else: a transport either delivered
  /// and got bytes back or it did not, and it has no opinion about statuses.
  ///
  /// An empty body is no body. An encoded payload is never empty, so an
  /// endpoint that declares one and an endpoint that does not stay apart.
  virtual auto request(HttpRequest request)
    -> Task<Result<HttpResponse, RequestError>>
    = 0;
};

} // namespace tenzir
