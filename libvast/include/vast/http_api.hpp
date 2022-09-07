//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// FIXME: Figure out what to do with `http.hpp`

#pragma once

#include <vast/data.hpp>
#include <vast/type.hpp>

#include <caf/actor_addr.hpp>

#include <string>

namespace vast {

// TODO: Move HTTP support classes into separate headers
enum class http_method {
  get,
  post,
};

enum class http_content_type {
  json,
  arrow,
};

enum class http_status_code {
  bad_request = 400,
  unprocessable_entity = 422,
};

// We use the virtual inheritance as a compilation firewall to
// avoid having the dependency on restinio creep into main VAST
// until we gained a bit more implementation experience and are
// confident that it's what we want in the long term.
class http_response {
public:
  virtual ~http_response() = default;

  /// Add a custom header field.
  // TODO: Is it a good idea to expose this?
  // virtual void add_header() = 0;

  /// Append data to the response body.
  virtual void append(std::string_view body) = 0;

  /// Return an error and close the connection.
  //  TODO: Statically verify that we can only abort
  //  with the documented error codes.
  virtual void abort(uint16_t error_code, std::string_view message) = 0;

  /// Flush the response body.
  // (This may be useful from time to time, but only in limited
  //  circumstances: For non-chunked responses, all response headers
  //  must be set before the first flush. For chunked responses,
  //  every chunk can have its own headers. So flushing makes sense
  //  if we know the whole size in advance and want to stream it (ie.
  //  a big video file) or if we don't know the whole size, are using HTTP 1,
  //  and want to send a single chunk.)
  // virtual void flush() = 0;
};

class http_request {
public:
  /// Data according to the type of the endpoint.
  vast::record params;

  /// Request body
  // (may be useful, but currently I can't really think of a use case)
  // std::string body;

  // TODO: Probably use caf::cow_ptr
  std::shared_ptr<http_response> response;
};

} // namespace vast
