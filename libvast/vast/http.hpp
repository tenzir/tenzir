//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/uri.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace vast::http {

struct header {
  std::string name;
  std::string value;
};

/// Base for HTTP messages.
struct message {
  std::string protocol;
  double version;
  std::vector<http::header> headers;
  std::string body;

  [[nodiscard]] const http::header* header(const std::string& name) const;
};

/// A HTTP request message.
struct request : message {
  std::string method;
  vast::uri uri;
};

/// A HTTP response message.
struct response : message {
  uint32_t status_code;
  std::string status_text;
};

} // namespace vast::http
