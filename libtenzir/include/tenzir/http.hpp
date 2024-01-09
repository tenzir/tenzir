//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/uri.hpp"

#include <caf/error.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace tenzir::http {

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

  [[nodiscard]] auto header(const std::string& name) const
    -> const http::header*;
};

/// A HTTP request message.
struct request : message {
  std::string method;
  tenzir::uri uri;
};

/// A HTTP response message.
struct response : message {
  uint32_t status_code;
  std::string status_text;
};

/// A HTTPie-inspired request item.
struct request_item {
  /// Parses a request item like HTTPie.
  static auto parse(std::string_view str) -> std::optional<request_item>;

  enum item_type : uint8_t {
    file_data_json,
    data_json,
    url_param,
    file_data,
    file_form,
    data,
    header,
  };

  friend auto inspect(auto& f, request_item& x) -> bool {
    using enum_type = std::underlying_type_t<item_type>;
    return f.object(x)
      .pretty_name("tenzir.http.request_item")
      .fields(f.field("type", reinterpret_cast<enum_type&>(x.type)),
              f.field("key", x.key), f.field("value", x.value));
  }

  item_type type;
  std::string key;
  std::string value;
};

/// Applies a list of request items to a given HTTP request.
/// We mimic HTTPie's behavior in processing request items.
auto apply(std::vector<request_item> items, request& req) -> caf::error;

} // namespace tenzir::http
