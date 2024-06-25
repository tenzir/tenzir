//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http.hpp"

#include "tenzir/curl.hpp"
#include "tenzir/detail/string.hpp"

#include <algorithm>
#include <cctype>

namespace tenzir::http {

namespace {} // namespace

auto message::header(const std::string& name) -> struct header* {
  auto pred = [&](auto& x) -> bool {
    if (x.name.size() != name.size())
      return false;
    for (auto i = 0u; i < name.size(); ++i)
      if (::toupper(x.name[i]) != ::toupper(name[i]))
        return false;
    return true;
  };
  auto i = std::find_if(headers.begin(), headers.end(), pred);
  return i == headers.end() ? nullptr : &*i;
}

auto message::header(const std::string& name) const -> const struct header* {
  // We use a const_cast to avoid duplicating logic.
  auto* self = const_cast<message*>(this);
  return self->header(name);
}

auto request_item::parse(std::string_view str) -> std::optional<request_item> {
  auto is_valid_header_name = [](std::string_view name) {
    for (char c : name) {
      if (std::isalnum(static_cast<unsigned char>(c)) == 0 and c != '-'
          and c != '_') {
        return false;
      }
    }
    return true;
  };
  auto xs = detail::split_escaped(str, ":=@", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = file_data_json, .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, ":=", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = data_json, .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, ":", "\\", 1);
  if (xs.size() == 2 and is_valid_header_name(xs[0]))
    return request_item{.type = header, .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "==", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = url_param, .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "=@", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = file_data, .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "@", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = file_form, .key = xs[0], .value = xs[1]};
  xs = detail::split_escaped(str, "=", "\\", 1);
  if (xs.size() == 2)
    return request_item{.type = data, .key = xs[0], .value = xs[1]};
  return {};
}

auto apply(std::vector<request_item> items, request& req) -> caf::error {
  auto body = record{};
  for (auto& item : items) {
    switch (item.type) {
      default:
        return caf::make_error(ec::unimplemented, "unsupported item type");
      case request_item::header: {
        req.headers.emplace_back(std::move(item.key), std::move(item.value));
        break;
      }
      case request_item::data: {
        if (req.method.empty())
          req.method = "POST";
        body.emplace(std::move(item.key), std::move(item.value));
        break;
      }
      case request_item::data_json: {
        if (req.method.empty())
          req.method = "POST";
        auto data = from_json(item.value);
        if (not data)
          return data.error();
        body.emplace(std::move(item.key), std::move(*data));
      }
    }
  }
  auto json_encode = [](const auto& x) {
    auto result = to_json(x, {.oneline = true});
    TENZIR_ASSERT(result);
    return std::move(*result);
  };
  auto url_encode = [](const auto& x) {
    return curl::escape(flatten(x));
  };

  // We assemble an Accept header as we go, unless we have one already.
  auto accept = std::optional<std::vector<std::string>>{};
  if (req.header("Accept") == nullptr) {
    accept = {"*/*"};
  }
  // If the user provided any request body data, we default to JSON encoding.
  // The user can override this behavior by setting a Content-Type header.
  const auto* content_type_header = req.header("Content-Type");
  if (content_type_header != nullptr
      and not content_type_header->value.empty()) {
    // Encode request body based on provided Content-Type header value.
    const auto& content_type = content_type_header->value;
    if (content_type.starts_with("application/x-www-form-urlencoded")) {
      if (not body.empty()) {
        req.body = url_encode(body);
        TENZIR_DEBUG("urlencoded request body: {}", req.body);
      }
    } else if (content_type.starts_with("application/json")) {
      if (not body.empty()) {
        req.body = json_encode(body);
        if (accept)
          accept->insert(accept->begin(), "application/json");
        TENZIR_DEBUG("JSON-encoded request body: {}", req.body);
      }
    } else {
      return caf::make_error(ec::parse_error,
                             fmt::format("cannot encode HTTP request body "
                                         "with Content-Type: {}",
                                         content_type));
    }
  } else if (not body.empty()) {
    // Without a Content-Type, we assume JSON.
    req.body = json_encode(body);
    req.headers.emplace_back("Content-Type", "application/json");
    if (accept)
      accept->insert(accept->begin(), "application/json");
  }
  // Add an Accept header unless we have one already.
  if (accept) {
    auto value = fmt::format("{}", fmt::join(*accept, ", "));
    req.headers.emplace_back("Accept", std::move(value));
  }
  return {};
}

} // namespace tenzir::http
