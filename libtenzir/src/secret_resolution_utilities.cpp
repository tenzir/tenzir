//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution_utilities.hpp"

#include "tenzir/data.hpp"

namespace tenzir {

auto make_secret_request(record r, location loc,
                         record_secret_request_callback callback)
  -> secret_request_combined {
  return secret_request_combined{std::in_place_type<secret_request_record>,
                                 std::move(r), loc, std::move(callback)};
}

auto make_secret_request(const located<record>& r,
                         record_secret_request_callback callback)
  -> secret_request_combined {
  return secret_request_combined{std::in_place_type<secret_request_record>,
                                 std::move(r.inner), r.source,
                                 std::move(callback)};
}

namespace {

auto arrow_uri_callback(std::string prefix, arrow::util::Uri& uri,
                        diagnostic_handler& dh, location loc)
  -> secret_request_callback {
  return [&uri, &dh, loc, prefix = std::move(prefix)](
           resolved_secret_value v) -> failure_or<void> {
    TRY(auto sv, v.utf8_view("uri", loc, dh));
    auto str = std::string{sv};
    if (not str.starts_with(prefix)) {
      str.insert(0, prefix);
    }
    const auto parse_result = uri.Parse(str);
    if (not parse_result.ok()) {
      diagnostic::error("failed to parse uri").primary(loc).emit(dh);
      return failure::promise();
    };
    return {};
  };
}

} // namespace

auto make_uri_request(const located<secret>& s, std::string prefix,
                      arrow::util::Uri& uri, diagnostic_handler& dh)
  -> secret_request {
  return secret_request{s, arrow_uri_callback(std::move(prefix), uri, dh,
                                              s.source)};
}

} // namespace tenzir
