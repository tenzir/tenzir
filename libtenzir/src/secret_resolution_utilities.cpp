//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution_utilities.hpp"

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

auto resolve_secrets_must_yield(operator_control_plane& ctrl,
                                std::vector<secret_request_combined> requests)
  -> bool {
  auto translated_requests = std::vector<secret_request>{};
  translated_requests.reserve(requests.size());
  for (auto& req : requests) {
    if (auto* v2 = try_as<secret_request>(req)) {
      translated_requests.push_back(std::move(*v2));
      continue;
    }
    auto& record_request = as<secret_request_record>(req);
    const auto handle_value
      = [&record_request, &translated_requests](
          this const auto& self, std::string key, tenzir::data& value) -> void {
      if (auto* s = try_as<secret>(value)) {
        translated_requests.emplace_back(std::move(*s), record_request.location,
                                         [cb = record_request.callback,
                                          key](resolved_secret_value v) {
                                           return cb(key, std::move(v));
                                         });
      }
      if (auto* r = try_as<record>(value)) {
        for (auto& [k, v] : *r) {
          self(key + "." + k, v);
        }
      }
      if (auto* l = try_as<list>(value)) {
        for (size_t i = 0; i < l->size(); ++i) {
          self(key + "[" + std::to_string(i) + "]", l->operator[](i));
        }
      }
    };
    for (auto& [k, v] : record_request.value) {
      handle_value(k, v);
    }
  }
  return ctrl.resolve_secrets_must_yield(std::move(translated_requests));
}
} // namespace tenzir
