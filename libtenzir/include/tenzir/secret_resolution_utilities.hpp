//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once
#include "tenzir/aliases.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/operator_control_plane.hpp"

#include <arrow/util/uri.h>

/// This is separate header from `secret_resolution.hpp` in order to not have to
/// pull all of `data` into `operator_control_plane`. Having this as a free
/// function in  separate header allows users to only include this if they need
/// the resolution utility for records

namespace tenzir {

using record_secret_request_callback
  = std::function<void(std::string_view key, resolved_secret_value value)>;

struct secret_request_record {
  record value;
  struct location location;
  record_secret_request_callback callback;
  secret_censor* censor = nullptr;
};

using secret_request_combined = variant<secret_request, secret_request_record>;

/// Creates a secret request that will invoke `callback` for every (key,secret)
/// pair in `r` on successfully resolution.
/// @relates operator_control_plane::resolve_secrets_must_yield
auto make_secret_request(record r, location loc,
                         record_secret_request_callback callback,
                         secret_censor* censor = nullptr)
  -> secret_request_combined;

/// Creates a secret request that will invoke `callback` for every (key,secret)
/// pair in `r` on successfully resolution.
/// @relates operator_control_plane::resolve_secrets_must_yield
auto make_secret_request(const located<record>& r,
                         record_secret_request_callback callback,
                         secret_censor* censor = nullptr)
  -> secret_request_combined;

/// Creates a secret request that will set `uri`. The secret URI is validated
/// as utf8 and potentially prepended with `prefix`.
/// @relates operator_control_plane::resolve_secrets_must_yield
auto make_request(secret s, location loc, std::string prefix,
                  arrow::util::Uri& uri, diagnostic_handler& dh,
                  secret_censor* censor = nullptr) -> secret_request;

/// Creates a secret request that will set `uri`. The secret URI is validated
/// as utf8 and potentially prepended with `prefix`.
/// @relates operator_control_plane::resolve_secrets_must_yield
auto make_uri_request(const located<secret>& s, std::string prefix,
                      arrow::util::Uri& uri, diagnostic_handler& dh,
                      secret_censor* censor = nullptr) -> secret_request;

/// A helper function that is able to resolve records in place
/// @relates operator_control_plane::resolve_secrets_must_yield
auto resolve_secrets_must_yield(operator_control_plane& ctrl,
                                std::vector<secret_request_combined> requests)
  -> bool;
} // namespace tenzir
