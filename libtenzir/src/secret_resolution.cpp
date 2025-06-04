//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution.hpp"

namespace tenzir {

namespace detail {
auto secret_setter_callback(std::string name, tenzir::location loc,
                            std::string& out, diagnostic_handler& dh)
  -> secret_request_callback {
  return [name, loc, &out, &dh](resolved_secret_value v) {
    out = std::string{v.utf8_view(name, loc, dh)};
  };
}

auto secret_setter_callback(std::string name, tenzir::location loc,
                            located<std::string>& out, diagnostic_handler& dh)
  -> secret_request_callback {
  return [name, loc, &out, &dh](resolved_secret_value v) {
    out = located{std::string{v.utf8_view(name, loc, dh)}, loc};
  };
}
} // namespace detail

auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         std::string& out, diagnostic_handler& dh)
  -> secret_request {
  return {s, loc,
          detail::secret_setter_callback(std::move(name), loc, out, dh)};
}

auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         located<std::string>& out, diagnostic_handler& dh)
  -> secret_request {
  return {s, loc,
          detail::secret_setter_callback(std::move(name), loc, out, dh)};
}

auto make_secret_request(std::string name, const located<secret>& s,
                         located<std::string>& out, diagnostic_handler& dh)
  -> secret_request {
  return secret_request{s, detail::secret_setter_callback(std::move(name),
                                                          s.source, out, dh)};
}

auto make_secret_request(std::string name, const located<secret>& s,
                         std::string& out, diagnostic_handler& dh)
  -> secret_request {
  return secret_request{s, detail::secret_setter_callback(std::move(name),
                                                          s.source, out, dh)};
}
} // namespace tenzir
