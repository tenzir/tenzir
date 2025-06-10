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

auto secret_resolved_setter_callback(resolved_secret_value& out,
                                     secret_censor* censor) {
  return [&out, censor](resolved_secret_value v) {
    if (censor) {
      censor->secrets.push_back(v);
    }
    out = std::move(v);
  };
}

auto secret_string_setter_callback(std::string name, tenzir::location loc,
                                   std::string& out, diagnostic_handler& dh,
                                   secret_censor* censor)
  -> secret_request_callback {
  return [name, loc, &out, &dh, censor](resolved_secret_value v) {
    out = std::string{v.utf8_view(name, loc, dh)};
    if (censor) {
      censor->secrets.push_back(std::move(v));
    }
  };
}

auto secret_string_setter_callback(std::string name, tenzir::location loc,
                                   located<std::string>& out,
                                   diagnostic_handler& dh,
                                   secret_censor* censor)
  -> secret_request_callback {
  return [name, loc, &out, &dh, censor](resolved_secret_value v) {
    out = located{std::string{v.utf8_view(name, loc, dh)}, loc};
    if (censor) {
      censor->secrets.push_back(std::move(v));
    }
  };
}

} // namespace detail

/// A secret request that will invoke `callback` on successful resolution
secret_request::secret_request(tenzir::secret secret, tenzir::location loc,
                               resolved_secret_value& out,
                               secret_censor* censor)
  : secret{std::move(secret)},
    location{loc},
    callback{detail::secret_resolved_setter_callback(out, censor)} {
}

secret_request::secret_request(const located<tenzir::secret>& secret,
                               resolved_secret_value& out,
                               secret_censor* censor)
  : secret{std::move(secret.inner)},
    location{secret.source},
    callback{detail::secret_resolved_setter_callback(out, censor)} {
}

auto secret_censor::censor(std::string text) const -> std::string {
  TENZIR_ASSERT(max_size > 0);
  for (const auto& s : secrets) {
    if (s.all_literal() and not censor_literals) {
      continue;
    }
    /// This is a naive implementation with pretty bad complexity. The
    /// assumption is that this is rarely used and usually only in error cases
    /// (e.g. when censoring an arrow::Status::ToString).
    const auto v = std::string_view{
      reinterpret_cast<const char*>(s.blob().data()), s.blob().size()};
    for (size_t length = v.size(); length >= max_size; --length) {
      for (auto start = size_t{0}; start <= v.size() - length; ++start) {
        auto search_start = size_t{0};
        while (true) {
          const auto pos = text.find(v.substr(start, length), search_start);
          if (pos == text.npos) {
            break;
          }
          text.replace(pos, length, 3, '*');
          search_start = pos + 3;
        }
      }
    }
  }
  return text;
}

auto secret_censor::censor(const arrow::Status& status) const -> std::string {
  return censor(status.ToStringWithoutContextLines());
}

auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         std::string& out, diagnostic_handler& dh,
                         secret_censor* censor) -> secret_request {
  return {s, loc,
          detail::secret_string_setter_callback(std::move(name), loc, out, dh,
                                                censor)};
}

auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         located<std::string>& out, diagnostic_handler& dh,
                         secret_censor* censor) -> secret_request {
  return {s, loc,
          detail::secret_string_setter_callback(std::move(name), loc, out, dh,
                                                censor)};
}

auto make_secret_request(std::string name, const located<secret>& s,
                         located<std::string>& out, diagnostic_handler& dh,
                         secret_censor* censor) -> secret_request {
  return secret_request{s, detail::secret_string_setter_callback(
                             std::move(name), s.source, out, dh, censor)};
}

auto make_secret_request(std::string name, const located<secret>& s,
                         std::string& out, diagnostic_handler& dh,
                         secret_censor* censor) -> secret_request {
  return secret_request{s, detail::secret_string_setter_callback(
                             std::move(name), s.source, out, dh, censor)};
}
} // namespace tenzir
