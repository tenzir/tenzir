//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution.hpp"

#include <arrow/util/utf8.h>

namespace tenzir {

auto resolved_secret_value::utf8_view() const
  -> std::optional<std::string_view> {
  const auto valid_utf8 = arrow::util::ValidateUTF8(
    reinterpret_cast<const uint8_t*>(value_.data()), value_.size());
  if (not valid_utf8) {
    return std::nullopt;
  }
  return std::string_view{
    reinterpret_cast<const char*>(value_.data()),
    value_.size(),
  };
}

auto resolved_secret_value::utf8_view(std::string_view name, location loc,
                                      diagnostic_handler& dh) const
  -> failure_or<std::string_view> {
  auto r = utf8_view();
  if (not r) {
    diagnostic::error("expected secret `{}` to be a UTF-8 string", name)
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  return *r;
}

namespace detail {

auto secret_resolved_setter_callback(resolved_secret_value& out) {
  return [&out](resolved_secret_value v) -> failure_or<void> {
    out = std::move(v);
    return {};
  };
}

auto secret_string_setter_callback(std::string name, tenzir::location loc,
                                   std::string& out, diagnostic_handler& dh)
  -> secret_request_callback {
  return [name, loc, &out, &dh](resolved_secret_value v) -> failure_or<void> {
    TRY(auto str, v.utf8_view(name, loc, dh));
    out = std::string{str};
    return {};
  };
}

auto secret_string_setter_callback(std::string name, tenzir::location loc,
                                   located<std::string>& out,
                                   diagnostic_handler& dh)
  -> secret_request_callback {
  return [name, loc, &out, &dh](resolved_secret_value v) -> failure_or<void> {
    TRY(auto str, v.utf8_view(name, loc, dh));
    out = located{std::string{str}, loc};
    return {};
  };
}

} // namespace detail

/// A secret request that will invoke `callback` on successful resolution
secret_request::secret_request(tenzir::secret secret, tenzir::location loc,
                               resolved_secret_value& out)
  : secret{std::move(secret)},
    location{loc},
    callback{detail::secret_resolved_setter_callback(out)} {
}

secret_request::secret_request(const located<tenzir::secret>& secret,
                               resolved_secret_value& out)
  : secret{std::move(secret.inner)},
    location{secret.source},
    callback{detail::secret_resolved_setter_callback(out)} {
}

auto secret_censor::censor(std::string text) const -> std::string {
  for (const auto& s : secrets) {
    const auto v = std::string_view{
      reinterpret_cast<const char*>(s.blob().data()), s.blob().size()};
    for (auto p = text.find(v); p != text.npos; p = text.find(v, p)) {
      text.replace(p, v.size(), "***");
    }
  }
  return text;
}

auto secret_censor::censor(const arrow::Status& status) const -> std::string {
  return censor(status.ToStringWithoutContextLines());
}

auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         std::string& out, diagnostic_handler& dh)
  -> secret_request {
  return {s, loc,
          detail::secret_string_setter_callback(std::move(name), loc, out, dh)};
}

auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         located<std::string>& out, diagnostic_handler& dh)
  -> secret_request {
  return {s, loc,
          detail::secret_string_setter_callback(std::move(name), loc, out, dh)};
}

auto make_secret_request(std::string name, const located<secret>& s,
                         located<std::string>& out, diagnostic_handler& dh)
  -> secret_request {
  return secret_request{s, detail::secret_string_setter_callback(
                             std::move(name), s.source, out, dh)};
}

auto make_secret_request(std::string name, const located<secret>& s,
                         std::string& out, diagnostic_handler& dh)
  -> secret_request {
  return secret_request{s, detail::secret_string_setter_callback(
                             std::move(name), s.source, out, dh)};
}
} // namespace tenzir
