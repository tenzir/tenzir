//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution.hpp"

#include "tenzir/curl.hpp"
#include "tenzir/detail/base58.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/hex_encode.hpp"

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
  constexpr static auto replacement = std::string_view{"***"};
  auto single_star = false;
  auto double_star = false;
  for (const auto& s : secrets) {
    const auto v
      = std::string_view{reinterpret_cast<const char*>(s.data()), s.size()};
    if (v.empty()) {
      continue;
    }
    if (v == "*") {
      single_star = true;
      continue;
    }
    if (v == "**") {
      double_star = true;
      continue;
    }
    if (v == "***") {
      continue;
    }
    for (auto p = text.find(v); p < text.size(); p = text.find(v, p)) {
      text.replace(p, v.size(), replacement);
      p += replacement.size();
    }
  }
  if (single_star or double_star) {
    for (auto p = text.find('*'); p < text.size(); p = text.find('*', p)) {
      const auto is_replacement
        = std::string_view{text}.substr(p, 3) == replacement;
      if (is_replacement) {
        p += replacement.size();
        continue;
      }
      const auto replace_two
        = double_star and p < text.size() - 1 and text[p + 1] == '*';
      text.replace(p, 1 + replace_two, replacement);
      p += replacement.size();
    }
  }
  return text;
}

auto secret_censor::censor(const arrow::Status& status) const -> std::string {
  return censor(status.ToStringWithoutContextLines());
}

auto secret_censor::censor(diagnostic diag) const -> diagnostic {
  if (not is_noop()) {
    diag.message = censor(std::move(diag.message));
    for (auto& annotation : diag.annotations) {
      annotation.text = censor(std::move(annotation.text));
    }
    for (auto& note : diag.notes) {
      note.message = censor(std::move(note.message));
    }
  }
  return diag;
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

namespace {

auto apply_transformation(ecc::cleansing_blob blob,
                          fbs::data::SecretTransformations operation,
                          diagnostic_handler& dh, location loc)
  -> failure_or<ecc::cleansing_blob> {
#define X_ENCODE(OPERATION, FUNCTION)                                          \
  case OPERATION: {                                                            \
    const auto encoded = FUNCTION(std::string_view{                            \
      reinterpret_cast<const char*>(blob.data()),                              \
      reinterpret_cast<const char*>(blob.data() + blob.size()),                \
    });                                                                        \
    const auto enc_bytes = as_bytes(encoded);                                  \
    blob.assign(enc_bytes.begin(), enc_bytes.end());                           \
    return blob;                                                               \
  }

#define X_DECODE(OPERATION, FUNCTION)                                          \
  case OPERATION: {                                                            \
    const auto decoded = FUNCTION(std::string_view{                            \
      reinterpret_cast<const char*>(blob.data()),                              \
      reinterpret_cast<const char*>(blob.data() + blob.size()),                \
    });                                                                        \
    if (not decoded) {                                                         \
      diagnostic::error("failed to `" #OPERATION "` secret value")             \
        .primary(loc)                                                          \
        .emit(dh);                                                             \
      return failure::promise();                                               \
    }                                                                          \
    const auto dec_bytes = as_bytes(*decoded);                                 \
    blob.assign(dec_bytes.begin(), dec_bytes.end());                           \
    return blob;                                                               \
  }
  switch (operation) {
    using enum fbs::data::SecretTransformations;
    X_ENCODE(encode_base64, detail::base64::encode)
    X_DECODE(decode_base64, detail::base64::try_decode)
    X_ENCODE(encode_url, curl::escape)
    X_DECODE(decode_url, curl::try_unescape)
    X_ENCODE(encode_base58, detail::base58::encode)
    X_DECODE(decode_base58, detail::base58::decode)
    X_ENCODE(encode_hex, detail::hex::encode)
    X_DECODE(decode_hex, detail::hex::decode)
  }
#undef X_ENCODE
#undef X_DECODE
  TENZIR_UNREACHABLE();
}

// Helper struct to work around GCC 15 ICE in lambda_expr_this_capture
// when using explicit object parameters with captures.
struct secret_resolver {
  const request_map_t& requested;
  secret_censor& censor;
  diagnostic_handler& dh;
  location loc;

  using ret_t = failure_or<resolved_secret_value>;

  auto operator()(const fbs::data::SecretLiteral& l) -> ret_t {
    const auto& v = detail::secrets::deref(l.value());
    const auto v_bytes = as_bytes(v);
    return resolved_secret_value{
      ecc::cleansing_blob{v_bytes.begin(), v_bytes.end()}, true};
  }

  auto operator()(const fbs::data::SecretName& l) -> ret_t {
    const auto it = requested.find(detail::secrets::deref(l.value()).str());
    TENZIR_ASSERT(it != requested.end());
    censor.add(it->second.value);
    return resolved_secret_value{it->second.value, false};
  }

  auto operator()(const fbs::data::SecretConcatenation& concat) -> ret_t {
    auto res = ecc::cleansing_blob{};
    auto all_literal = true;
    for (auto* p : detail::secrets::deref(concat.secrets())) {
      TRY(auto part, match(detail::secrets::deref(p), *this));
      all_literal = all_literal and part.all_literal();
      res.insert(res.end(), part.blob().begin(), part.blob().end());
    }
    return resolved_secret_value{std::move(res), all_literal};
  }

  auto operator()(const fbs::data::SecretTransformed& trafo) -> ret_t {
    TRY(auto nested, match(detail::secrets::deref(trafo.secret()), *this));
    auto blob = ecc::cleansing_blob{nested.blob().begin(), nested.blob().end()};
    TRY(auto transformed,
        apply_transformation(std::move(blob), trafo.transformation(), dh, loc));
    censor.add(transformed);
    return resolved_secret_value{std::move(transformed), nested.all_literal()};
  }
};
} // namespace

auto secret_finisher::finish(const request_map_t& requested,
                             secret_censor& censor,
                             diagnostic_handler& dh) const -> failure_or<void> {
  auto f = secret_resolver{requested, censor, dh, this->loc};
  TRY(auto res, match(secret, f));
  return callback(std::move(res));
}

} // namespace tenzir
