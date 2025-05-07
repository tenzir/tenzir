//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <arrow/util/compression.h>
#include <caf/action.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/flow/observable_builder.hpp>
#include <caf/net/http/method.hpp>
#include <caf/net/http/server.hpp>
#include <caf/net/http/with.hpp>
#include <caf/net/ssl/context.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/timespan.hpp>

#include <unordered_map>

namespace tenzir::plugins::http {
namespace {
using namespace std::literals;

namespace http = caf::net::http;
namespace ssl = caf::net::ssl;

struct http_args {
  location op;
  ast::expression url;
  std::optional<ast::expression> method;
  std::optional<ast::expression> payload;
  std::optional<ast::expression> headers;
  std::optional<location> tls;
  std::optional<located<std::string>> keyfile;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> password;
  located<duration> connection_timeout{5s, location::unknown};
  uint64_t max_retry_count{0};
  located<duration> retry_delay{1s, location::unknown};

  auto add_to(argument_parser2& p) {
    p.positional("url", url, "string");
    p.named("method", method, "string");
    p.named("payload", payload, "string");
    p.named("headers", headers, "record");
    p.named("tls", tls);
    p.named("certfile", certfile);
    p.named("keyfile", keyfile);
    p.named("password", password);
    p.named_optional("connection_timeout", connection_timeout);
    p.named_optional("max_retry_count", max_retry_count);
    p.named_optional("retry_delay", retry_delay);
  }

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    TENZIR_ASSERT(op);
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, http_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("url", x.url), f.field("method", x.method),
      f.field("payload", x.payload), f.field("tls", x.tls),
      f.field("certfile", x.certfile), f.field("keyfile", x.keyfile),
      f.field("password", x.password),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay));
  }
};

auto try_decompress_payload(const std::string_view encoding,
                            const std::span<const std::byte> payload,
                            diagnostic_handler& dh) -> std::optional<blob> {
  if (encoding.empty()) {
    return std::nullopt;
  }
  const auto compression_type
    = arrow::util::Codec::GetCompressionType(std::string{encoding});
  // Arrow straight up crashes if we use a codec created from the
  // string "uncompressed", so we just don't do that.
  // Last checked with Arrow 12.0.
  // TODO: Recheck if this is still the case.
  if (not compression_type.ok()
      or *compression_type == arrow::Compression::UNCOMPRESSED) {
    diagnostic::warning("invalid compression type: {}", encoding)
      .hint("must be one of `brotli`, `bz2`, `gzip`, `lz4`, `zstd`")
      .note("skipping decompression")
      .emit(dh);
    return std::nullopt;
  }
  auto out = blob{};
  out.resize(payload.size_bytes() * 2);
  const auto codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  TENZIR_ASSERT(codec.ok());
  const auto decompressor = check(codec.ValueUnsafe()->MakeDecompressor());
  auto written = size_t{};
  auto read = size_t{};
  while (read != payload.size_bytes()) {
    const auto result = decompressor->Decompress(
      detail::narrow<int64_t>(payload.size_bytes() - read),
      reinterpret_cast<const uint8_t*>(payload.data() + read),
      detail::narrow<int64_t>(out.capacity() - written),
      reinterpret_cast<uint8_t*>(out.data() + written));
    if (not result.ok()) {
      diagnostic::warning("failed to decompress: {}",
                          result.status().ToString())
        .note("emitting compressed payload")
        .emit(dh);
      return std::nullopt;
    }
    TENZIR_ASSERT(std::cmp_less_equal(result->bytes_written, out.size()));
    written += result->bytes_written;
    read += result->bytes_read;
    if (result->need_more_output) {
      if (out.size() == out.max_size()) [[unlikely]] {
        diagnostic::error("failed to resize buffer").emit(dh);
        return std::nullopt;
      }
      if (out.size() < out.max_size() / 2) {
        out.resize(out.size() * 2);
      } else [[unlikely]] {
        out.resize(out.max_size());
      }
    }
    // In case the input contains multiple concatenated compressed streams,
    // we gracefully reset the decompressor.
    if (decompressor->IsFinished()) {
      const auto result = decompressor->Reset();
      if (not result.ok()) {
        diagnostic::warning("failed to reset decompressor: {}",
                            result.ToString())
          .note("emitting compressed payload")
          .emit(dh);
        return std::nullopt;
      }
    }
  }
  TENZIR_ASSERT(written != 0);
  out.resize(written);
  return out;
}

class http_operator final : public crtp_operator<http_operator> {
public:
  http_operator() = default;

  http_operator(http_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto& dh = ctrl.diagnostics();
    // TODO: consider if this should also be extractable
    // lambda because context has a deleted copy constructor
    const auto context = [&] {
      return ssl::context::enable(args_.tls.has_value())
        .and_then(ssl::emplace_server(ssl::tls::v1_2))
        .and_then(ssl::enable_default_verify_paths())
        .and_then(ssl::use_private_key_file_if(
          args_.keyfile ? args_.keyfile->inner : "", ssl::format::pem))
        .and_then(ssl::use_certificate_file_if(
          args_.certfile ? args_.certfile->inner : "", ssl::format::pem))
        .and_then(
          ssl::use_password_if(args_.password ? args_.password->inner : ""));
    };
    auto awaiting = uint64_t{};
    auto slices = std::vector<table_slice>{};
    auto warned = false;
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto urls = eval_string(args_.url, slice, dh);
      auto methods = eval_optional_string(args_.method, slice, dh);
      auto payloads = eval_optional_string(args_.payload, slice, dh);
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        const auto url = urls.next().value();
        const auto method = methods.next().value();
        const auto payload = payloads.next().value();
        if (url.empty()) {
          diagnostic::warning("`url` must not be empty")
            .primary(args_.url)
            .note("skipping request")
            .emit(dh);
          continue;
        }
        auto m = http::method{};
        // should we keep this?
        const auto make_method = [&] -> std::string_view {
          if (not method.empty()) {
            return method;
          }
          if (not args_.method and args_.payload) {
            return "post";
          }
          return "get";
        };
        if (not http::from_string(make_method(), m)) {
          diagnostic::warning("unknown http method: `{}`", method)
            .primary(args_.method.value())
            .note("skipping request")
            .emit(dh);
          continue;
        }
        http::with(ctrl.self().system())
          .context(context())
          .connect(caf::make_uri(url))
          .connection_timeout(args_.connection_timeout.inner)
          .max_retry_count(args_.max_retry_count)
          .retry_delay(args_.retry_delay.inner)
          .add_header_fields(make_headers(slice, dh, warned))
          .request(m, payload)
          .transform([&](auto&& r) {
            ++awaiting;
            r.first.bind_to(ctrl.self())
              .then(
                [&](const http::response& r) {
                  --awaiting;
                  auto sb = series_builder{};
                  auto rb = sb.record();
                  if (not r.header_fields().empty()) {
                    auto hb = rb.field("headers").record();
                    for (auto&& [k, v] : r.header_fields()) {
                      hb.field(k, v);
                    }
                  }
                  if (not r.body().empty()) {
                    const auto& headers = r.header_fields();
                    const auto* it
                      = std::ranges::find_if(headers, [](auto&& x) {
                          return caf::icase_equal(x.first, "content-encoding");
                        });
                    const auto encoding
                      = it != std::ranges::end(headers) ? it->first : "";
                    if (auto body
                        = try_decompress_payload(encoding, r.body(), dh)) {
                      rb.field("payload", std::move(*body));
                    } else {
                      rb.field("payload", r.body());
                    }
                  }
                  ctrl.set_waiting(false);
                  slices.push_back(sb.finish_assert_one_slice());
                },
                [&](const caf::error& e) {
                  --awaiting;
                  diagnostic::warning("request failed: `{}`", e)
                    .primary(args_.op)
                    .emit(dh);
                });
          })
          .or_else([&](const caf::error& e) {
            diagnostic::warning("failed to make http request: {}", e)
              .primary(args_.op)
              .emit(ctrl.diagnostics());
          });
      }
      // ctrl.set_waiting(true);
      // co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to the
      // observe loop after yielding here, causing the vector's iterator to
      // be invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
    }
    while (awaiting != 0) {
      ctrl.set_waiting(true);
      co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to the
      // observe loop after yielding here, causing the vector's iterator to
      // be invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
    }
  }

  static auto eval_string(const ast::expression& expr, const table_slice& slice,
                          diagnostic_handler& dh)
    -> generator<std::string_view> {
    const auto ms = eval(expr, slice, dh);
    for (const auto& s : ms.parts()) {
      if (s.type.kind().is<null_type>()) {
        for (auto i = int64_t{}; i < s.length(); ++i) {
          co_yield {};
        }
        continue;
      }
      if (s.type.kind().is<string_type>()) {
        for (auto val : s.values<string_type>()) {
          co_yield val.value_or("");
        }
        continue;
      }
      diagnostic::warning("expected `string`, got `{}`", s.type.kind())
        .primary(expr)
        .emit(dh);
      for (auto i = int64_t{}; i < s.length(); ++i) {
        co_yield {};
      }
    }
  }

  static auto
  eval_optional_string(const std::optional<ast::expression>& expr,
                       const table_slice& slice, diagnostic_handler& dh)
    -> generator<std::string_view> {
    if (not expr) {
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        co_yield {};
      }
      co_return;
    }
    for (auto val : eval_string(expr.value(), slice, dh)) {
      co_yield val;
    }
  }

  auto make_headers(const table_slice& slice, diagnostic_handler& dh,
                    bool& warned) const
    -> std::unordered_map<std::string, std::string> {
    if (not args_.headers) {
      return {};
    }
    auto headers = std::unordered_map<std::string, std::string>{};
    auto ms = eval(args_.headers.value(), slice, dh);
    for (const auto& s : ms.parts()) {
      if (s.type.kind().is<null_type>()) {
        continue;
      }
      if (s.type.kind().is_not<record_type>()) {
        diagnostic::warning("expected `record`, got `{}`", s.type.kind())
          .primary(args_.headers.value())
          .note("skipping headers")
          .emit(dh);
        continue;
      }
      for (const auto& val : s.values<record_type>()) {
        if (not val) {
          continue;
        }
        for (const auto& [k, v] : *val) {
          if (const auto* str = try_as<std::string_view>(v)) {
            headers.emplace(k, *str);
          } else if (not warned) {
            warned = true;
            diagnostic::warning("`headers` must be `{{ string: string }}`")
              .primary(args_.headers.value())
              .emit(dh);
          }
        }
      }
    }
    return headers;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    // should this be unordered?
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "tql2.http";
  }

  friend auto inspect(auto& f, http_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  http_args args_;
};

struct http_plugin final : public operator_plugin2<http_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<http_operator>(std::move(args));
  }
};

} // namespace
} // namespace tenzir::plugins::http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::http_plugin)
