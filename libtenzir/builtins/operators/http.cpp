//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"

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

#include <charconv>

namespace tenzir::plugins::http {
namespace {

namespace http = caf::net::http;
namespace ssl = caf::net::ssl;

struct http_args {
  located<std::string> url;
  std::optional<located<bool>> server;
  std::optional<located<record>> responses;
  located<bool> tls{false, location::unknown};
  std::optional<located<std::string>> keyfile;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> password;
  located<uint64_t> max_request_size{10 * 1024 * 1024, location::unknown};
  location op;
  uint16_t port{};

  auto add_to(argument_parser2& p) {
    p.positional("url", url);
    p.named("server", server);
    p.named("responses", responses);
    p.named("tls", tls);
    p.named("certfile", certfile);
    p.named("keyfile", keyfile);
    p.named("password", password);
    p.named_optional("max_request_size", max_request_size);
  }

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    TENZIR_ASSERT(op);
    if (url.inner.empty()) {
      diagnostic::error("`url` must not be empty").primary(url).emit(dh);
      return failure::promise();
    }
    const auto col = url.inner.rfind(':');
    if (col == 0 or col == std::string::npos) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(url)
        .emit(dh);
      return failure::promise();
    }
    const auto* end = url.inner.data() + url.inner.size();
    const auto [ptr, err]
      = std::from_chars(url.inner.data() + col + 1, end, port);
    if (err != std::errc{}) {
      diagnostic::error("failed to parse port").primary(url).emit(dh);
      return failure::promise();
    }
    if (ptr != end) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(url)
        .emit(dh);
      return failure::promise();
    }
    url.inner.resize(col);
    if (not server) {
      diagnostic::error("HTTP client is not yet implement")
        .note("pass `server=true` to start an HTTP server")
        .emit(dh);
      return failure::promise();
    }
    if (not server->inner) {
      diagnostic::error("HTTP client is not yet implement")
        .primary(*server, "set to `true` to start an HTTP server")
        .emit(dh);
      return failure::promise();
    }
    if (max_request_size.inner == 0) {
      diagnostic::error("request size must not be zero")
        .primary(max_request_size)
        .emit(dh);
      return failure::promise();
    }
    if (responses) {
      if (responses->inner.empty()) {
        diagnostic::error("`responses` must not be empty")
          .primary(*responses)
          .emit(dh);
        return failure::promise();
      }
      for (const auto& [k, v] : responses->inner) {
        const auto* rec = try_as<record>(v);
        if (not rec) {
          diagnostic::error("field must be `record`")
            .primary(*responses)
            .emit(dh);
          return failure::promise();
        }
        TRY(has_typed_key<uint64_t>(*rec, "code", responses->source, dh));
        TRY(has_typed_key<std::string>(*rec, "content_type", responses->source,
                                       dh));
        TRY(has_typed_key<std::string>(*rec, "body", responses->source, dh));
        const auto code = as<uint64_t>(rec->at("code"));
        const auto ucode
          = detail::narrow<std::underlying_type_t<http::status>>(code);
        auto status = http::status{};
        if (not http::from_integer(ucode, status)) {
          diagnostic::error("got invalid http status code `{}`", code)
            .primary(responses->source)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    const auto tls_logic
      = [&](const std::optional<located<std::string>>& opt,
            std::string_view name, bool required = false) -> failure_or<void> {
      if (opt) {
        if (opt->inner.empty()) {
          diagnostic::error("`{}` must not be empty", name)
            .primary(*opt)
            .emit(dh);
          return failure::promise();
        }
        if (not tls.inner and tls.source) {
          diagnostic::warning("`{}` is unused when `tls` is disabled", name)
            .primary(*opt)
            .emit(dh);
          return {};
        }
        tls.inner = true;
        return {};
      }
      if (required) {
        diagnostic::error("`{}` must be set when enabling `tls`", name)
          .primary(tls.source ? tls.source : op)
          .emit(dh);
        return failure::promise();
      }
      return {};
    };
    TRY(tls_logic(certfile, "certfile", true));
    TRY(tls_logic(keyfile, "keyfile", true));
    TRY(tls_logic(password, "password"));
    return {};
  }

  template <typename T>
  auto has_typed_key(const record& r, std::string_view name, location loc,
                     diagnostic_handler& dh) -> failure_or<void> {
    const auto key = r.find(name);
    if (key == r.end()) {
      diagnostic::error("`responses` must contain key `{}`", name)
        .primary(loc)
        .emit(dh);
      return failure::promise();
    }
    if (not is<T>(key->second)) {
      diagnostic::error("`{}` must be of type `{}`", name,
                        type::infer(T{})->kind())
        .primary(loc)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, http_args& x) -> bool {
    return f.object(x).fields(
      f.field("url", x.url), f.field("responses", x.responses),
      f.field("tls", x.tls), f.field("certfile", x.certfile),
      f.field("keyfile", x.keyfile), f.field("password", x.password),
      f.field("max_request_size", x.max_request_size), f.field("op", x.op),
      f.field("port", x.port));
  }
};

auto try_decompress_payload(const http::request& r, diagnostic_handler& dh)
  -> std::optional<blob> {
  if (not r.header().has_field("content-encoding")) {
    return std::nullopt;
  }
  const auto encoding = r.header().field("content-encoding");
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
  out.resize(r.payload().size_bytes() * 2);
  const auto codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  TENZIR_ASSERT(codec.ok());
  const auto decompressor
    = codec.ValueUnsafe()->MakeDecompressor().ValueOrDie();
  auto written = size_t{};
  auto read = size_t{};
  while (read != r.payload().size_bytes()) {
    const auto result = decompressor->Decompress(
      detail::narrow<int64_t>(r.payload().size_bytes() - read),
      reinterpret_cast<const uint8_t*>(r.payload().data() + read),
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
  return std::move(out);
}

class from_http_operator final : public crtp_operator<from_http_operator> {
public:
  from_http_operator() = default;

  from_http_operator(http_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto pull = std::optional<caf::async::consumer_resource<http::request>>{};
    auto context
      = ssl::context::enable(args_.tls.inner)
          .and_then(ssl::emplace_server(ssl::tls::v1_2))
          .and_then(ssl::enable_default_verify_paths())
          .and_then(ssl::use_private_key_file_if(
            args_.keyfile ? args_.keyfile->inner : "", ssl::format::pem))
          .and_then(ssl::use_certificate_file_if(
            args_.certfile ? args_.certfile->inner : "", ssl::format::pem))
          .and_then(
            ssl::use_password_if(args_.password ? args_.password->inner : ""));
    auto server
      = http::with(ctrl.self().system())
          .context(std::move(context))
          .accept(args_.port, args_.url.inner)
          .monitor(static_cast<exec_node_actor>(&ctrl.self()))
          .max_request_size(args_.max_request_size.inner)
          .start([&](caf::async::consumer_resource<http::request> cr) {
            TENZIR_ASSERT(not pull);
            pull = std::move(cr);
          });
    if (not server) {
      diagnostic::error("failed to setup http server: {}", server.error())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
      co_return;
    }
    TENZIR_ASSERT(pull);
    auto slices = std::vector<table_slice>{};
    auto [ptr, launch] = ctrl.self().system().spawn_inactive();
    ptr->link_to(static_cast<exec_node_actor>(&ctrl.self()));
    auto stream
      = pull->observe_on(ptr)
          .map([responses = args_.responses, dh = ctrl.shared_diagnostics()](
                 const http::request& r) mutable {
            if (responses) {
              const auto it = responses->inner.find(r.header().path());
              if (it != responses->inner.end()) {
                auto rec = as<record>(it->second);
                auto code = as<uint64_t>(rec["code"]);
                auto ty = as<std::string>(rec["content_type"]);
                auto body = as<std::string>(rec["body"]);
                r.respond(static_cast<http::status>(code), ty, body);
              }
            } else {
              r.respond(http::status::ok, "", "");
            }
            auto sb = series_builder{};
            auto rb = sb.record();
            if (r.header().num_fields() != 0) {
              auto hb = rb.field("headers").record();
              r.header().for_each_field(
                [&](std::string_view k, std::string_view v) {
                  hb.field(k, v);
                });
            }
            if (not r.header().query().empty()) {
              auto qb = rb.field("query").record();
              for (const auto& [k, v] : r.header().query()) {
                qb.field(k, v);
              }
            }
            const auto add_field
              = [&](std::string_view name, std::string_view val) {
                  if (not val.empty()) {
                    rb.field(name, val);
                  }
                };
            add_field("path", r.header().path());
            add_field("fragment", r.header().fragment());
            add_field("method", to_string(r.header().method()));
            add_field("version", r.header().version());
            if (not r.body().empty()) {
              if (auto body = try_decompress_payload(r, dh)) {
                rb.field("body", std::move(*body));
              } else {
                rb.field("body", r.body());
              }
            }
            return sb.finish_assert_one_slice();
          })
          .to_typed_stream<table_slice>("from_http", std::chrono::seconds{1},
                                        1);
    ctrl.self().observe(stream, 30, 10).for_each([&](const table_slice& slice) {
      ctrl.set_waiting(false);
      slices.push_back(slice);
    });
    launch();
    while (true) {
      ctrl.set_waiting(true);
      co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to the
      // observe loop after yielding here, causing the vector's iterator to be
      // invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "tql2.http";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, from_http_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  http_args args_;
};

struct from_http final : public virtual operator_plugin2<from_http_operator> {
  auto name() const -> std::string override {
    return "tql2.from_http";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<from_http_operator>(std::move(args));
  }
};

} // namespace
} // namespace tenzir::plugins::http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http)
