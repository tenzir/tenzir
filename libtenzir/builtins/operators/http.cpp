//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/pipeline_executor.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <arrow/util/compression.h>
#include <caf/action.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/detail/pp.hpp>
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
#include <ranges>
#include <unordered_map>
#include <utility>

namespace tenzir::plugins::http {
namespace {

namespace http = caf::net::http;
namespace ssl = caf::net::ssl;
using namespace std::literals;

struct from_http_args {
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
    p.named_optional("tls", tls);
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
        if (not tls.inner and tls.source) {
          diagnostic::warning("`{}` is unused when `tls` is disabled", name)
            .primary(*opt)
            .emit(dh);
          return {};
        }
        tls.inner = true;
        if (opt->inner.empty()) {
          diagnostic::error("`{}` must not be empty", name)
            .primary(*opt)
            .emit(dh);
          return failure::promise();
        }
        return {};
      }
      if (tls.inner and required) {
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

  friend auto inspect(auto& f, from_http_args& x) -> bool {
    return f.object(x).fields(
      f.field("url", x.url), f.field("responses", x.responses),
      f.field("tls", x.tls), f.field("certfile", x.certfile),
      f.field("keyfile", x.keyfile), f.field("password", x.password),
      f.field("max_request_size", x.max_request_size), f.field("op", x.op),
      f.field("port", x.port));
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

class from_http_operator final : public crtp_operator<from_http_operator> {
public:
  from_http_operator() = default;

  from_http_operator(from_http_args args) : args_{std::move(args)} {
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
              if (auto body = try_decompress_payload(
                    r.header().field("content-encoding"), r.payload(), dh)) {
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
    return "tql2.from_http";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, from_http_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  from_http_args args_;
};

struct from_http final : public virtual operator_plugin2<from_http_operator> {
  auto name() const -> std::string override {
    return "tql2.from_http";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<from_http_operator>(std::move(args));
  }
};

// --------------------------------------- http ----------------------------------

struct http_args {
  location op;
  ast::expression url;
  std::optional<ast::expression> method;
  std::optional<ast::expression> payload;
  std::optional<ast::expression> headers;
  std::optional<ast::field_path> response_field;
  std::optional<ast::field_path> metadata_field;
  std::optional<ast::expression> paginate;
  located<duration> paginate_delay{0s, location::unknown};
  located<uint64_t> parallel{1, location::unknown};
  std::optional<location> tls;
  std::optional<located<std::string>> keyfile;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> password;
  located<duration> connection_timeout{5s, location::unknown};
  uint64_t max_retry_count{};
  located<duration> retry_delay{1s, location::unknown};
  located<pipeline> parse;
  std::optional<expression> filter;

  auto add_to(argument_parser2& p) {
    p.positional("url", url, "string");
    p.named("method", method, "string");
    p.named("payload", payload, "string");
    p.named("headers", headers, "record");
    p.named("response_field", response_field);
    p.named("metadata_field", metadata_field);
    p.named("paginate", paginate, "string");
    p.named_optional("paginate_delay", paginate_delay);
    p.named_optional("parallel", parallel);
    p.named("tls", tls);
    p.named("certfile", certfile);
    p.named("keyfile", keyfile);
    p.named("password", password);
    p.named_optional("connection_timeout", connection_timeout);
    p.named_optional("max_retry_count", max_retry_count);
    p.named_optional("retry_delay", retry_delay);
    p.positional("{ … }", parse);
  }

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    TENZIR_ASSERT(op);
    if (response_field and metadata_field) {
      auto rp = std::views::transform(response_field->path(),
                                      &ast::field_path::segment::id)
                | std::views::transform(&ast::identifier::name);
      auto mp = std::views::transform(metadata_field->path(),
                                      &ast::field_path::segment::id)
                | std::views::transform(&ast::identifier::name);
      auto [ri, mi] = std::ranges::mismatch(rp, mp);
      if (ri == end(rp) or mi == end(mp)) {
        diagnostic::error("`response_field` and `metadata_field` must not "
                          "point to same or nested field")
          .primary(*response_field)
          .primary(*metadata_field)
          .emit(dh);
        return failure::promise();
      }
    }
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (parallel.inner == 0) {
      diagnostic::error("`parallel` must be not be zero")
        .primary(parallel)
        .emit(dh);
      return failure::promise();
    }
    auto ty = parse.inner.infer_type(tag_v<chunk_ptr>);
    if (not ty) {
      diagnostic::error(ty.error()).primary(parse).emit(dh);
      return failure::promise();
    }
    if (ty.value().is_not<table_slice>()) {
      diagnostic::error("pipeline must return events").primary(parse).emit(dh);
      return failure::promise();
    }
    return {};
  }

  auto make_headers(const table_slice& slice, diagnostic_handler& dh,
                    bool& warned) const
    -> std::unordered_map<std::string, std::string> {
    if (not headers) {
      return {};
    }
    auto hs = std::unordered_map<std::string, std::string>{};
    auto ms = eval(*headers, slice, dh);
    for (const auto& s : ms.parts()) {
      if (s.type.kind().is_not<record_type>()) {
        diagnostic::warning("expected `record`, got `{}`", s.type.kind())
          .primary(*headers)
          .note("skipping headers")
          .emit(dh);
        continue;
      }
      for (const auto& val : s.values<record_type>()) {
        if (not val) {
          diagnostic::warning("expected `record`, got `null`")
            .primary(*headers)
            .note("skipping headers")
            .emit(dh);
          continue;
        }
        for (const auto& [k, v] : *val) {
          if (const auto* str = try_as<std::string_view>(v)) {
            hs.emplace(k, *str);
          } else if (not warned) {
            warned = true;
            diagnostic::warning("`headers` must be `{{ string: string }}`")
              .primary(*headers)
              .emit(dh);
          }
        }
      }
    }
    return hs;
  }

  auto make_method(const std::string_view method) const
    -> std::optional<http::method> {
    if (method.empty()) {
      if (not this->method and payload) {
        return http::method::post;
      }
      return http::method::get;
    }
    auto m = http::method{};
    if (http::from_string(method, m)) {
      return m;
    }
    return std::nullopt;
  }

  auto next_url(const table_slice& slice, diagnostic_handler& dh) const
    -> std::optional<std::string> {
    if (not paginate) {
      return std::nullopt;
    }
    if (slice.rows() != 1) {
      diagnostic::warning("cannot paginate over multiple events")
        .primary(parse)
        .emit(dh);
      // TODO: Decide on the semantics
      return std::nullopt;
    }
    const auto ms = eval(*paginate, slice, dh);
    const auto val = ms.value_at(0);
    return match(
      val,
      [](const caf::none_t&) -> std::optional<std::string> {
        TENZIR_DEBUG("[http] finishing pagination");
        return std::nullopt;
      },
      [](const std::string_view& url) -> std::optional<std::string> {
        TENZIR_DEBUG("[http] paginating: {}", url);
        return std::string{url};
      },
      [&](const auto&) -> std::optional<std::string> {
        diagnostic::error("expected `paginate` to be `string`, got `{}`",
                          ms.parts().front().type.kind())
          .primary(*paginate)
          .emit(dh);
        return std::nullopt;
      });
  }

  auto make_ssl_context() const -> caf::expected<ssl::context> {
    constexpr auto inner = [](const located<std::string>& x) {
      return x.inner;
    };
    return ssl::context::enable(tls.has_value())
      .and_then(ssl::emplace_context(ssl::tls::any))
      .and_then(ssl::enable_default_verify_paths())
      .and_then(ssl::use_private_key_file_if(keyfile.transform(inner),
                                             ssl::format::pem))
      .and_then(ssl::use_certificate_file_if(certfile.transform(inner),
                                             ssl::format::pem))
      .and_then(ssl::use_password_if(password.transform(inner)));
  }

  friend auto inspect(auto& f, http_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("url", x.url), f.field("method", x.method),
      f.field("payload", x.payload), f.field("headers", x.headers),
      f.field("tls", x.tls), f.field("certfile", x.certfile),
      f.field("keyfile", x.keyfile), f.field("password", x.password),
      f.field("metadata_field", x.metadata_field),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay), f.field("parallel", x.parallel),
      f.field("paginate", x.paginate),
      f.field("paginate_delay", x.paginate_delay), f.field("parse", x.parse),
      f.field("filter", x.filter));
  }
};

struct http_actor_traits final {
  using signatures = caf::type_list<
    /// Push events from subpipeline into self.
    auto(atom::internal, atom::push, table_slice output)->caf::result<void>,
    /// Get events from self to parent.
    auto(atom::pull)->caf::result<table_slice>>
    /// Support the diagnostic receiver interface.
    ::append_from<receiver_actor<diagnostic>::signatures>
    /// Support the metrics receiver interface for the branch pipelines.
    ::append_from<metrics_receiver_actor::signatures>;
};

using http_actor = caf::typed_actor<http_actor_traits>;

struct internal_source final : public crtp_operator<internal_source> {
  internal_source() = default;

  internal_source(chunk_ptr ptr) : ptr_{std::move(ptr)} {
  }

  auto operator()(operator_control_plane&) const -> generator<chunk_ptr> {
    co_yield {};
    co_yield ptr_;
  }

  auto name() const -> std::string override {
    return "internal-http-source";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, internal_source& x) {
    return f.apply(x.ptr_);
  }

private:
  chunk_ptr ptr_;
};

struct internal_sink final : public crtp_operator<internal_sink> {
  internal_sink() = default;

  internal_sink(http_actor actor, std::optional<expression> filter,
                tenzir::location op)
    : actor_{std::move(actor)}, op_{op}, filter_{std::move(filter)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto slice = table_slice{};
    for (auto slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::push_v, std::move(slice))
        .request(actor_, caf::infinite)
        .then(
          [&] {
            TENZIR_DEBUG("[internal-http-sink] pushed slice");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& e) {
            diagnostic::error("failed to push events: {}", e)
              .primary(op_)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::internal_v, atom::push_v, table_slice{})
      .request(actor_, caf::infinite)
      .then(
        [&] {
          TENZIR_DEBUG("[internal-http-sink] pushed final slice");
          ctrl.set_waiting(false);
        },
        [&](const caf::error& e) {
          diagnostic::error("failed to push events: {}", e)
            .primary(op_)
            .emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto name() const -> std::string override {
    return "internal-http-sink";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return {filter_, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, internal_sink& x) {
    return f.object(x).fields(f.field("actor_", x.actor_),
                              f.field("op_", x.op_),
                              f.field("filter_", x.filter_));
  }

private:
  http_actor actor_;
  tenzir::location op_;
  std::optional<expression> filter_;
};

struct http_state {
  http_state(http_actor::pointer self, shared_diagnostic_handler dh,
             metrics_receiver_actor metrics, uint64_t operator_index)
    : self_{self},
      dh_{std::move(dh)},
      metrics_receiver_{std::move(metrics)},
      operator_index_{operator_index} {
  }

  auto make_behavior() -> http_actor::behavior_type {
    return {
      [&](atom::internal, atom::push, table_slice output) {
        if (slice_rp_.pending()) {
          slice_rp_.deliver(std::move(output));
          return;
        }
        slices_.push_back(std::move(output));
      },
      [&](atom::pull) -> caf::result<table_slice> {
        TENZIR_ASSERT(not slice_rp_.pending());
        if (slices_.empty()) {
          if (exited) {
            self_->quit();
            return {};
          }
          slice_rp_ = self_->make_response_promise<table_slice>();
          return slice_rp_;
        }
        auto x = std::move(slices_.front());
        slices_.pop_front();
        return x;
      },
      [this](diagnostic diag) {
        dh_.emit(std::move(diag));
      },
      [this](const caf::exit_msg& msg) {
        exited = true;
        if (slices_.empty() or msg.reason) {
          self_->quit(msg.reason);
        }
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             type schema) {
        auto& id
          = registered_metrics_[nested_operator_index][nested_metrics_id];
        id = uuid::random();
        return self_->mail(operator_index_, id, std::move(schema))
          .delegate(metrics_receiver_);
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             record metrics) {
        const auto& id
          = registered_metrics_[nested_operator_index][nested_metrics_id];
        return self_->mail(operator_index_, id, std::move(metrics))
          .delegate(metrics_receiver_);
      },
      [](const operator_metric& metrics) {
        // We deliberately ignore operator metrics. There's no good way to
        // forward them from nested pipelines, and nowadays operator metrics
        // are really only relevant for generating pipeline metrics. If there's
        // a sink in the then-branch we're unfortunately losing its egress
        // metrics at the moment.
        TENZIR_UNUSED(metrics);
      },
    };
  }

private:
  http_actor::pointer self_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor metrics_receiver_;
  uint64_t operator_index_ = 0;
  detail::flat_map<uint64_t, detail::flat_map<uuid, uuid>> registered_metrics_;
  caf::typed_response_promise<table_slice> slice_rp_;
  std::deque<table_slice> slices_;
  bool exited = false;
};

auto make_metadata(const http::response& r, diagnostic_handler& dh,
                   uint64_t len = 1, bool with_payload = true)
  -> series_builder {
  auto sb = series_builder{};
  for (auto i = uint64_t{}; i < len; ++i) {
    auto rb = sb.record();
    rb.field("code", static_cast<uint64_t>(r.code()));
    if (not r.header_fields().empty()) {
      auto hb = rb.field("headers").record();
      for (const auto& [k, v] : r.header_fields()) {
        hb.field(k, v);
      }
    }
    if (with_payload and not r.body().empty()) {
      const auto& headers = r.header_fields();
      const auto* it = std::ranges::find_if(headers, [](const auto& x) {
        return caf::icase_equal(x.first, "content-encoding");
      });
      const auto encoding = it != std::ranges::end(headers) ? it->first : "";
      if (auto body = try_decompress_payload(encoding, r.body(), dh)) {
        rb.field("payload", std::move(*body));
      } else {
        rb.field("payload", r.body());
      }
    }
  }
  return sb;
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
    // NOTE: lambda because context has a deleted copy constructor
    auto awaiting = uint64_t{};
    auto slices = std::vector<table_slice>{};
    auto paginate_queue = std::vector<http::client_factory>{};
    auto warned = false;
    const auto handle_response = [&](view<record> og) {
      return [&, og = materialize(std::move(og))](const http::response& r) {
        TENZIR_DEBUG("[http] handling response");
        const auto actor = spawn_pipeline(ctrl, chunk::copy(r.body()));
        std::invoke([&, &args_ = args_, r, og,
                     actor](this const auto& pull) -> void {
          TENZIR_DEBUG("[http] requesting slice");
          ctrl.self()
            .mail(atom::pull_v)
            .request(actor, caf::infinite)
            .then(
              [&, r, pull, og, actor](table_slice slice) {
                TENZIR_DEBUG("[http] pulled slice");
                if (slice.rows() == 0) {
                  TENZIR_DEBUG("[http] finishing subpipeline");
                  --awaiting;
                  if (awaiting < args_.parallel.inner) {
                    ctrl.set_waiting(false);
                  }
                  return;
                }
                pull();
                if (args_.response_field) {
                  auto sb = series_builder{};
                  for (auto i = size_t{}; i < slice.rows(); ++i) {
                    sb.data(og);
                  }
                  slice = assign(*args_.response_field, series{slice},
                                 sb.finish_assert_one_slice(), dh);
                }
                if (args_.metadata_field) {
                  auto sb
                    = make_metadata(r, ctrl.diagnostics(), slice.rows(), false);
                  slice = assign(*args_.metadata_field,
                                 sb.finish_assert_one_array(), slice,
                                 ctrl.diagnostics());
                }
                if (auto url = args_.next_url(slice, dh)) {
                  if (not url->starts_with("http://")
                      and not url->starts_with("https://")) {
                    url->insert(0, args_.tls ? "https://" : "http://");
                  }
                  if (args_.tls and url->starts_with("http://")) {
                    url->insert(4, "s");
                  }
                  paginate_queue.push_back(std::move(
                    http::with(ctrl.self().system())
                      .context(args_.make_ssl_context())
                      .connect(caf::make_uri(*url))
                      .connection_timeout(args_.connection_timeout.inner)
                      .max_retry_count(args_.max_retry_count)
                      .retry_delay(args_.retry_delay.inner)
                      .add_header_fields(
                        args_.make_headers(slice, dh, warned))));
                } else {
                  TENZIR_DEBUG("[http] done paginating");
                }
                slices.push_back(std::move(slice));
              },
              [&](const caf::error& e) {
                diagnostic::error("failed to get slice: {}", e)
                  .primary(args_.op)
                  .emit(ctrl.diagnostics());
              });
        });
        TENZIR_DEBUG("[http] handled response");
      };
    };
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto urls = eval_string(args_.url, slice, dh);
      auto methods = eval_optional_string(args_.method, slice, dh);
      auto payloads = eval_optional_string(args_.payload, slice, dh);
      for (auto row : slice.values()) {
        auto url = std::string{urls.next().value()};
        const auto method = methods.next().value();
        const auto payload = payloads.next().value();
        if (url.empty()) {
          diagnostic::warning("`url` must not be empty")
            .primary(args_.url)
            .note("skipping request")
            .emit(dh);
          continue;
        }
        if (not url.starts_with("http://")
            and not url.starts_with("https://")) {
          url.insert(0, args_.tls ? "https://" : "http://");
        }
        if (args_.tls and url.starts_with("http://")) {
          url.insert(4, "s");
        }
        const auto m = args_.make_method(method);
        if (not m) {
          diagnostic::warning("invalid http method: `{}`", method)
            .primary(args_.method.value())
            .emit(dh);
          continue;
        }
        http::with(ctrl.self().system())
          .context(args_.make_ssl_context())
          .connect(caf::make_uri(url))
          .connection_timeout(args_.connection_timeout.inner)
          .max_retry_count(args_.max_retry_count)
          .retry_delay(args_.retry_delay.inner)
          .add_header_fields(args_.make_headers(slice, dh, warned))
          .request(*m, payload)
          .or_else([&](const caf::error& e) {
            diagnostic::error("failed to make http request: {}", e)
              .primary(args_.op)
              .emit(dh);
          })
          .transform([](auto&& x) {
            return std::move(x.first);
          })
          .transform([&](const caf::async::future<http::response>& fut) {
            ++awaiting;
            fut.bind_to(ctrl.self())
              .then(handle_response(row), [&](const caf::error& e) {
                --awaiting;
                if (awaiting < args_.parallel.inner) {
                  ctrl.set_waiting(false);
                }
                diagnostic::warning("request failed: `{}`", e)
                  .primary(args_.op)
                  .emit(dh);
              });
          });
        if (awaiting >= args_.parallel.inner) {
          ctrl.set_waiting(true);
          co_yield {};
        }
        // NOTE: Must be an index-based loop. The thread can go back to the
        // observe loop after yielding here, causing the vector's iterator to
        // be invalidated.
        for (auto i = size_t{}; i < slices.size(); ++i) {
          co_yield slices[i];
        }
        slices.clear();
        // NOTE: Must be an index-based loop. The thread can go back to the
        // observe loop after yielding here, causing the vector's iterator to
        // be invalidated.
        for (auto i = size_t{}; i < paginate_queue.size(); ++i) {
          ++awaiting;
          ctrl.self().run_delayed(
            args_.paginate_delay.inner,
            [&, req = std::move(paginate_queue[i])] mutable {
              std::move(req)
                .get()
                .or_else([&](const caf::error& e) {
                  diagnostic::error("failed to make http request: {}", e)
                    .primary(args_.op)
                    .emit(dh);
                })
                .transform([](auto&& x) {
                  return x.first;
                })
                .transform([&](const caf::async::future<http::response>& fut) {
                  fut.bind_to(ctrl.self())
                    .then(handle_response(row), [&](const caf::error& e) {
                      --awaiting;
                      if (awaiting < args_.parallel.inner) {
                        ctrl.set_waiting(false);
                      }
                      diagnostic::warning("request failed: `{}`", e)
                        .primary(args_.op)
                        .emit(dh);
                    });
                });
            });
          if (awaiting >= args_.parallel.inner) {
            ctrl.set_waiting(true);
            co_yield {};
          }
          // NOTE: Must be an index-based loop. The thread can go back to the
          // observe loop after yielding here, causing the vector's iterator to
          // be invalidated.
          for (auto i = size_t{}; i < slices.size(); ++i) {
            co_yield slices[i];
          }
          slices.clear();
        }
        paginate_queue.clear();
      }
    }
    do {
      ctrl.set_waiting(awaiting != 0);
      co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to the
      // observe loop after yielding here, causing the vector's iterator to
      // be invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
    } while (awaiting != 0);
    TENZIR_ASSERT(paginate_queue.empty());
  }

  auto spawn_pipeline(operator_control_plane& ctrl, chunk_ptr ptr) const
    -> http_actor {
    TENZIR_DEBUG("[http] spawning http_actor");
    auto ha = ctrl.self().spawn<caf::linked>(caf::actor_from_state<http_state>,
                                             ctrl.shared_diagnostics(),
                                             ctrl.metrics_receiver(),
                                             ctrl.operator_index());
    ctrl.self().monitor(ha, [&](const caf::error& e) {
      if (e) {
        diagnostic::error(e).primary(args_.op).emit(ctrl.diagnostics());
      }
    });
    auto parse = args_.parse.inner;
    parse.prepend(std::make_unique<internal_source>(std::move(ptr)));
    parse.append(std::make_unique<internal_sink>(ha, args_.filter, args_.op));
    TENZIR_DEBUG("[http] spawning subpipeline");
    const auto handle = ctrl.self().spawn<caf::linked>(
      pipeline_executor, std::move(parse).optimize_if_closed(),
      std::string{ctrl.definition()}, ha, ha, ctrl.node(), ctrl.has_terminal(),
      ctrl.is_hidden());
    handle->link_to(ha);
    ctrl.self().attach_functor([handle] {});
    TENZIR_DEBUG("[http] requesting subpipeline start");
    ctrl.self()
      .mail(atom::start_v)
      .request(handle, caf::infinite)
      .then(
        [] {
          TENZIR_DEBUG("[http] subpipeline started");
        },
        [&](const caf::error& e) {
          TENZIR_DEBUG("[http] failed to start subpipeline: {}", e);
          diagnostic::error(e).primary(args_.parse).emit(ctrl.diagnostics());
        });
    return ha;
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

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& expr, event_order) const
    -> optimize_result override {
    const auto make_copy = [&] -> operator_ptr {
      if (not args_.paginate) {
        auto args = args_;
        args.filter = expr;
        return std::make_unique<http_operator>(std::move(args));
      }
      return copy();
    };
    return {
      std::nullopt,
      args_.parallel.inner == 1 ? event_order::ordered : event_order::unordered,
      make_copy(),
    };
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

using internal_source_plugin = operator_inspection_plugin<internal_source>;
using internal_sink_plugin = operator_inspection_plugin<internal_sink>;

} // namespace
} // namespace tenzir::plugins::http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::http_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::internal_source_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::internal_sink_plugin)
