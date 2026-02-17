//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/async.hpp"
#include "tenzir/async/tls.hpp"
#include "tenzir/async/unbounded_queue.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <arrow/util/compression.h>
#include <boost/url/parse.hpp>
#include <boost/url/parse_query.hpp>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/ServerSocket.h>
#define nsel_CONFIG_SELECT_EXPECTED 1
#include <proxygen/lib/http/codec/HTTPCodecFactory.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>

#include <charconv>
#include <ranges>
#include <utility>

namespace tenzir::plugins::http {
namespace {

namespace phttp = proxygen;
using namespace tenzir::si_literals;

constexpr auto default_max_request_size = 10_Mi;
constexpr auto default_listen_backlog = uint32_t{128};

template <typename T>
constexpr auto inner(std::optional<located<T>> const& x) -> std::optional<T> {
  return x.transform([](auto&& x) {
    return x.inner;
  });
};

auto try_decompress_body(std::string_view const encoding,
                         std::span<std::byte const> const body,
                         diagnostic_handler& dh) -> std::optional<blob> {
  if (encoding.empty()) {
    return std::nullopt;
  }
  auto const compression_type
    = arrow::util::Codec::GetCompressionType(std::string{encoding});
  if (not compression_type.ok()) {
    diagnostic::warning("invalid compression type: {}", encoding)
      .hint("must be one of `brotli`, `bz2`, `gzip`, `lz4`, `zstd`")
      .note("skipping decompression")
      .emit(dh);
    return std::nullopt;
  }
  auto out = blob{};
  out.resize(body.size_bytes() * 2);
  auto const codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  TENZIR_ASSERT(codec.ok());
  if (not codec.ValueUnsafe()) {
    return std::nullopt;
  }
  auto const decompressor = check(codec.ValueUnsafe()->MakeDecompressor());
  auto written = size_t{};
  auto read = size_t{};
  while (read != body.size_bytes()) {
    auto const result = decompressor->Decompress(
      detail::narrow<int64_t>(body.size_bytes() - read),
      reinterpret_cast<uint8_t const*>(body.data() + read),
      detail::narrow<int64_t>(out.size() - written),
      reinterpret_cast<uint8_t*>(out.data() + written));
    if (not result.ok()) {
      diagnostic::warning("failed to decompress: {}",
                          result.status().ToString())
        .note("emitting compressed body")
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
      auto const result = decompressor->Reset();
      if (not result.ok()) {
        diagnostic::warning("failed to reset decompressor: {}",
                            result.ToString())
          .note("emitting compressed body")
          .emit(dh);
        return std::nullopt;
      }
    }
  }
  TENZIR_ASSERT(written != 0);
  out.resize(written);
  return out;
}

struct ProxygenRequestData {
  std::string method;
  std::string path;
  std::string fragment;
  std::string version;
  std::vector<std::pair<std::string, std::string>> headers;
  std::vector<std::pair<std::string, std::string>> query;
  blob body;
};

auto decode_query_string(std::string_view query)
  -> std::vector<std::pair<std::string, std::string>> {
  auto result = std::vector<std::pair<std::string, std::string>>{};
  if (query.empty()) {
    return result;
  }
  auto parsed = boost::urls::parse_query(query);
  if (not parsed) {
    return result;
  }
  for (auto const& param : *parsed) {
    result.emplace_back(std::string{param.key}, std::string{param.value});
  }
  return result;
}

auto find_header_value(
  std::vector<std::pair<std::string, std::string>> const& headers,
  std::string_view name) -> std::string_view {
  auto const it = std::ranges::find_if(headers, [&](auto const& kv) {
    return detail::ascii_icase_equal(kv.first, name);
  });
  if (it == headers.end()) {
    return {};
  }
  return it->second;
}

auto make_request_record(ProxygenRequestData const& request) -> record {
  auto headers = record{};
  for (auto const& [k, v] : request.headers) {
    headers.emplace(k, v);
  }
  auto query = record{};
  for (auto const& [k, v] : request.query) {
    query.emplace(k, v);
  }
  return record{
    {"headers", std::move(headers)},
    {"query", std::move(query)},
    {"path", request.path},
    {"fragment", request.fragment},
    {"method", request.method},
    {"version", request.version},
    {"body", request.body},
  };
}

auto make_request_event(ProxygenRequestData const& request) -> table_slice {
  auto sb = series_builder{};
  sb.data(make_request_record(request));
  return sb.finish_assert_one_slice();
}

auto try_parse_response_code(data const& value) -> std::optional<uint16_t> {
  if (auto const* x = try_as<uint64_t>(value)) {
    if (*x <= std::numeric_limits<uint16_t>::max()) {
      return detail::narrow<uint16_t>(*x);
    }
    return std::nullopt;
  }
  if (auto const* x = try_as<int64_t>(value)) {
    if (*x >= 0 and *x <= std::numeric_limits<uint16_t>::max()) {
      return detail::narrow<uint16_t>(*x);
    }
    return std::nullopt;
  }
  return std::nullopt;
}

struct AcceptHTTPExecutorArgs {
  location op = location::unknown;
  located<secret> url;
  std::optional<located<record>> responses;
  std::optional<located<uint64_t>> max_request_size;
  std::optional<located<uint64_t>> max_connections;
  std::optional<located<data>> tls;
  std::optional<located<ir::pipeline>> parse;
  let_id request_let;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (max_request_size and max_request_size->inner == 0) {
      diagnostic::error("`max_request_size` must not be zero")
        .primary(max_request_size->source)
        .emit(dh);
      return failure::promise();
    }
    if (max_connections and max_connections->inner == 0) {
      diagnostic::error("`max_connections` must not be zero")
        .primary(max_connections->source)
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
      for (auto const& [_, value] : responses->inner) {
        auto const* rec = try_as<record>(value);
        if (not rec) {
          diagnostic::error("field must be `record`")
            .primary(*responses)
            .emit(dh);
          return failure::promise();
        }
        if (rec->find("code") == rec->end() or rec->find("content_type") == rec->end()
            or rec->find("body") == rec->end()) {
          diagnostic::error("`responses` record must contain `code`, `content_type`, `body`")
            .primary(*responses)
            .emit(dh);
          return failure::promise();
        }
        if (not try_parse_response_code(rec->at("code"))) {
          diagnostic::error("`responses` field `code` must be an integer between 0 and 65535")
            .primary(*responses)
            .emit(dh);
          return failure::promise();
        }
        if (not is<std::string>(rec->at("content_type"))) {
          diagnostic::error("`responses` field `content_type` must be a string")
            .primary(*responses)
            .emit(dh);
          return failure::promise();
        }
        if (not is<std::string>(rec->at("body"))) {
          diagnostic::error("`responses` field `body` must be a string")
            .primary(*responses)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    if (parse) {
      auto output = parse->inner.infer_type(tag_v<chunk_ptr>, dh);
      if (not output) {
        return failure::promise();
      }
      if (*output and not (*output)->is_any<void, table_slice>()) {
        diagnostic::error("pipeline must return events or be a sink")
          .primary(*parse)
          .emit(dh);
        return failure::promise();
      }
    }
    auto tls_opts = tls ? tls_options{*tls, {.tls_default = false, .is_server = true}}
                        : tls_options{{.tls_default = false, .is_server = true}};
    TRY(tls_opts.validate(dh));
    return {};
  }
};

struct AcceptHTTPServerState {
  std::shared_ptr<UnboundedQueue<std::optional<ProxygenRequestData>>> queue;
  std::optional<record> responses;
  size_t max_request_size = default_max_request_size;
};

class AcceptHTTPHandler final : public phttp::coro::HTTPHandler {
public:
  explicit AcceptHTTPHandler(std::shared_ptr<AcceptHTTPServerState> state)
    : state_{std::move(state)} {
  }

  auto handleRequest(folly::EventBase*, phttp::coro::HTTPSessionContextPtr,
                     phttp::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<phttp::coro::HTTPSourceHolder> override {
    auto request = ProxygenRequestData{};
    auto body_too_large = false;
    auto reader = phttp::coro::HTTPSourceReader{std::move(request_source)};
    reader.onHeaders(
      [&](std::unique_ptr<phttp::HTTPMessage> msg, bool is_final,
          bool /*eom*/) -> bool {
        if (not is_final) {
          return phttp::coro::HTTPSourceReader::Continue;
        }
        request.method = msg->getMethodString();
        request.path = msg->getPath();
        request.query = decode_query_string(msg->getQueryString());
        auto [major, minor] = msg->getHTTPVersion();
        request.version = fmt::format("{}.{}", major, minor);
        auto parsed = boost::urls::parse_uri_reference(msg->getURL());
        if (parsed) {
          request.fragment = std::string{parsed->fragment()};
        }
        msg->getHeaders().forEach([&](std::string const& name,
                                      std::string const& value) {
          request.headers.emplace_back(name, value);
        });
        return phttp::coro::HTTPSourceReader::Continue;
      });
    reader.onBody([&](phttp::coro::BufQueue body, bool /*eom*/) -> bool {
      auto bytes = body.chainLength();
      if (request.body.size() + bytes > state_->max_request_size) {
        body_too_large = true;
        return phttp::coro::HTTPSourceReader::Cancel;
      }
      auto iobuf = body.move();
      if (iobuf) {
        for (auto const& range : *iobuf) {
          auto* begin = reinterpret_cast<std::byte const*>(range.data());
          request.body.insert(request.body.end(), begin, begin + range.size());
        }
      }
      return phttp::coro::HTTPSourceReader::Continue;
    });
    co_await reader.read();
    if (body_too_large) {
      co_return phttp::coro::HTTPSourceHolder{
        phttp::coro::HTTPFixedSource::makeFixedResponse(413, std::string{})};
    }
    state_->queue->enqueue(std::optional<ProxygenRequestData>{request});
    auto response_code = uint16_t{200};
    auto content_type = std::string{};
    auto body = std::string{};
    if (state_->responses) {
      auto const it = state_->responses->find(request.path);
      if (it != state_->responses->end()) {
        auto rec = as<record>(it->second);
        auto code = try_parse_response_code(rec["code"]);
        TENZIR_ASSERT(code);
        response_code = *code;
        content_type = as<std::string>(rec["content_type"]);
        body = as<std::string>(rec["body"]);
      }
    }
    auto response_source
      = phttp::coro::HTTPFixedSource::makeFixedResponse(response_code,
                                                        std::move(body));
    if (not content_type.empty()) {
      response_source->msg_->getHeaders().set("Content-Type", content_type);
    }
    co_return phttp::coro::HTTPSourceHolder{response_source};
  }

private:
  std::shared_ptr<AcceptHTTPServerState> state_;
};

struct AcceptHTTPTaskEvent {
  std::optional<ProxygenRequestData> request;
};

class AcceptHTTPExecutorOperator final : public Operator<void, table_slice> {
public:
  explicit AcceptHTTPExecutorOperator(AcceptHTTPExecutorArgs args)
    : args_{std::move(args)}, request_let_id_{args_.request_let} {
  }

  AcceptHTTPExecutorOperator(AcceptHTTPExecutorOperator const&) = delete;
  auto operator=(AcceptHTTPExecutorOperator const&)
    -> AcceptHTTPExecutorOperator& = delete;
  AcceptHTTPExecutorOperator(AcceptHTTPExecutorOperator&&) noexcept
    = default;
  auto operator=(AcceptHTTPExecutorOperator&&) noexcept
    -> AcceptHTTPExecutorOperator& = default;

  ~AcceptHTTPExecutorOperator() override {
    stop_server();
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto request = make_secret_request("url", args_.url, resolved_url_, dh);
    auto resolved = co_await ctx.resolve_secrets({std::move(request)});
    if (not resolved) {
      done_ = true;
      co_return;
    }
    if (resolved_url_.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      done_ = true;
      co_return;
    }
    auto colon = resolved_url_.rfind(':');
    if (colon == 0 or colon == std::string::npos) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      done_ = true;
      co_return;
    }
    auto host = resolved_url_.substr(0, colon);
    auto port = uint16_t{};
    auto* end = resolved_url_.data() + resolved_url_.size();
    auto [ptr, ec] = std::from_chars(resolved_url_.data() + colon + 1, end, port);
    if (ec != std::errc{} or ptr != end) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      done_ = true;
      co_return;
    }
    tls_options_ = args_.tls
                     ? tls_options{*args_.tls,
                                   {.tls_default = false, .is_server = true}}
                     : tls_options{{.tls_default = false, .is_server = true}};
    auto ssl_result = tls_options_.make_folly_ssl_context(dh);
    if (not ssl_result) {
      done_ = true;
      co_return;
    }
    tls_context_ = std::move(*ssl_result);
    server_state_ = std::make_shared<AcceptHTTPServerState>();
    server_state_->queue = queue_;
    server_state_->responses = args_.responses.transform([](auto const& x) {
      return x.inner;
    });
    server_state_->max_request_size
      = inner(args_.max_request_size).value_or(default_max_request_size);
    auto backlog = default_listen_backlog;
    if (auto max_connections = inner(args_.max_connections)) {
      backlog = detail::narrow<uint32_t>(*max_connections);
    }
    auto* event_base = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(event_base);
    auto socket = folly::AsyncServerSocket::newSocket(event_base);
    auto addr = folly::SocketAddress{host, port, true};
    server_ = Box<folly::coro::ServerSocket>{std::in_place, std::move(socket), addr,
                                             backlog};
    handler_ = std::make_shared<AcceptHTTPHandler>(server_state_);
    ctx.spawn_task(folly::coro::co_withExecutor(event_base, accept_loop()));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    auto request = co_await queue_->dequeue();
    co_return AcceptHTTPTaskEvent{std::move(request)};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto event = std::move(result).as<AcceptHTTPTaskEvent>();
    if (not event.request) {
      done_ = true;
      co_return;
    }
    auto request = std::move(*event.request);
    if (not args_.parse) {
      auto slice = make_request_event(request);
      co_await push(std::move(slice));
      co_return;
    }
    if (request.body.empty()) {
      co_return;
    }
    auto request_record = make_request_record(request);
    auto body = std::move(request.body);
    auto encoding = find_header_value(request.headers, "content-encoding");
    auto payload = chunk_ptr{};
    auto body_span = std::span<std::byte const>{body.data(), body.size()};
    if (auto decompressed = try_decompress_body(encoding, body_span, ctx.dh())) {
      payload = chunk::make(std::move(*decompressed));
    } else {
      payload = chunk::make(std::move(body));
    }
    auto pipeline = args_.parse->inner;
    auto env = substitute_ctx::env_t{};
    env[request_let_id_] = std::move(request_record);
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    auto sub_result = pipeline.substitute(substitute_ctx{b_ctx, &env}, true);
    if (not sub_result) {
      co_return;
    }
    active_sub_ = std::move(request);
    auto sub = co_await ctx.spawn_sub(caf::none, std::move(pipeline),
                                      tag_v<chunk_ptr>);
    auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
    auto push_result = co_await open_pipeline.push(std::move(payload));
    if (push_result.is_err()) {
      done_ = true;
      co_return;
    }
    co_await open_pipeline.close();
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    if (not active_sub_ or slice.rows() == 0) {
      co_return;
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&) -> Task<void> override {
    active_sub_ = std::nullopt;
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  auto accept_loop() -> Task<void> {
    TENZIR_ASSERT(server_);
    while (not stopping_) {
      std::unique_ptr<folly::coro::Transport> transport;
      try {
        transport = co_await (*server_)->accept();
      } catch (...) {
        if (not stopping_) {
          queue_->enqueue(std::nullopt);
        }
        co_return;
      }
      if (not transport) {
        continue;
      }
      auto peer_addr = transport->getPeerAddress();
      if (tls_context_) {
        try {
          co_await upgrade_transport_to_tls_server(transport, tls_context_);
        } catch (std::exception const& ex) {
          TENZIR_DEBUG("accept_http: TLS handshake failed with {}: {}",
                       peer_addr.describe(), ex.what());
          continue;
        }
      }
      auto codec = phttp::HTTPCodecFactory::getCodec(
        phttp::CodecProtocol::HTTP_1_1,
        phttp::TransportDirection::DOWNSTREAM);
      TENZIR_ASSERT(codec);
      auto tinfo = wangle::TransportInfo{};
      auto session = phttp::coro::HTTPCoroSession::makeDownstreamCoroSession(
        std::move(transport), handler_, std::move(codec), std::move(tinfo));
      TENZIR_ASSERT(session);
      session->run().start();
    }
  }

  auto stop_server() -> void {
    stopping_ = true;
    if (server_) {
      (*server_)->close();
    }
  }

  AcceptHTTPExecutorArgs args_;
  std::string resolved_url_;
  std::shared_ptr<AcceptHTTPServerState> server_state_;
  std::shared_ptr<UnboundedQueue<std::optional<ProxygenRequestData>>> queue_
    = std::make_shared<UnboundedQueue<std::optional<ProxygenRequestData>>>();
  std::optional<ProxygenRequestData> active_sub_;
  std::optional<Box<folly::coro::ServerSocket>> server_;
  std::shared_ptr<AcceptHTTPHandler> handler_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  let_id request_let_id_;
  tls_options tls_options_{{.tls_default = false, .is_server = true}};
  bool stopping_ = false;
  bool done_ = false;
};

} // namespace

struct AcceptHTTP final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.accept_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptHTTPExecutorArgs, AcceptHTTPExecutorOperator>{};
    d.operator_location(&AcceptHTTPExecutorArgs::op);
    auto url = d.positional("url", &AcceptHTTPExecutorArgs::url);
    auto responses = d.named("responses", &AcceptHTTPExecutorArgs::responses);
    auto max_request_size
      = d.named("max_request_size", &AcceptHTTPExecutorArgs::max_request_size);
    auto max_connections
      = d.named("max_connections", &AcceptHTTPExecutorArgs::max_connections);
    auto tls = d.named("tls", &AcceptHTTPExecutorArgs::tls);
    auto parse
      = d.pipeline(&AcceptHTTPExecutorArgs::parse,
                   {{"request", &AcceptHTTPExecutorArgs::request_let}});
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto args = AcceptHTTPExecutorArgs{};
      args.op = ctx.get_location(url).value_or(location::unknown);
      if (auto x = ctx.get(url)) {
        args.url = *x;
      }
      if (auto x = ctx.get(responses)) {
        args.responses = *x;
      }
      if (auto x = ctx.get(max_request_size)) {
        args.max_request_size = *x;
      }
      if (auto x = ctx.get(max_connections)) {
        args.max_connections = *x;
      }
      if (auto x = ctx.get(tls)) {
        args.tls = *x;
      }
      if (auto x = ctx.get(parse)) {
        args.parse = *x;
      }
      std::ignore = args.validate(ctx);
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::AcceptHTTP)
