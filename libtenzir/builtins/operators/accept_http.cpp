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
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <boost/url/parse.hpp>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/ServerSocket.h>
#define nsel_CONFIG_SELECT_EXPECTED 1
#include <proxygen/lib/http/codec/HTTPCodecFactory.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>

#include <unordered_set>
#include <utility>

namespace tenzir::plugins {
namespace {

using namespace tenzir::si_literals;

constexpr auto default_max_request_size = 10_Mi;
constexpr auto default_listen_backlog = uint32_t{128};

template <typename T>
constexpr auto inner(std::optional<located<T>> const& x) -> std::optional<T> {
  return x.transform([](auto&& x) {
    return x.inner;
  });
};

struct AcceptHttpArgs {
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
      TRY(http::validate_response_map(responses->inner, dh, responses->source));
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

struct AcceptHttpServerState {
  std::shared_ptr<UnboundedQueue<std::optional<http::RequestData>>> queue;
  std::optional<record> responses;
  size_t max_request_size = default_max_request_size;
};

class AcceptHttpHandler final : public proxygen::coro::HTTPHandler {
public:
  explicit AcceptHttpHandler(std::shared_ptr<AcceptHttpServerState> state)
    : state_{std::move(state)} {
  }

  auto handleRequest(folly::EventBase*, proxygen::coro::HTTPSessionContextPtr,
                     proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    auto request = http::RequestData{};
    auto body_too_large = false;
    auto reader = proxygen::coro::HTTPSourceReader{std::move(request_source)};
    reader.onHeaders(
      [&](std::unique_ptr<proxygen::HTTPMessage> msg, bool is_final,
          bool /*eom*/) -> bool {
        if (not is_final) {
          return proxygen::coro::HTTPSourceReader::Continue;
        }
        request.method = msg->getMethodString();
        request.path = msg->getPath();
        request.query = http::decode_query_string(msg->getQueryString());
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
        return proxygen::coro::HTTPSourceReader::Continue;
      });
    reader.onBody([&](proxygen::coro::BufQueue body, bool /*eom*/) -> bool {
      auto bytes = body.chainLength();
      if (request.body.size() + bytes > state_->max_request_size) {
        body_too_large = true;
        return proxygen::coro::HTTPSourceReader::Cancel;
      }
      auto iobuf = body.move();
      if (iobuf) {
        for (auto const& range : *iobuf) {
          auto* begin = reinterpret_cast<std::byte const*>(range.data());
          request.body.insert(request.body.end(), begin, begin + range.size());
        }
      }
      return proxygen::coro::HTTPSourceReader::Continue;
    });
    co_await reader.read();
    if (body_too_large) {
      co_return http::make_fixed_response_source(413, std::string{});
    }
    state_->queue->enqueue(std::optional<http::RequestData>{request});
    auto response_code = uint16_t{200};
    auto content_type = std::string{};
    auto body = std::string{};
    if (state_->responses) {
      if (auto response = http::lookup_response(*state_->responses, request.path)) {
        response_code = response->code;
        content_type = response->content_type;
        body = response->body;
      }
    }
    co_return http::make_fixed_response_source(response_code, std::move(body),
                                               content_type);
  }

private:
  std::shared_ptr<AcceptHttpServerState> state_;
};

struct AcceptHttpTaskEvent {
  std::optional<http::RequestData> request;
};

class AcceptHttp final : public Operator<void, table_slice> {
public:
  explicit AcceptHttp(AcceptHttpArgs args)
    : args_{std::move(args)}, request_let_id_{args_.request_let} {
  }

  AcceptHttp(AcceptHttp const&) = delete;
  auto operator=(AcceptHttp const&)
    -> AcceptHttp& = delete;
  AcceptHttp(AcceptHttp&&) noexcept
    = default;
  auto operator=(AcceptHttp&&) noexcept
    -> AcceptHttp& = default;

  ~AcceptHttp() override {
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
    auto endpoint = http::parse_host_port_endpoint(resolved_url_);
    if (endpoint.is_err()) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      done_ = true;
      co_return;
    }
    auto [host, port] = std::move(endpoint).unwrap();
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
    server_state_ = std::make_shared<AcceptHttpServerState>();
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
    handler_ = std::make_shared<AcceptHttpHandler>(server_state_);
    ctx.spawn_task(folly::coro::co_withExecutor(event_base, accept_loop()));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    auto request = co_await queue_->dequeue();
    co_return AcceptHttpTaskEvent{std::move(request)};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto event = std::move(result).as<AcceptHttpTaskEvent>();
    if (not event.request) {
      done_ = true;
      co_return;
    }
    auto request = std::move(*event.request);
    if (not args_.parse) {
      auto slice = http::make_request_event(request);
      co_await push(std::move(slice));
      co_return;
    }
    if (request.body.empty()) {
      co_return;
    }
    auto request_record = http::make_request_record(request);
    auto body = std::move(request.body);
    auto encoding
      = http::find_header_value(request.headers, "content-encoding");
    auto payload = chunk_ptr{};
    auto body_span = std::span<std::byte const>{body.data(), body.size()};
    if (auto decompressed
        = http::try_decompress_body(encoding, body_span, ctx.dh())) {
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
    auto sub_key = data{next_sub_key_++};
    active_sub_keys_.insert(sub_key);
    auto sub = co_await ctx.spawn_sub(sub_key, std::move(pipeline),
                                      tag_v<chunk_ptr>);
    auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
    auto push_result = co_await open_pipeline.push(std::move(payload));
    if (push_result.is_err()) {
      active_sub_keys_.erase(sub_key);
      done_ = true;
      co_return;
    }
    co_await open_pipeline.close();
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    auto sub_key = materialize(key);
    if (active_sub_keys_.find(sub_key) == active_sub_keys_.end()) {
      co_return;
    }
    if (slice.rows() == 0) {
      co_return;
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&) -> Task<void> override {
    auto sub_key = materialize(key);
    active_sub_keys_.erase(sub_key);
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
      auto codec = proxygen::HTTPCodecFactory::getCodec(
        proxygen::CodecProtocol::HTTP_1_1,
        proxygen::TransportDirection::DOWNSTREAM);
      TENZIR_ASSERT(codec);
      auto tinfo = wangle::TransportInfo{};
      auto session = proxygen::coro::HTTPCoroSession::makeDownstreamCoroSession(
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

  AcceptHttpArgs args_;
  std::string resolved_url_;
  std::shared_ptr<AcceptHttpServerState> server_state_;
  std::shared_ptr<UnboundedQueue<std::optional<http::RequestData>>> queue_
    = std::make_shared<UnboundedQueue<std::optional<http::RequestData>>>();
  std::unordered_set<data> active_sub_keys_;
  std::optional<Box<folly::coro::ServerSocket>> server_;
  std::shared_ptr<AcceptHttpHandler> handler_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  let_id request_let_id_;
  uint64_t next_sub_key_ = 0;
  tls_options tls_options_{{.tls_default = false, .is_server = true}};
  bool stopping_ = false;
  bool done_ = false;
};

} // namespace

struct AcceptHttpPlugin final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.accept_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptHttpArgs, AcceptHttp>{};
    d.operator_location(&AcceptHttpArgs::op);
    auto url = d.positional("url", &AcceptHttpArgs::url);
    auto responses = d.named("responses", &AcceptHttpArgs::responses);
    auto max_request_size
      = d.named("max_request_size", &AcceptHttpArgs::max_request_size);
    auto max_connections
      = d.named("max_connections", &AcceptHttpArgs::max_connections);
    auto tls = d.named("tls", &AcceptHttpArgs::tls);
    auto parse
      = d.pipeline(&AcceptHttpArgs::parse,
                   {{"request", &AcceptHttpArgs::request_let}});
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto args = AcceptHttpArgs{};
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

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::AcceptHttpPlugin)
